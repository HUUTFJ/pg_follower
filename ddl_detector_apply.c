/*-------------------------------------------------------------------------
 *
 * ddl_detector_apply.c
 *
 * IDENTIFICATION
 *		ddl_detector/ddl_detector_apply.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "access/xlog.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/walreceiver.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

PG_FUNCTION_INFO_V1(start_catchup);

PGDLLEXPORT void ddl_detector_worker_main(Datum main_arg);
static bool start_bgworker(const char *connection_string);
static void ddl_init_shmem(void *ptr);
static void ddl_attach_shmem(bool require_found);
static void create_replication_slot(WalReceiverConn *conn);
static bool start_streaming(WalReceiverConn *conn);
static void apply_loop(WalReceiverConn *conn);
static void apply_message(StringInfo message);
static void send_feedback(WalReceiverConn *conn, XLogRecPtr recvpos, bool force,
						  bool requestReply);

static uint32 ddl_detector_we_main = 0;

static MemoryContext message_context = NULL;
static MemoryContext ddl_worker_context = NULL;

/* Determine the max length for the connection string */
#define MAXCONNSTRING 1024

/* Determine name of used replication slot */
#define DDW_SLOT_NAME "ddl_detector_tmp_slot"

/* Determine name of used plugin */
#define DDW_PLUGIN_NAME "ddl_detector"

/* Shared state information for ddl_detector bgworker. */
typedef struct
{
	Oid		local_database_oid;
	char	connection_string[MAXCONNSTRING];
} ddl_detector_shared_state;

/* Pointer to shared-memory state. */
static ddl_detector_shared_state *ddw_state;

/*
 * Create a logical replication slot to the upstream node.
 *
 * walrcv_create_slot() macro cannot be used becasue it cannot create with an
 * arbitrary logical decoding output plugin.
 */
static void
create_replication_slot(WalReceiverConn *conn)
{
#define CREATE_SLOT_OUTPUT_COL_COUNT 4
	StringInfoData 	query;
	Oid				slot_row[CREATE_SLOT_OUTPUT_COL_COUNT] = {TEXTOID, TEXTOID,
															 TEXTOID, TEXTOID};
	bool			started_tx = false;

	initStringInfo(&query);
	appendStringInfo(&query, "CREATE_REPLICATION_SLOT %s TEMPORARY LOGICAL %s",
					 DDW_SLOT_NAME, DDW_PLUGIN_NAME);

	/* The syscache access in walrcv_exec() needs a transaction env. */
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		started_tx = true;
	}

	walrcv_exec(conn, query.data, CREATE_SLOT_OUTPUT_COL_COUNT, slot_row);

	if (started_tx)
		CommitTransactionCommand();

	pfree(query.data);
}

/*
 * Start streaming data from upstream.
 *
 * walrcv_startstreaming() macro cannot be used becasue it requires to specify
 * the name of publications.
 */
static bool
start_streaming(WalReceiverConn *conn)
{
	StringInfoData 	query;
	bool			started_tx;

	initStringInfo(&query);

	/* The syscache access in walrcv_exec() needs a transaction env. */
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		started_tx = true;
	}

	appendStringInfo(&query, "START_REPLICATION SLOT %s LOGICAL 0/0 ;",
					 DDW_SLOT_NAME);

	/*
	 * Since START_REPLICATION returns PGRES_COPY_BOTH response, no need to
	 * prepare nRetTypes and retTypes.
	 */
	walrcv_exec(conn, query.data, 0, NULL);

	if (started_tx)
		CommitTransactionCommand();

	pfree(query.data);

	return true;
}

/*
 * Send a Standby Status Update message to server.
 *
 * 'recvpos' is the latest LSN we've received data to, force is set if we need
 * to send a response to avoid timeouts.
 */
static void
send_feedback(WalReceiverConn *conn, XLogRecPtr recvpos, bool force,
			  bool requestReply)
{
	static StringInfo reply_message = NULL;
	static TimestampTz send_time = 0;

	static XLogRecPtr last_recvpos = InvalidXLogRecPtr;
	static XLogRecPtr last_writepos = InvalidXLogRecPtr;
	static XLogRecPtr last_flushpos = InvalidXLogRecPtr;

	/* XXX: Reply as all the WAL records has been flushed */
	XLogRecPtr	writepos = recvpos;
	XLogRecPtr	flushpos = recvpos;
	TimestampTz now;

	/*
	 * If the user doesn't want status to be reported to the publisher, be
	 * sure to exit before doing anything at all.
	 */
	if (!force && wal_receiver_status_interval <= 0)
		return;

	/* It's legal to not pass a recvpos */
	if (recvpos < last_recvpos)
		recvpos = last_recvpos;

	if (writepos < last_writepos)
		writepos = last_writepos;

	if (flushpos < last_flushpos)
		flushpos = last_flushpos;

	now = GetCurrentTimestamp();

	/* if we've already reported everything we're good */
	if (!force &&
		writepos == last_writepos &&
		flushpos == last_flushpos &&
		!TimestampDifferenceExceeds(send_time, now,
									wal_receiver_status_interval * 1000))
		return;
	send_time = now;

	if (!reply_message)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(ddl_worker_context);

		reply_message = makeStringInfo();
		MemoryContextSwitchTo(oldctx);
	}
	else
		resetStringInfo(reply_message);

	pq_sendbyte(reply_message, 'r');
	pq_sendint64(reply_message, recvpos);	/* write */
	pq_sendint64(reply_message, flushpos);	/* flush */
	pq_sendint64(reply_message, writepos);	/* apply */
	pq_sendint64(reply_message, now);	/* sendTime */
	pq_sendbyte(reply_message, requestReply);	/* replyRequested */

	elog(DEBUG2, "sending feedback (force %d) to recv %X/%X, write %X/%X, flush %X/%X",
		 force,
		 LSN_FORMAT_ARGS(recvpos),
		 LSN_FORMAT_ARGS(writepos),
		 LSN_FORMAT_ARGS(flushpos));

	walrcv_send(conn,
				reply_message->data, reply_message->len);

	if (recvpos > last_recvpos)
		last_recvpos = recvpos;
	if (writepos > last_writepos)
		last_writepos = writepos;
	if (flushpos > last_flushpos)
		last_flushpos = flushpos;
}

/*
 * Read received message and apply via server programming interface 
 */
static void
apply_message(StringInfo message)
{
	const char *query = pq_getmsgbytes(message,
									   (message->len - message->cursor));

	elog(DEBUG1, "received query: %s", query);

	if (strncmp(query, "BEGIN", 5) == 0)
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
	}
	else if (strncmp(query, "CREATE", 5) == 0)
	{
		int ret;

		ret = SPI_execute(query, false, 1);

		if (ret != SPI_OK_UTILITY)
			elog(ERROR, "failed to execute query :%s :%d", query, ret);
	}
	else if (strncmp(query, "INSERT", 6) == 0)
	{
		int ret;

		ret = SPI_execute(query, false, 1);

		if (ret != SPI_OK_INSERT)
			elog(ERROR, "failed to execute query :%s :%d", query, ret);
	}
	else if (strncmp(query, "COMMIT", 6) == 0)
	{
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
}

/*
 * main loop for the ddl worker
 *
 * XXX: basically ported from LogicalRepApplyLoop()
 */
static void
apply_loop(WalReceiverConn *conn)
{
	XLogRecPtr last_received = InvalidXLogRecPtr;
	TimeLineID	tli;

	/* Init the message_context which we clean up after each message */
	message_context = AllocSetContextCreate(ddl_worker_context,
											"ddl_worker_context",
											ALLOCSET_DEFAULT_SIZES);

	for (;;)
	{
		pgsocket	fd = PGINVALID_SOCKET;
		int			rc;
		int			len;
		char	   *buf = NULL;
		bool		endofstream = false;

		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(message_context);

		len = walrcv_receive(conn, &buf, &fd);

		if (len != 0)
		{
			/* Loop to process all available data (without blocking). */
			for (;;)
			{
				CHECK_FOR_INTERRUPTS();

				if (len == 0)
				{
					break;
				}
				else if (len < 0)
				{
					ereport(LOG,
							(errmsg("data stream from publisher has ended")));
					endofstream = true;
					break;
				}
				else
				{
					int			c;
					StringInfoData s;

					if (ConfigReloadPending)
					{
						ConfigReloadPending = false;
						ProcessConfigFile(PGC_SIGHUP);
					}

					/* Ensure we are reading the data into our memory context. */
					MemoryContextSwitchTo(message_context);

					initReadOnlyStringInfo(&s, buf, len);

					c = pq_getmsgbyte(&s);

					if (c == 'w')
					{
						XLogRecPtr	start_lsn;
						XLogRecPtr	end_lsn;

						elog(LOG, "XXX got w");

						start_lsn = pq_getmsgint64(&s);
						end_lsn = pq_getmsgint64(&s);
						/* Timestamp is not used now */
						(void) pq_getmsgint64(&s);

						if (last_received < start_lsn)
							last_received = start_lsn;

						if (last_received < end_lsn)
							last_received = end_lsn;

						apply_message(&s);
					}
					else if (c == 'k')
					{
						XLogRecPtr	end_lsn;
						bool		reply_requested;

						elog(LOG, "XXX got k");

						end_lsn = pq_getmsgint64(&s);
						/* Timestamp is not used now */
						(void) pq_getmsgint64(&s);
						reply_requested = pq_getmsgbyte(&s);

						if (last_received < end_lsn)
							last_received = end_lsn;

						send_feedback(conn, last_received, reply_requested, false);
					}
					/* other message types are purposefully ignored */

					MemoryContextReset(message_context);
				}

				len = walrcv_receive(conn, &buf, &fd);
			}
		}

		send_feedback(conn, last_received, false, false);

		/* Cleanup the memory. */
		MemoryContextReset(message_context);
		MemoryContextSwitchTo(ddl_worker_context);

		/* Check if we need to exit the streaming loop. */
		if (endofstream)
			break;

		/* Sleep 1s or until new message would come */
		rc = WaitLatchOrSocket(MyLatch,
							   WL_SOCKET_READABLE | WL_LATCH_SET |
							   WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							   fd, 1000L,
							   ddl_detector_we_main);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* We won't do timeout */
	}

	walrcv_endstreaming(conn, &tli);
}

void
ddl_detector_worker_main(Datum main_arg)
{
	Oid					database_oid;
	char			   *connection_string;
	WalReceiverConn	   *ddw_walrcv_conn = NULL;
	char			   *err;

	/* Setup signal handlers */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Determine a memory context which is mainly used */
	ddl_worker_context = AllocSetContextCreate(TopMemoryContext,
											   "ddl_worker_context",
											   ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(ddl_worker_context);

	/* Attach the shared memory */
	ddl_attach_shmem(true);

	/* And accept information */
	database_oid = ddw_state->local_database_oid;
	connection_string = pstrdup(ddw_state->connection_string);

	/* Allocate or get the custom wait event */
	if (ddl_detector_we_main == 0)
		ddl_detector_we_main = WaitEventExtensionNew("DdlDetectorWorkerMain");

	/* Connect to a local database */
	BackgroundWorkerInitializeConnectionByOid(database_oid, InvalidOid, 0);

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);

	/* Connect to the upstream */
	ddw_walrcv_conn = walrcv_connect(connection_string, true, true, false,
									 "ddl_detector worker", &err);

	pfree(connection_string);

	/* Create a replication slot */
	create_replication_slot(ddw_walrcv_conn);

	/* Start streaming */
	start_streaming(ddw_walrcv_conn);

	/* RUn main loop */
	apply_loop(ddw_walrcv_conn);

	walrcv_disconnect(ddw_walrcv_conn);
}

/*
 * An implentation for init_callback callback
 */
static void
ddl_init_shmem(void *ptr)
{
	ddl_detector_shared_state *handler = (ddl_detector_shared_state *) ptr;

	handler->local_database_oid = InvalidOid;
	memset(handler->connection_string, 0, MAXCONNSTRING);
}

/*
 * Attach or initialize a shared memory segment
 */
static void
ddl_attach_shmem(bool require_found)
{
	bool found = false;

	ddw_state = GetNamedDSMSegment("ddl_detector",
								   sizeof(ddl_detector_shared_state),
								   ddl_init_shmem,
								   &found);

	if (require_found && !found)
		elog(ERROR, "caller requires to attach the allocated memory, but not found");
}

static bool
start_bgworker(const char *connection_string)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	strcpy(worker.bgw_name, "ddl_detector worker");
	strcpy(worker.bgw_type, "ddl_detector worker");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(worker.bgw_library_name, "ddl_detector");
	strcpy(worker.bgw_function_name, "ddl_detector_worker_main");

	worker.bgw_main_arg = (Datum) 0;

	/* must set notify PID to wait for startup */
	worker.bgw_notify_pid = MyProcPid;

	/* Dinamically allocate a shared memory */
	ddl_attach_shmem(false);

	/* Fill shared-memory data structure for passing info to the worker */ 
	ddw_state->local_database_oid = MyDatabaseId;
	strncpy(ddw_state->connection_string, connection_string, MAXCONNSTRING);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "could not register background process");

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		elog(ERROR, "could not start background process");

	return true;
}

Datum
start_catchup(PG_FUNCTION_ARGS)
{
	char *connection_string;

	if (RecoveryInProgress())
		elog(ERROR, "recovery is in progress");

	connection_string = text_to_cstring(PG_GETARG_TEXT_PP(0));

	start_bgworker(connection_string);

	PG_RETURN_VOID();
}