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
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/wait_event.h"

PG_FUNCTION_INFO_V1(start_catchup);

PGDLLEXPORT void ddl_detector_worker_main(Datum main_arg);
static bool start_bgworker(const char *connection_string);
static void ddl_init_shmem(void *ptr);
static void ddl_attach_shmem(bool require_found);

static uint32 ddl_detector_we_main = 0;

/* Determine the max length for the connection string */
#define MAXCONNSTRING 1024

/* Shared state information for ddl_detector bgworker. */
typedef struct
{
	Oid		local_database_oid;
	char	connection_string[MAXCONNSTRING];
} ddl_detector_shared_state;

/* Pointer to shared-memory state. */
static ddl_detector_shared_state *ddw_state;

void
ddl_detector_worker_main(Datum main_arg)
{
	Oid		database_oid;

	/* Do we have to define signal handlers? */
	BackgroundWorkerUnblockSignals();

	/* Attach the shared memory */
	ddl_attach_shmem(true);

	/* And accept information */
	database_oid = ddw_state->local_database_oid;

	/* Connect to a local database */
	BackgroundWorkerInitializeConnectionByOid(database_oid, InvalidOid, 0);

	/* Allocate or get the custom wait event */
	if (ddl_detector_we_main == 0)
		ddl_detector_we_main = WaitEventExtensionNew("DdlDetectorWorkerMain");

	for (;;)
	{
		elog(LOG, "XXX loop");

		ResetLatch(MyLatch);

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
						 1000L,
						 ddl_detector_we_main);
	}
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