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

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"

#include "storage/latch.h"

#include "utils/wait_event.h"

#include "storage/ipc.h"

#include "access/xlog.h"

PG_FUNCTION_INFO_V1(start_catchup);

PGDLLEXPORT void ddl_detector_worker_main(Datum main_arg);
static bool start_bgworker(const char *connection_string);

static uint32 ddl_detector_we_main = 0;

void
ddl_detector_worker_main(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);

	BackgroundWorkerUnblockSignals();

	/* first time, allocate or get the custom wait event */
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

	/* TODO: bypass the connection string. How? */

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