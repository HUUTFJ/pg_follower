/*-------------------------------------------------------------------------
 *
 * detect_ddl.c
 *		Minimal DDL detector
 *
 *
 * IDENTIFICATION
 *		  detect_ddl/detect_ddl.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "commands/event_trigger.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_function);

/*
 * Trigger function
 */
Datum
test_function(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "must be called as event trigger");

	trigdata = (EventTriggerData *) fcinfo->context;

	PG_RETURN_NULL();
}