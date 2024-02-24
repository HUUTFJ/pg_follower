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

PG_FUNCTION_INFO_V1(start_catchup);


Datum
start_catchup(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(true);
}