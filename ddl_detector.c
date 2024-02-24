/*-------------------------------------------------------------------------
 *
 * ddl_detector.c
 *		Minimal DDL detector
 *
 *
 * IDENTIFICATION
 *		ddl_detector/ddl_detector.c
 *
 *-------------------------------------------------------------------------
 */

#include <string.h>

#include "postgres.h"
#include "fmgr.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/event_trigger.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_function);

static bool verify_statement(CreateStmt *stmt);
static char *deparse_createstmt(CreateStmt *stmt);

/*
 * Check whether the relation can be exported.
 */
static bool
verify_statement(CreateStmt *stmt)
{
	ListCell *lc;

	/* Only normal relation is supported */
	if (stmt->relation->relpersistence != RELPERSISTENCE_PERMANENT)
	{
		elog(WARNING, "unlogged/temporary tables are not supported");
		return false;
	}

	/* Partitioned relation is not supported */
	if (stmt->partspec != NULL)
	{
		elog(WARNING, "partitioned tables are not supported");
		return false;
	}

	/* Inherited relation is not supported */
	if (stmt->inhRelations != NIL)
	{
		elog(WARNING, "inherited tables are not supported");
		return false;
	}

	/*
	 * TypeName must have valid "names" attribute.
	 *
	 * XXX: When would it become NIL?
	 */
	foreach(lc, stmt->tableElts)
	{
		ColumnDef  *colDef = lfirst(lc);

		if (colDef->typeName->names == NIL)
		{
			elog(WARNING, "not supported");
			return false;
		}
	}

	return true;
}


/*
 * Deparse CreateStmt structure. Returns deparsed result.
 */
static char *
deparse_createstmt(CreateStmt *stmt)
{
	StringInfoData	deparsed;
	ListCell	   *lc;
	bool			first_try = true;

	initStringInfo(&deparsed);
	appendStringInfo(&deparsed, "CREATE TABLE %s.%s ( ",
					 stmt->relation->schemaname, stmt->relation->relname);

	foreach(lc, stmt->tableElts)
	{
		ColumnDef  *colDef = lfirst(lc);
		char	   *typename = NameListToString(colDef->typeName->names);

		if (!first_try)
			appendStringInfoString(&deparsed, ", ");

		appendStringInfo(&deparsed, "%s %s" , colDef->colname, typename);

		pfree(typename);
		first_try = false;
	}

	appendStringInfo(&deparsed, " );");

	return deparsed.data;
}

/*
 * Trigger function
 */
Datum
test_function(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;
	char			 *query;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "must be called as event trigger");

	trigdata = (EventTriggerData *) fcinfo->context;

	/* Only CREATE TABLE command is supported, for now */
	if (trigdata->tag != CMDTAG_CREATE_TABLE)
	{
		elog(WARNING, "this DDL is not supported: %s",
			 GetCommandTagName(trigdata->tag));

		PG_RETURN_NULL();
	}

	Assert(IsA(trigdata->parsetree, CreateStmt));

	/* Can we deparse the statement? If not, do nothing */
	if (!verify_statement((CreateStmt *)trigdata->parsetree))
		PG_RETURN_NULL();

	query = deparse_createstmt((CreateStmt *)trigdata->parsetree);

	elog(INFO, "deparse result: %s", query);

	PG_RETURN_NULL();
}