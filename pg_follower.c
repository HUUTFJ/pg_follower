/*-------------------------------------------------------------------------
 *
 * pg_follower.c
 *		Capture changes and follow
 *
 *
 * IDENTIFICATION
 *		pg_follower/pg_follower.c
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
#include "replication/message.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(detect_ddl);

static bool verify_statement(CreateStmt *stmt);
static char *deparse_createstmt(CreateStmt *stmt);

/*
 * Check whether the relation can be exported.
 */
static bool
verify_statement(CreateStmt *stmt)
{
	ListCell *lc;

	/* Both UNLOGGED and TEMPORARY clauses are not supported */
	if (stmt->relation->relpersistence != RELPERSISTENCE_PERMANENT)
	{
		elog(WARNING, "unlogged/temporary tables are not supported");
		return false;
	}

	/* PARTITION OF clause is not supported */
	if (stmt->partspec != NULL)
	{
		elog(WARNING, "partition tables are not supported");
		return false;
	}

	/* PARTITION BY clause is not supported */
	if (stmt->partspec != NULL)
	{
		elog(WARNING, "partitioned tables are not supported");
		return false;
	}

	/* INHERITS clause is not supported */
	if (stmt->inhRelations != NIL)
	{
		elog(WARNING, "inherited tables are not supported");
		return false;
	}

	/* OF type_name clause is not supported */
	if (stmt->ofTypename != NULL)
	{
		elog(WARNING, "typed tables are not supported");
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
	appendStringInfo(&deparsed, "CREATE TABLE %s %s.%s ( ",
					 stmt->if_not_exists ? "IF NOT EXISTS" : "",
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
detect_ddl(PG_FUNCTION_ARGS)
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

	elog(DEBUG1, "deparse result: %s", query);

	/* Emit the result to the log. */
	LogLogicalMessage("pg_follower", query, strlen(query), true, false);

	pfree(query);

	PG_RETURN_NULL();
}