/*-------------------------------------------------------------------------
 *
 * ddl_detector_output.c
 *
 * IDENTIFICATION
 *		ddl_detector/ddl_detector_output.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/* Support routines */
static void output_insert(StringInfo out, Relation relation, char *schema_name,
						  ReorderBufferChange *change);
static void output_update(StringInfo out, Relation relation, char *schema_name,
						  ReorderBufferChange *change);
static void output_delete(StringInfo out, Relation relation, char *schema_name,
						  ReorderBufferChange *change);

/* Callback routines */
static void detector_startup(LogicalDecodingContext *ctx,
							 OutputPluginOptions *options,
							 bool is_init);
static void detector_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void detector_change(LogicalDecodingContext *ctx,
							ReorderBufferTXN *txn,
							Relation relation,
							ReorderBufferChange *change);
static void detector_commit(LogicalDecodingContext *ctx,
							ReorderBufferTXN *txn,
							XLogRecPtr commit_lsn);
static void detector_message(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn,
							 XLogRecPtr message_lsn,
							 bool transactional,
							 const char *prefix,
							 Size message_size,
							 const char *message);

/*
 * Print literal `outputstr' already represented as string of type `typid'
 * into stringbuf `s'.
 *
 * Some builtin types aren't quoted, the rest is quoted. Escaping is done as
 * if standard_conforming_strings were enabled.
 *
 * XXX: ported from test_decoding.c
 */
static void
print_literal(StringInfo s, Oid typid, char *outputstr)
{
	const char *valptr;

	switch (typid)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			/* NB: We don't care about Inf, NaN et al. */
			appendStringInfoString(s, outputstr);
			break;

		case BITOID:
		case VARBITOID:
			appendStringInfo(s, "B'%s'", outputstr);
			break;

		case BOOLOID:
			if (strcmp(outputstr, "t") == 0)
				appendStringInfoString(s, "true");
			else
				appendStringInfoString(s, "false");
			break;

		default:
			appendStringInfoChar(s, '\'');
			for (valptr = outputstr; *valptr; valptr++)
			{
				char		ch = *valptr;

				if (SQL_STR_DOUBLE(ch, false))
					appendStringInfoChar(s, ch);
				appendStringInfoChar(s, ch);
			}
			appendStringInfoChar(s, '\'');
			break;
	}
}

static void
detector_startup(LogicalDecodingContext *ctx, OutputPluginOptions *options,
				 bool is_init)
{
	options->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
}

static void
detector_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	OutputPluginPrepareWrite(ctx, true);

	appendStringInfo(ctx->out, "BEGIN;");

	OutputPluginWrite(ctx, true);
}

/*
 * Construct a INSERT query. Format is:
 *
 * 	INSERT INTO $schema.$table ($type1 [, $type2 ...])
 * 								VALUES ($value1 [, $value2 ...]);
 */
static void
output_insert(StringInfo out, Relation relation, char *schema_name,
			  ReorderBufferChange *change)
{
	HeapTuple		new_tuple;
	TupleDesc		descriptor;
	bool			first_try = true;
	StringInfoData	values;

	Assert(change->action == REORDER_BUFFER_CHANGE_INSERT);

	/* Extract information from arguments */
	new_tuple = change->data.tp.newtuple;
	descriptor = RelationGetDescr(relation);

	/* Construction the query */
	appendStringInfo(out, "INSERT INTO %s.%s ( ", schema_name,
					 RelationGetRelationName(relation));

	initStringInfo(&values);

	/*
	 * Seek each attributes to gather the datatype and value of them. System,
	 * invalid, and null attributes would be skipped.
	 */
	for (int atts = 0; atts < descriptor->natts; atts++)
	{
		Form_pg_attribute	att = TupleDescAttr(descriptor, atts);
		bool				isnull;
		Datum				datum;
		Oid					typoutput;
		bool				typisvarlena;

		/* Skip if the attribute is invalid */
		if (att->attisdropped || att->attgenerated)
			continue;

		/* Get the Datum representation of this value */
		datum = heap_getattr(new_tuple, atts + 1, descriptor, &isnull);

		/* Skip if the attribute is NULL */
		if (isnull)
			continue;

		/* Add a comma if this attribute is the second try */
		if (!first_try)
		{
			appendStringInfoString(out, ", ");
			appendStringInfoString(&values, ", ");
		}

		/*
		 * OK, let's start to write the each attributes. Since someone might be
		 * skipped, all to-be-written attributes must be explicitly described.
		 */
		appendStringInfo(out, "%s", quote_identifier(NameStr(att->attname)));

		getTypeOutputInfo(att->atttypid, &typoutput, &typisvarlena);

		if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(datum))
			appendStringInfoString(&values, "unchanged-toast-datum");
		else if (!typisvarlena)
			print_literal(&values, att->atttypid,
						  OidOutputFunctionCall(typoutput, datum));
		else
		{
			Datum		val;

			val = PointerGetDatum(PG_DETOAST_DATUM(datum));
			print_literal(&values, att->atttypid,
						  OidOutputFunctionCall(typoutput, val));
		}

		first_try = false;
	}

	appendStringInfo(out, " ) VALUES ( %s );", values.data);

	pfree(values.data);
	pfree(schema_name);
}

static void
output_update(StringInfo out, Relation relation, char *schema_name,
			  ReorderBufferChange *change)
{
	HeapTuple old_tuple;
	HeapTuple new_tuple;

	Assert(change->action == REORDER_BUFFER_CHANGE_UPDATE);

	/* Extract information from arguments */
	old_tuple = change->data.tp.oldtuple;
	new_tuple = change->data.tp.newtuple;

	/* Construction the query */
	appendStringInfo(out, "UPDATE %s.%s SET ", schema_name,
					 RelationGetRelationName(relation));
}

static void
output_delete(StringInfo out, Relation relation, char *schema_name,
			  ReorderBufferChange *change)
{
	HeapTuple old_tuple;

	Assert(change->action == REORDER_BUFFER_CHANGE_DELETE);

	/* Extract information from arguments */
	old_tuple = change->data.tp.oldtuple;

	/* Construction the query */
	appendStringInfo(out, "DELETE FROM %s.%s ", schema_name,
					 RelationGetRelationName(relation));
}

static void
detector_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
{
	char *schema_name = get_namespace_name(RelationGetNamespace(relation));

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			OutputPluginPrepareWrite(ctx, true);
			output_insert(ctx->out, relation, schema_name, change);
			OutputPluginWrite(ctx, true);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			OutputPluginPrepareWrite(ctx, true);
			output_update(ctx->out, relation, schema_name, change);
			OutputPluginWrite(ctx, true);
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			OutputPluginPrepareWrite(ctx, true);
			output_delete(ctx->out, relation, schema_name, change);
			OutputPluginWrite(ctx, true);
			break;
		default:
			elog(ERROR, "unknown change");
			break;
	}

	pfree(schema_name);
}

static void
detector_commit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				XLogRecPtr commit_lsn)
{
	OutputPluginPrepareWrite(ctx, true);

	appendStringInfo(ctx->out, "COMMIT;");

	OutputPluginWrite(ctx, true);
}

static void
detector_message(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 XLogRecPtr message_lsn, bool transactional,
				 const char *prefix, Size message_size, const char *message)
{

}

/* Public routines */

/* Specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = detector_startup;
	cb->begin_cb = detector_begin;
	cb->change_cb = detector_change;
	cb->commit_cb = detector_commit;
	cb->message_cb = detector_message;
}
