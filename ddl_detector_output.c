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

#include "replication/logical.h"

extern void _PG_init(void);

/* Support routines */
static void output_insert(StringInfo out,
						  Relation relation,
						  ReorderBufferChange *change);
static void output_update(StringInfo out,
						  Relation relation,
						  ReorderBufferChange *change);
static void output_delete(StringInfo out,
						  Relation relation,
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

static void
output_insert(StringInfo out, Relation relation, ReorderBufferChange *change)
{
	appendStringInfoString(out, "INSERTED");
}

static void
output_update(StringInfo out, Relation relation, ReorderBufferChange *change)
{
	appendStringInfoString(out, "UPDATED");
}

static void
output_delete(StringInfo out, Relation relation, ReorderBufferChange *change)
{
	appendStringInfoString(out, "DELETED");
}

static void
detector_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
{
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			OutputPluginPrepareWrite(ctx, true);
			output_insert(ctx->out, relation, change);
			OutputPluginWrite(ctx, true);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			OutputPluginPrepareWrite(ctx, true);
			output_update(ctx->out, relation, change);
			OutputPluginWrite(ctx, true);
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			OutputPluginPrepareWrite(ctx, true);
			output_delete(ctx->out, relation, change);
			OutputPluginWrite(ctx, true);
			break;
		default:
			elog(ERROR, "unknown change");
			break;
	}
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

void
_PG_init(void)
{
    /* NO-OP */
}

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
