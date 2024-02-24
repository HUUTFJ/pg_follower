/* ddl_detector/ddl_detector--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ddl_detector" to load this file. \quit

CREATE FUNCTION start_catchup(text)
RETURNS void
AS 'MODULE_PATHNAME'
PARALLEL RESTRICTED
LANGUAGE C;

-- Trigger function
CREATE FUNCTION test_function()
RETURNS event_trigger
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Event trigger
CREATE EVENT TRIGGER test_trigger
ON ddl_command_end
WHEN TAG in ('CREATE TABLE')
EXECUTE FUNCTION test_function();
