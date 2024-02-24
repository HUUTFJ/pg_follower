/* detect_ddl/detect_ddl--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION detect_ddl" to load this file. \quit

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
