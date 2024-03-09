/* pg_follower/pg_follower--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_follower" to load this file. \quit

-- Start to follow
CREATE FUNCTION start_follow(text)
RETURNS void
AS 'MODULE_PATHNAME'
PARALLEL RESTRICTED
LANGUAGE C;

-- Trigger function
CREATE FUNCTION detect_ddl()
RETURNS event_trigger
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Event trigger
CREATE EVENT TRIGGER test_trigger
ON ddl_command_end
WHEN TAG in ('CREATE TABLE', 'DROP TABLE')
EXECUTE FUNCTION detect_ddl();
