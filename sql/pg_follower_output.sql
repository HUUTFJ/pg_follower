
SELECT slot_name FROM pg_create_logical_replication_slot('test', 'pg_follower');

BEGIN;
CREATE TABLE foo (id int, data text);
INSERT INTO foo VALUES (1, 'test data');
UPDATE foo SET id = 2 WHERE id = 1;
DELETE FROM foo;
CREATE TABLE var (id int);
TRUNCATE foo, var RESTART IDENTITY CASCADE;
DROP TABLE foo;
COMMIT;

SELECT data FROM pg_logical_slot_get_changes('test', NULL, NULL, 'include-xids', '0');

SELECT * FROM pg_drop_replication_slot('test');
