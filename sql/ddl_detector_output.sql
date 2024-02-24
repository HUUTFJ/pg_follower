
SELECT * FROM pg_create_logical_replication_slot('test', 'ddl_detector');

CREATE TABLE foo (id int);

BEGIN;
INSERT INTO foo VALUES (1);
UPDATE foo SET id = 2 WHERE id = 1;
DELETE FROM foo;
COMMIT;

SELECT * FROM pg_logical_slot_get_changes('test', NULL, NULL, 'include-xids', '0');

DROP TABLE foo;
SELECT * FROM pg_drop_replication_slot('test');
