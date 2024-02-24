
SELECT * FROM pg_create_logical_replication_slot('test', 'ddl_detector');

CREATE TABLE IF NOT EXISTS foo (id int, data text, value real);

SELECT * FROM pg_logical_slot_get_changes('test', NULL, NULL, 'include-xids', '0');

DROP TABLE foo;
SELECT * FROM pg_drop_replication_slot('test');
