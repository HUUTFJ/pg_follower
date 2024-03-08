CREATE EXTENSION pg_follower;

SELECT slot_name FROM pg_create_logical_replication_slot('test', 'test_decoding');

CREATE TABLE IF NOT EXISTS foo (id int, data text, value real);

SELECT data FROM pg_logical_slot_get_changes('test', NULL, NULL, 'include-xids', '0');

DROP TABLE foo;
SELECT * FROM pg_drop_replication_slot('test');