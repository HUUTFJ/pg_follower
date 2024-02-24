CREATE EXTENSION ddl_detector;

CREATE TABLE IF NOT EXISTS foo (id int, data text, value real);

DROP TABLE foo;
