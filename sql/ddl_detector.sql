CREATE EXTENSION ddl_detector;

CREATE TABLE foo AS (SELECT 1 FROM generate_series(1, 10));

SELECT * FROM foo;
