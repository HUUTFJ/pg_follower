# pg_follower - Capture changes and follow

The `pg_follower` extension provides a logical replication feature for PostgreSQL.

Note that this extension was created for educational purposes. Please do not use it in the production stage.

[![Workflow result](https://github.com/HUUTFJ/pg_follower/actions/workflows/test.yml/badge.svg)](https://github.com/HUUTFJ/pg_follower/actions/workflows/test.yml)

## Prerequisite

The `pg_follower` extension must be installed on both upstream and downstream.
Since this extension intensely uses the logical decoding mechanism, the wal_level must be set to logical on the upstream.

## Usage

This section describes the primary usage of `pg_follower`.

At first, you must install `pg_follower` extension on both node, e.g.:

```
upstream=# CREATE EXTENSION pg_follower ;
CREATE EXTENSION
```

Then, `start_follow` can be called on the downstream, with a connection string toward the upstream:

```
downstream=# SELECT * FROM start_follow('user=postgres port=5431');
 start_follow
--------------

(1 row)
```

This function kicks the background worker, which receives and applies changes from the upstream.

```
$ ps aux | grep postgres
...
hayato     80919  0.0  0.2 203692 11888 ?        Ss   03:06   0:00 postgres: pg_follower worker
hayato     80920  0.0  0.4 211184 16092 ?        Ss   03:06   0:00 postgres: walsender postgres postgres [local] START_REPLICATION
```

After that, the downstream can follow changes done on the upstream.
Assuming a table is created and 20 tuples are inserted upstream.

```
upstream=# CREATE TABLE foo (id int);
CREATE TABLE
upstream=# INSERT INTO foo VALUES (generate_series(1, 20));
INSERT 0 20
```

After a specific time, we can see the table is also created on the downstream, and tuples exist.

```
downstream=# SELECT * FROM foo ;
 id
----
  1
  2
  3
  4
  5
  6
  7
  8
  9
 10
 11
 12
 13
 14
 15
 16
 17
 18
 19
 20
(20 rows)
```


## Supported feature

For now, only `INSERT` and `CREATE TABLE` statements can be replicated.
Any constraints and parameters for the `CREATE TABLE` would be ignored.
Also, an ERROR would be raised if below clauses are used:

* `UNLOGGED`
* `TEMPORARY`
* `PARTITION OF`
* `PARTITION BY`
* `INHERITS`
* `OF type_name`

## Internals

The `pg_follower` extension contains a logical decoding output plugin, a background worker, and an event trigger.

### logical decoding output plugin

The logical decoding plugin outputs a mimic of raw SQL statements from reorder-buffer changes.

### background worker

The worker connects to the upstream via the libpqwalreceiver shared library.
The connection string is passed from the kick function.
Then, the worker creates a temporary replication slot with the output plugin described above and requests stream changes.

When the worker receives messages (it would be a usual SQL statement) from the upstream, it opens a transaction and executes them via SPI.

### event trigger

The event trigger will fire when DDL commands end.
In the trigger function, the parse-tree is checked and de-parsed into an SQL statement.
The result would be written to WAL record as logical decoding messages.

## TODO

* Add support for table/column constraints
* Add support for `DROP TABLE` statement
* Add support for `UPDATE` statement
* Add support for `DELETE` statement
