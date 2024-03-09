
# Trivial tests for decoding WAL records

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Setup upstream node
my $upstream = PostgreSQL::Test::Cluster->new('upstream');
$upstream->init(allows_streaming => 'logical');
$upstream->start;

# Install the pg_follower extension
$upstream->safe_psql('postgres', "CREATE EXTENSION pg_follower;");

# Setup downstream as well
my $downstream = PostgreSQL::Test::Cluster->new('downstream');
$downstream->init();
# $downstream->append_conf('postgresql.conf', "log_min_messages = debug1");
$downstream->start;
$downstream->safe_psql('postgres', "CREATE EXTENSION pg_follower;");

# Call start_follow() for starting a worker
my $upstream_connstr = $upstream->connstr . ' dbname=postgres';
$downstream->safe_psql('postgres', "SELECT * FROM start_follow('$upstream_connstr')");

# Wait until the worker would be started
$downstream->poll_query_until(
	'postgres', "SELECT count(1) = 1 FROM pg_stat_activity WHERE wait_event = 'PgFollowerWorkerMain'"
) or die "Timed out while waiting worker to be started";

# Wait until the worker connect to the upstream node
$upstream->poll_query_until(
	'postgres', "SELECT count(1) = 1 FROM pg_stat_activity WHERE application_name = 'pg_follower worker'"
) or die "Timed out while waiting worker to connect to the upstream";

# Confirm the worker connects to the upstream's postgres database
my $result = $upstream->safe_psql(
	'postgres', "SELECT datname FROM pg_stat_activity WHERE application_name = 'pg_follower worker'
");
is($result, "postgres", "check the worker connects to the postgres database");

# Confirm a new replication slot was created
$upstream->poll_query_until(
	'postgres', "SELECT count(1) = 1 FROM pg_replication_slots;"
) or die "Timed out while waiting worker to create a replication slot";

# ...and its status
$result = $upstream->safe_psql(
	'postgres', "SELECT slot_name, plugin, slot_type, database, temporary FROM pg_replication_slots;
");
is($result, "pg_follower_tmp_slot|pg_follower|logical|postgres|t",
   "check the replication stop has appropriate profiles");

# Create a table and insert tuples to it
$upstream->safe_psql('postgres', "CREATE TABLE foo (id int);");
$upstream->safe_psql('postgres', "INSERT INTO foo VALUES (generate_series(1, 10));");

# Confirm messages were applied on the downstream
$result = $downstream->safe_psql('postgres', "SELECT count(1) FROM foo");
is($result, "10", "check the DDL was propagated");

# DROP TABLE can be also replicated
$upstream->safe_psql('postgres', "DROP TABLE foo;");
$result = $downstream->safe_psql('postgres', "SELECT count(1) FROM pg_class WHERE relname = 'foo'");
is($result, "0", "table was dropped");

# Shutdown both nodes.
# XXX: The downstream must be stopped first because the worker won't consume
# any changes yet.
$downstream->stop;
$upstream->stop;

done_testing();
