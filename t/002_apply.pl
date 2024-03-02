
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

# Install the ddl_detector extension
$upstream->safe_psql('postgres', "CREATE EXTENSION ddl_detector;");

# Setup downstream as well
my $downstream = PostgreSQL::Test::Cluster->new('downstream');
$downstream->init();
$downstream->start;
$downstream->safe_psql('postgres', "CREATE EXTENSION ddl_detector;");

# Call start_catchup() for starting a worker
my $upstream_connstr = $upstream->connstr . ' dbname=postgres';
$downstream->safe_psql('postgres', "SELECT * FROM start_catchup('$upstream_connstr')");

# Wait until the worker would be started
$downstream->poll_query_until(
	'postgres', "SELECT count(1) = 1 FROM pg_stat_activity WHERE wait_event = 'DdlDetectorWorkerMain'"
) or die "Timed out while waiting worker to be started";

# Wait until the worker connect to the upstream node
$upstream->poll_query_until(
	'postgres', "SELECT count(1) = 1 FROM pg_stat_activity WHERE application_name = 'ddl_detector worker'"
) or die "Timed out while waiting worker to connect to the upstream";

# Confirm the worker connects to the upstream's postgres database
my $result = $upstream->safe_psql(
	'postgres', "SELECT datname FROM pg_stat_activity WHERE application_name = 'ddl_detector worker'
");
is($result, "postgres");

# Confirm a new replication slot was created
$upstream->poll_query_until(
	'postgres', "SELECT count(1) = 1 FROM pg_replication_slots;"
) or die "Timed out while waiting worker to create a replication slot";

# ...and its status
$result = $upstream->safe_psql(
	'postgres', "SELECT slot_name, plugin, slot_type, database, temporary FROM pg_replication_slots;
");
is($result, "ddl_detector_tmp_slot|ddl_detector|logical|postgres|t");

# Shutdown both nodes
$upstream->stop;
$downstream->stop;

done_testing();
