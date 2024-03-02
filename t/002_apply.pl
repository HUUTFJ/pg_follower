
# Trivial tests for decoding WAL records

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Test set-up
my $node = PostgreSQL::Test::Cluster->new('test');
$node->init(allows_streaming => 'logical');
$node->start;

# Install the ddl_detector extension
$node->safe_psql('postgres', "CREATE EXTENSION ddl_detector;");

# Call start_catchup() for starting a worker
$node->safe_psql('postgres', "SELECT * FROM start_catchup('host=localhost')");

# Wait until the worker would be started
$node->poll_query_until(
	'postgres', "SELECT count(1) = 1 FROM pg_stat_activity WHERE wait_event = 'DdlDetectorWorkerMain'"
) or die "Timed out while waiting worker to be started";

# Check the worker connects to postgres database 
my $result = $node->safe_psql(
	'postgres', "SELECT datname FROM pg_stat_activity WHERE backend_type LIKE 'ddl_detector worker'");
is($result, 'postgres');

# Shutdown
$node->stop;

done_testing();
