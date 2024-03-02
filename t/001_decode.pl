
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
$node->safe_psql('postgres', "CREATE  EXTENSION ddl_detector;");

# Create replication slots.
$node->safe_psql('postgres',
	"SELECT slot_name FROM pg_create_logical_replication_slot('regression_slot1', 'ddl_detector');"
);

# Create table.
$node->safe_psql('postgres', "CREATE TABLE test_repl_stat(col1 int)");

# Insert some data.
$node->safe_psql('postgres',
	"INSERT INTO test_repl_stat values(generate_series(1, 5));");

# Get changes
my $result = $node->safe_psql('postgres',
	"SELECT data FROM pg_logical_slot_get_changes('regression_slot1', NULL, NULL, 'include-xids', '0');"
);

# Check 
is($result, qq{BEGIN;
CREATE TABLE  public.test_repl_stat ( col1 pg_catalog.int4 );
COMMIT;
BEGIN;
INSERT INTO public.test_repl_stat ( col1 ) VALUES ( 1 );
INSERT INTO public.test_repl_stat ( col1 ) VALUES ( 2 );
INSERT INTO public.test_repl_stat ( col1 ) VALUES ( 3 );
INSERT INTO public.test_repl_stat ( col1 ) VALUES ( 4 );
INSERT INTO public.test_repl_stat ( col1 ) VALUES ( 5 );
COMMIT;});

# Shutdown
$node->stop;

done_testing();
