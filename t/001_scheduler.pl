# ============================================================
# 001_scheduler.pl - end-to-end tests for the background worker
#
# Starts a node with the scheduler preloaded and verifies that the
# background worker dispatches due jobs, handles failures/retries and
# reacts to configuration reloads.
# ============================================================
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');

$node->init;
$node->start;

# The library is only registered as a background worker when it is loaded
# through shared_preload_libraries, but the SQL objects can be created
# before that.  Create the extension first, then restart with the worker.
$node->safe_psql('postgres', 'CREATE EXTENSION scheduler');

$node->append_conf('postgresql.conf', "shared_preload_libraries = 'scheduler'");
$node->append_conf('postgresql.conf', "scheduler.database = 'postgres'");
$node->append_conf('postgresql.conf', "scheduler.wake_interval = '1s'");
$node->restart;

# ------------------------------------------------------------
# A recurring SQL job is executed by the worker without any manual call
# ------------------------------------------------------------
$node->safe_psql(
	'postgres', q{
	CREATE TABLE public.counter(x int);
	SELECT scheduler.add_or_update_job(
		'inc', 'sql', 'INSERT INTO public.counter VALUES (1)', '1s', NULL, 3);
});

ok( $node->poll_query_until(
		'postgres',
		"SELECT count(*) >= 3 FROM scheduler.job_logs l
		   JOIN scheduler.jobs j USING (job_id)
		  WHERE j.job_name = 'inc'"),
	'recurring SQL job executed repeatedly by the worker');

ok( $node->poll_query_until(
		'postgres',
		"SELECT count(*) >= 3 FROM public.counter"),
	'SQL job side effects are visible');

is( $node->safe_psql(
		'postgres',
		"SELECT bool_and(status) FROM scheduler.job_logs l
		   JOIN scheduler.jobs j USING (job_id)
		  WHERE j.job_name = 'inc'"),
	't',
	'all worker runs of the SQL job succeeded');

# ------------------------------------------------------------
# A shell job runs through COPY PROGRAM
# ------------------------------------------------------------
my $outfile = $node->basedir . '/scheduler_shell.out';

$node->safe_psql(
	'postgres',
	"SELECT scheduler.add_or_update_job('shelljob', 'shell', \$\$echo hello >> '$outfile'\$\$, '1s', NULL, 3)");

ok( $node->poll_query_until(
		'postgres',
		"SELECT count(*) >= 1 FROM scheduler.job_logs l
		   JOIN scheduler.jobs j USING (job_id)
		  WHERE j.job_name = 'shelljob'"),
	'shell job executed by the worker');

ok( -e $outfile, 'shell job produced its output file' );
like( slurp_file($outfile), qr/hello/, 'shell job output is correct' );

# ------------------------------------------------------------
# A failing job is retried and disabled after max_attempts
# ------------------------------------------------------------
$node->safe_psql(
	'postgres',
	"SELECT scheduler.add_or_update_job('boom', 'sql', 'SELECT 1/0', '1s', NULL, 2)");

ok( $node->poll_query_until(
		'postgres',
		"SELECT NOT enabled FROM scheduler.jobs WHERE job_name = 'boom'"),
	'failing job is disabled after max_attempts');

is( $node->safe_psql(
		'postgres',
		"SELECT current_attempts FROM scheduler.jobs WHERE job_name = 'boom'"),
	'2',
	'failure counter reached max_attempts');

is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM scheduler.job_logs l
		   JOIN scheduler.jobs j USING (job_id)
		  WHERE j.job_name = 'boom' AND NOT l.status"),
	'2',
	'both failed attempts were logged');

# ------------------------------------------------------------
# Disabled jobs are never picked up by the worker
# ------------------------------------------------------------
$node->safe_psql(
	'postgres', q{
	SELECT scheduler.add_or_update_job(
		'idle', 'sql', 'INSERT INTO public.counter VALUES (1)', '1s', NULL, 3);
	SELECT scheduler.toggle_job('idle', false);
});

is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM scheduler.job_logs l
		   JOIN scheduler.jobs j USING (job_id)
		  WHERE j.job_name = 'idle'"),
	'0',
	'disabled job is not executed');

# ------------------------------------------------------------
# Configuration reload: an invalid wake interval is rejected with a warning
# ------------------------------------------------------------
$node->append_conf('postgresql.conf', "scheduler.wake_interval = 'nonsense'");
$node->reload;

# The worker processes the reload asynchronously, so poll the log briefly.
my $found = 0;
for (1 .. 100)
{
	if ($node->log_contains('Invalid interval format'))
	{
		$found = 1;
		last;
	}
	select(undef, undef, undef, 0.1);
}
ok($found, 'invalid wake interval reported on reload');

done_testing();
