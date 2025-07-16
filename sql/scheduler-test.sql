-- Clean up any existing jobs
DELETE FROM scheduler.job_logs;
DELETE FROM scheduler.jobs;

-- 1. Test ADD_JOB and retrieval
SELECT scheduler.add_or_update_job('test_sql', 'sql', $$SELECT 1$$, '1 min', NULL, 2);
-- Expect job created with name 'test_sql'
SELECT job_name, job_type, schedule_interval, max_attempts
  FROM scheduler.jobs WHERE job_name = 'test_sql';

-- 2. Test duplicate ADD_JOB updates existing job
SELECT scheduler.add_or_update_job('test_sql', 'sql', $$SELECT 2$$, '2 min', NULL, 4);
SELECT command, schedule_interval, max_attempts
  FROM scheduler.jobs WHERE job_name = 'test_sql';

-- 3. Test TOGGLE_JOB disables and enables
SELECT scheduler.toggle_job('test_sql', false);
SELECT enabled FROM scheduler.jobs WHERE job_name = 'test_sql';
SELECT scheduler.toggle_job('test_sql', true);
SELECT enabled FROM scheduler.jobs WHERE job_name = 'test_sql';

-- 4. Test DELETE_JOB removes job
SELECT scheduler.delete_job('test_sql');
-- Next line should return 0 rows
SELECT * FROM scheduler.jobs WHERE job_name = 'test_sql';

-- 5. Test EXECUTE_JOB logging and retry
-- Create a helper table
CREATE TABLE scheduler.test_data(id serial primary key, marker text);

-- Add a shell job that appends to test_data
SELECT scheduler.add_or_update_job(
  'test_shell', 'shell', 'echo hello >> /tmp/scheduler_test.log', NULL, NOW(), 1
);

-- Directly invoke execution; expect job_log entry
SELECT scheduler.execute_job((SELECT job_id FROM scheduler.jobs WHERE job_name = 'test_shell'));

-- Verify job_logs
SELECT status, message FROM scheduler.job_logs
  WHERE job_id = (SELECT job_id FROM scheduler.jobs WHERE job_name = 'test_shell');

-- 6. Cleanup
DROP TABLE scheduler.test_data;
SELECT scheduler.delete_job('test_shell');