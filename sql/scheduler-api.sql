-- ============================================================
-- scheduler-api: job definition API and table constraints
-- ============================================================
\set VERBOSITY terse
SET TIME ZONE 'UTC';
SET datestyle TO ISO, MDY;
SET intervalstyle TO postgres;

DELETE FROM scheduler.job_logs;
DELETE FROM scheduler.jobs;

-- 1. Create a recurring job without a start time
SELECT scheduler.add_or_update_job('api_recur', 'sql', 'SELECT 1', '1 hour', NULL, 3);
SELECT job_name, job_type, command, schedule_interval, max_attempts, enabled, current_attempts
  FROM scheduler.jobs WHERE job_name = 'api_recur';

-- 2. Updating an existing job resets runtime state and re-enables it
SELECT scheduler.toggle_job('api_recur', FALSE);
SELECT scheduler.add_or_update_job('api_recur', 'shell', 'echo hi', '30 minutes', NULL, 5);
SELECT job_type, command, schedule_interval, max_attempts, enabled, current_attempts
  FROM scheduler.jobs WHERE job_name = 'api_recur';

-- 3. One-time job with a fixed future start time
SELECT scheduler.add_or_update_job('api_once', 'shell', 'true', NULL, '2030-01-01 00:00:00+00', 1);
SELECT schedule_interval IS NULL AS no_interval,
       schedule_time,
       max_attempts,
       next_run = schedule_time AS next_run_matches
  FROM scheduler.jobs WHERE job_name = 'api_once';

-- 4. Disabling a job clears next_run, enabling recomputes it
SELECT scheduler.toggle_job('api_once', FALSE);
SELECT enabled, next_run IS NULL AS next_run_null
  FROM scheduler.jobs WHERE job_name = 'api_once';
SELECT scheduler.toggle_job('api_once', TRUE);
SELECT enabled, next_run IS NOT NULL AS next_run_set
  FROM scheduler.jobs WHERE job_name = 'api_once';

-- 5. Unknown job names raise a clear error
SELECT scheduler.toggle_job('nope', FALSE);
SELECT scheduler.delete_job('nope');

-- 6. Table constraints are enforced
INSERT INTO scheduler.jobs(job_name, job_type, command) VALUES ('api_bad_type', 'python', 'x');
INSERT INTO scheduler.jobs(job_name, job_type, command) VALUES ('api_recur', 'sql', 'x');
INSERT INTO scheduler.jobs(job_name, command) VALUES ('api_no_type', 'x');

-- 7. Deleting a job cascades to its execution logs
INSERT INTO scheduler.job_logs(job_id, status, message)
  SELECT job_id, TRUE, 'pre' FROM scheduler.jobs WHERE job_name = 'api_recur';
SELECT count(*) AS logs_before FROM scheduler.job_logs;
SELECT scheduler.delete_job('api_recur');
SELECT count(*) AS logs_after FROM scheduler.job_logs;

-- cleanup
SELECT scheduler.delete_job('api_once');
