-- ============================================================
-- scheduler-execute: job execution, retries, logs and cascade
-- ============================================================
\set VERBOSITY terse
SET TIME ZONE 'UTC';
SET datestyle TO ISO, MDY;
SET intervalstyle TO postgres;

DELETE FROM scheduler.job_logs;
DELETE FROM scheduler.jobs;

CREATE TABLE scheduler.exec_target(x INT);

-- 1. Successful SQL job inserts a row and logs success
SELECT scheduler.add_or_update_job('exec_sql', 'sql', 'INSERT INTO scheduler.exec_target VALUES (1)', '1 hour', NULL, 3);
SELECT scheduler.execute_job((SELECT job_id FROM scheduler.jobs WHERE job_name = 'exec_sql'));
SELECT count(*) AS rows_inserted FROM scheduler.exec_target;
SELECT status, message, duration IS NOT NULL AS has_duration
  FROM scheduler.job_logs l JOIN scheduler.jobs j USING (job_id)
  WHERE j.job_name = 'exec_sql';

-- 2. A successful run resets current_attempts to zero
UPDATE scheduler.jobs SET current_attempts = 5 WHERE job_name = 'exec_sql';
SELECT scheduler.execute_job((SELECT job_id FROM scheduler.jobs WHERE job_name = 'exec_sql'));
SELECT current_attempts FROM scheduler.jobs WHERE job_name = 'exec_sql';

-- 3. A failing SQL job records the error and retries until disabled
SELECT scheduler.add_or_update_job('exec_fail', 'sql', 'SELECT 1/0', NULL, NULL, 2);
SELECT scheduler.execute_job((SELECT job_id FROM scheduler.jobs WHERE job_name = 'exec_fail'));
SELECT current_attempts, enabled, last_status
  FROM scheduler.jobs WHERE job_name = 'exec_fail';
SELECT scheduler.execute_job((SELECT job_id FROM scheduler.jobs WHERE job_name = 'exec_fail'));
SELECT current_attempts, enabled, last_status
  FROM scheduler.jobs WHERE job_name = 'exec_fail';
SELECT message FROM scheduler.job_logs l JOIN scheduler.jobs j USING (job_id)
  WHERE j.job_name = 'exec_fail' ORDER BY l.log_id;

-- 4. Disabled jobs are skipped and produce no log row
SELECT scheduler.add_or_update_job('exec_off', 'sql', 'INSERT INTO scheduler.exec_target VALUES (99)', '1 hour', NULL, 3);
SELECT scheduler.toggle_job('exec_off', FALSE);
SET client_min_messages = warning;
SELECT scheduler.execute_job((SELECT job_id FROM scheduler.jobs WHERE job_name = 'exec_off'));
RESET client_min_messages;
SELECT count(*) AS off_logs FROM scheduler.job_logs l JOIN scheduler.jobs j USING (job_id)
  WHERE j.job_name = 'exec_off';
SELECT count(*) AS target_rows FROM scheduler.exec_target;

-- 5. Executing an unknown job id raises an error
SELECT scheduler.execute_job(999999);

-- 6. Shell jobs and the shell helper
SELECT scheduler.add_or_update_job('exec_shell', 'shell', 'true', NULL, NULL, 3);
SELECT scheduler.execute_job((SELECT job_id FROM scheduler.jobs WHERE job_name = 'exec_shell'));
SELECT status, message FROM scheduler.job_logs l JOIN scheduler.jobs j USING (job_id)
  WHERE j.job_name = 'exec_shell';
SELECT scheduler.execute_shell_command('true');

-- 7. Deleting a job cascades to its logs
SELECT count(*) AS total_logs_before FROM scheduler.job_logs;
SELECT scheduler.delete_job('exec_fail');
SELECT count(*) AS total_logs_after FROM scheduler.job_logs;

-- cleanup
DROP TABLE scheduler.exec_target;
SELECT scheduler.delete_job('exec_sql');
SELECT scheduler.delete_job('exec_shell');
SELECT scheduler.delete_job('exec_off');
