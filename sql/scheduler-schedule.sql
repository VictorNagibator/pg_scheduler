-- ============================================================
-- scheduler-schedule: next_run computation and updated_at trigger
-- ============================================================
SET TIME ZONE 'UTC';
SET datestyle TO ISO, MDY;
SET intervalstyle TO postgres;

DELETE FROM scheduler.job_logs;
DELETE FROM scheduler.jobs;

-- Recurring job with a start time in the future: next_run == schedule_time
SELECT scheduler.add_or_update_job('sched_future', 'sql', 'SELECT 1', '1 hour', '2030-01-01 12:00:00+00', 3);
SELECT next_run = schedule_time AS next_run_is_start
  FROM scheduler.jobs WHERE job_name = 'sched_future';

-- Recurring job with a start time in the past: next_run is in the future
SELECT scheduler.add_or_update_job('sched_past', 'sql', 'SELECT 1', '1 hour', '2000-01-01 12:00:00+00', 3);
SELECT next_run > NOW() AS next_run_in_future
  FROM scheduler.jobs WHERE job_name = 'sched_past';

-- Recurring job without a start time: next_run is computed from NOW()
SELECT scheduler.add_or_update_job('sched_now', 'sql', 'SELECT 1', '2 hours', NULL, 3);
SELECT next_run > NOW() AS next_run_in_future
  FROM scheduler.jobs WHERE job_name = 'sched_now';

-- One-time job: next_run == schedule_time and no interval
SELECT scheduler.add_or_update_job('sched_once', 'sql', 'SELECT 1', NULL, '2031-06-15 08:30:00+00', 3);
SELECT next_run = schedule_time AS next_run_is_start,
       schedule_interval IS NULL AS one_time
  FROM scheduler.jobs WHERE job_name = 'sched_once';

-- Disabled jobs never get a next_run
SELECT scheduler.add_or_update_job('sched_off', 'sql', 'SELECT 1', '1 hour', NULL, 3);
SELECT scheduler.toggle_job('sched_off', FALSE);
SELECT next_run IS NULL AS next_run_null
  FROM scheduler.jobs WHERE job_name = 'sched_off';

-- The updated_at trigger advances on update
CREATE TEMP TABLE sched_before AS
  SELECT updated_at FROM scheduler.jobs WHERE job_name = 'sched_now';
SELECT pg_sleep(0.05);
UPDATE scheduler.jobs SET command = 'SELECT 2' WHERE job_name = 'sched_now';
SELECT b.updated_at < j.updated_at AS updated_at_advanced
  FROM sched_before b, scheduler.jobs j WHERE j.job_name = 'sched_now';

-- cleanup
SELECT scheduler.delete_job('sched_future');
SELECT scheduler.delete_job('sched_past');
SELECT scheduler.delete_job('sched_now');
SELECT scheduler.delete_job('sched_once');
SELECT scheduler.delete_job('sched_off');
