/* contrib/pg_scheduler/scheduler--1.0.sql */

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

-- Create dedicated schema for scheduler objects
CREATE SCHEMA scheduler;

/**
 * Main jobs table storing task definitions and state
 * Columns:
 *   job_id           - Auto-generated unique job identifier
 *   job_name         - Human-readable unique job name
 *   job_type         - Execution type: 'sql' or 'shell'
 *   command          - SQL statement or shell command to execute
 *   schedule_interval- Recurring execution interval (NULL for one-time jobs)
 *   schedule_time    - Specific execution time (NULL for recurring jobs)
 *   enabled          - Job activation status
 *   last_run         - Timestamp of last execution attempt
 *   next_run         - Calculated next execution time
 *   last_status      - Result of last execution (true = success)
 *   last_message     - Error message from last failure
 *   max_attempts     - Maximum retries before disabling job
 *   current_attempts - Current consecutive failure count
 *   created_at       - Job creation timestamp
 *   updated_at       - Last modification timestamp
 */
CREATE TABLE scheduler.jobs (
    job_id            SERIAL         PRIMARY KEY,
    job_name          TEXT           UNIQUE NOT NULL,
    job_type          TEXT           NOT NULL CHECK (job_type IN ('sql', 'shell')),
    command           TEXT           NOT NULL,
    schedule_interval INTERVAL       NULL,
    schedule_time     TIMESTAMPTZ    NULL,
    enabled           BOOLEAN        NOT NULL DEFAULT TRUE,
    last_run          TIMESTAMPTZ    NULL,
    next_run          TIMESTAMPTZ    NULL,
    last_status       BOOLEAN        NULL,
    last_message      TEXT           NULL,
    max_attempts      INT            NOT NULL DEFAULT 3,
    current_attempts  INT            NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

/**
 * Execution log table tracking job runs
 * Columns:
 *   log_id    - Auto-generated log identifier
 *   job_id    - Reference to jobs table
 *   run_time  - Actual execution start time
 *   status    - Execution result (true = success)
 *   message   - Detailed output or error message
 *   duration  - Execution time measured by system clock
 */
CREATE TABLE scheduler.job_logs (
    log_id        SERIAL         PRIMARY KEY,
    job_id        INT            NOT NULL REFERENCES scheduler.jobs(job_id) ON DELETE CASCADE,
    run_time      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    status        BOOLEAN        NOT NULL,
    message       TEXT           NULL,
    duration      INTERVAL       NULL
);

/**
 * Automatic timestamp update function
 * Used in trigger to maintain updated_at column
 */
CREATE OR REPLACE FUNCTION scheduler.update_timestamp() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply timestamp trigger to jobs table
CREATE TRIGGER trg_jobs_timestamp
BEFORE UPDATE ON scheduler.jobs
FOR EACH ROW EXECUTE FUNCTION scheduler.update_timestamp();

/**
 * Next-run calculator function
 * For recurring jobs: next_run = NOW() + interval
 * For one-time jobs: next_run = schedule_time
 * Disabled jobs have next_run = NULL
 */
CREATE OR REPLACE FUNCTION scheduler.set_next_run() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.enabled THEN
    NEW.next_run := 
        CASE 
            WHEN NEW.schedule_interval IS NOT NULL THEN NOW() + NEW.schedule_interval
            ELSE NEW.schedule_time
        END;
  ELSE
    NEW.next_run := NULL;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply next-run calculation on insert/update
CREATE TRIGGER trg_set_next_run
BEFORE INSERT OR UPDATE ON scheduler.jobs
FOR EACH ROW EXECUTE FUNCTION scheduler.set_next_run();

/**
 * Add or update job definition
 * Parameters:
 *   p_name      - Unique job name
 *   p_type      - 'sql' or 'shell'
 *   p_cmd       - Command to execute
 *   p_interval  - Recurring interval (optional)
 *   p_time      - Specific execution time (optional)
 *   p_max_attempts - Max retry attempts (default 3)
 * 
 * On conflict (job_name):
 *   - Update existing job parameters
 *   - Reset state (enabled, current_attempts)
 */
CREATE OR REPLACE FUNCTION scheduler.add_job(
    p_name TEXT, 
    p_type TEXT, 
    p_cmd TEXT, 
    p_interval INTERVAL = NULL, 
    p_time TIMESTAMPTZ = NULL,
    p_max_attempts INT = 3
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO scheduler.jobs(
      job_name, 
      job_type, 
      command, 
      schedule_interval, 
      schedule_time,
      max_attempts
  )
  VALUES (
      p_name, 
      p_type, 
      p_cmd, 
      p_interval, 
      p_time,
      p_max_attempts
  )
  ON CONFLICT (job_name) DO UPDATE SET
    job_type = EXCLUDED.job_type,
    command = EXCLUDED.command,
    schedule_interval = EXCLUDED.schedule_interval,
    schedule_time = EXCLUDED.schedule_time,
    max_attempts = EXCLUDED.max_attempts,
    enabled = TRUE,
    current_attempts = 0;
END;
$$;

/**
 * Enable/disable job by name
 * Parameters:
 *   p_name    - Job name to modify
 *   p_enabled - Target state (true = enabled)
 * 
 * Throws exception if job not found
 */
CREATE OR REPLACE FUNCTION scheduler.toggle_job(
    p_name TEXT, 
    p_enabled BOOLEAN
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  UPDATE scheduler.jobs
  SET enabled = p_enabled
  WHERE job_name = p_name;
  
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Job "%" not found', p_name;
  END IF;
END;
$$;

/**
 * Permanently delete job and associated logs
 * Parameters:
 *   p_name - Job name to remove
 * 
 * Throws exception if job not found
 */
CREATE OR REPLACE FUNCTION scheduler.delete_job(p_name TEXT) 
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM scheduler.jobs 
  WHERE job_name = p_name;
  
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Job "%" not found', p_name;
  END IF;
END;
$$;

/**
 * Core job execution function
 * Parameters:
 *   target_job_id - ID of job to execute
 * 
 * Behavior:
 *   1. Checks job exists and is enabled
 *   2. Executes SQL command directly or shell command via COPY PROGRAM
 *   3. Updates job state:
 *        - Resets attempts on success
 *        - Increments attempts on failure
 *        - Disables job after max_attempts failures
 *   4. Calculates next run time for recurring jobs
 *   5. Logs execution details in job_logs
 */
CREATE OR REPLACE FUNCTION scheduler.execute_job(target_job_id INT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    job_rec scheduler.jobs%ROWTYPE;
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    duration INTERVAL;
    result_status BOOLEAN;
    result_message TEXT;
    sql_cmd TEXT;
BEGIN
    -- Get job details with lock
    SELECT * INTO job_rec 
    FROM scheduler.jobs 
    WHERE job_id = target_job_id
    FOR UPDATE;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job % not found', target_job_id;
    END IF;
    
    -- Skip if job disabled
    IF NOT job_rec.enabled THEN
        RETURN;
    END IF;
    
    -- Initialize execution variables
    start_time := clock_timestamp();
    result_status := TRUE;
    result_message := 'Success';
    
    -- Execute based on job type
    BEGIN
        IF job_rec.job_type = 'sql' THEN
            EXECUTE job_rec.command;  -- Dynamic SQL execution
        ELSIF job_rec.job_type = 'shell' THEN
            -- Safe execution via COPY PROGRAM
            sql_cmd := format('COPY (SELECT 1) TO PROGRAM %L', job_rec.command);
            EXECUTE sql_cmd;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            result_status := FALSE;
            result_message := SQLERRM;  -- Capture PostgreSQL error
    END;
    
    -- Calculate duration
    end_time := clock_timestamp();
    duration := end_time - start_time;
    
    -- Update job status and compute next run
    UPDATE scheduler.jobs
    SET 
        last_run = start_time,
        last_status = result_status,
        last_message = result_message,
        current_attempts = CASE 
            WHEN result_status THEN 0 
            ELSE current_attempts + 1 
        END,
        enabled = CASE
            WHEN NOT result_status AND current_attempts + 1 >= max_attempts THEN FALSE
            ELSE enabled
        END,
        next_run = CASE 
            WHEN result_status AND schedule_interval IS NOT NULL 
            THEN start_time + schedule_interval 
            ELSE next_run 
        END
    WHERE job_id = job_rec.job_id;
    
    -- Log execution
    INSERT INTO scheduler.job_logs(
        job_id, 
        run_time, 
        status, 
        message, 
        duration
    ) VALUES (
        job_rec.job_id,
        start_time,
        result_status,
        result_message,
        duration
    );
END;
$$;

/**
 * Convenience function for shell command execution
 * Parameters:
 *   cmd - Shell command to execute
 * 
 * Implementation:
 *   Uses PostgreSQL's COPY PROGRAM feature to safely execute
 *   OS commands in a controlled environment
 */
CREATE OR REPLACE FUNCTION scheduler.execute_shell_command(cmd TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', cmd);
END;
$$;