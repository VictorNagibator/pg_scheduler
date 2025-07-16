/* scheduler--1.0.sql */

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

/* Create dedicated scheduler schema */
CREATE SCHEMA scheduler;

/*-------------------------------------------------------------------------
 * JOBS TABLE: Core job definitions and state tracking
 * 
 * Columns:
 *   job_id           - Auto-incrementing unique identifier
 *   job_name         - Unique human-readable identifier
 *   job_type         - 'sql' or 'shell'
 *   command          - SQL statement or shell command
 *   schedule_interval- Recurring interval (e.g., '1 h', '7 d', '1 mon')
 *   schedule_time    - Specific start time for recurring jobs
 *   enabled          - Whether job is active
 *   last_run         - Timestamp of last execution
 *   next_run         - Next scheduled execution time
 *   last_status      - Success status of last run
 *   last_message     - Error message from last failure
 *   max_attempts     - Maximum retries before disabling
 *   current_attempts - Current consecutive failures
 *   created_at       - Timestamp of job creation
 *   updated_at       - Timestamp of last modification
 *-------------------------------------------------------------------------
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

COMMENT ON TABLE scheduler.jobs IS 'Job definitions and current state';
COMMENT ON COLUMN scheduler.jobs.schedule_time IS 'Initial execution time for recurring jobs';

/*-------------------------------------------------------------------------
 * JOB_LOGS TABLE: Execution history
 * 
 * Columns:
 *   log_id    - Auto-incrementing log identifier
 *   job_id    - Reference to jobs table
 *   run_time  - Actual execution start time
 *   status    - Success status
 *   message   - Output or error message
 *   duration  - Execution duration
 *-------------------------------------------------------------------------
 */
CREATE TABLE scheduler.job_logs (
    log_id        SERIAL         PRIMARY KEY,
    job_id        INT            NOT NULL REFERENCES scheduler.jobs(job_id) ON DELETE CASCADE,
    run_time      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    status        BOOLEAN        NOT NULL,
    message       TEXT           NULL,
    duration      INTERVAL       NULL
);

COMMENT ON TABLE scheduler.job_logs IS 'Historical job execution records';

/*-------------------------------------------------------------------------
 * UPDATE_TIMESTAMP() - Maintain updated_at column
 * 
 * Trigger function to automatically update updated_at column
 * on any modification to jobs table.
 *-------------------------------------------------------------------------
 */
CREATE OR REPLACE FUNCTION scheduler.update_timestamp() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

/* Apply timestamp trigger */
CREATE TRIGGER trg_jobs_timestamp
BEFORE UPDATE ON scheduler.jobs
FOR EACH ROW EXECUTE FUNCTION scheduler.update_timestamp();

/*-------------------------------------------------------------------------
 * SET_NEXT_RUN() - Calculate next execution time
 * 
 * Computes next_run based on:
 *   - For recurring jobs with schedule_time: 
 *        First run at schedule_time, subsequent at interval
 *   - For recurring jobs without schedule_time: 
 *        First run immediately, subsequent at interval
 *   - For one-time jobs: next_run = schedule_time
 *-------------------------------------------------------------------------
 */
CREATE OR REPLACE FUNCTION scheduler.set_next_run() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.enabled THEN
    NEW.next_run := 
        CASE 
            /* Recurring job with start time */
            WHEN NEW.schedule_interval IS NOT NULL AND NEW.schedule_time IS NOT NULL THEN
                CASE 
                    WHEN NEW.schedule_time > NOW() THEN NEW.schedule_time
                    ELSE NOW() + NEW.schedule_interval
                END
            /* Recurring job without start time */
            WHEN NEW.schedule_interval IS NOT NULL THEN 
                NOW() + NEW.schedule_interval
            /* One-time job */
            ELSE 
                NEW.schedule_time
        END;
  ELSE
    NEW.next_run := NULL;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

/* Apply next-run calculation */
CREATE TRIGGER trg_set_next_run
BEFORE INSERT OR UPDATE ON scheduler.jobs
FOR EACH ROW EXECUTE FUNCTION scheduler.set_next_run();

/*-------------------------------------------------------------------------
 * ADD_OR_UPDATE_JOB() - Create or update job definition
 * 
 * Parameters:
 *   p_name       - Unique job name
 *   p_type       - 'sql' or 'shell'
 *   p_cmd        - Command to execute
 *   p_interval   - Recurring interval (optional)
 *   p_time       - Specific start time (optional)
 *   p_max_attempts - Max retries (default 3)
 * 
 * On conflict (job_name) updates existing job and resets state
 *-------------------------------------------------------------------------
 */
CREATE OR REPLACE FUNCTION scheduler.add_or_update_job(
    p_name TEXT, 
    p_type TEXT, 
    p_cmd TEXT, 
    p_interval INTERVAL = NULL, 
    p_time TIMESTAMPTZ = NULL,
    p_max_attempts INT = 3
) RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    action TEXT;
    job_exists BOOLEAN;
BEGIN
    -- Check if this task exists
    SELECT EXISTS(SELECT 1 FROM scheduler.jobs WHERE job_name = p_name) INTO job_exists;
    
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
    
    -- Choose action
    IF job_exists THEN
        action := 'updated';
    ELSE
        action := 'created';
    END IF;
    
    RAISE NOTICE 'Job "%" % successfully', p_name, action;
    RETURN action;
END;
$$;

COMMENT ON FUNCTION scheduler.add_or_update_job IS 'Create or update job definition';

/*-------------------------------------------------------------------------
 * TOGGLE_JOB() - Enable/disable job by name
 * 
 * Parameters:
 *   p_name    - Job name to modify
 *   p_enabled - Target state (true = enabled)
 * 
 * Throws exception if job not found
 *-------------------------------------------------------------------------
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

COMMENT ON FUNCTION scheduler.toggle_job IS 'Enable or disable job by name';

/*-------------------------------------------------------------------------
 * DELETE_JOB() - Remove job and associated logs
 * 
 * Parameters:
 *   p_name - Job name to delete
 * 
 * Throws exception if job not found
 *-------------------------------------------------------------------------
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

COMMENT ON FUNCTION scheduler.delete_job IS 'Permanently remove job';

/*-------------------------------------------------------------------------
 * EXECUTE_JOB() - Core job execution function
 * 
 * Parameters:
 *   target_job_id - ID of job to execute
 * 
 * Functionality:
 *   1. Locks job row for update
 *   2. Executes command based on job_type
 *   3. Updates job status and next_run
 *   4. Inserts execution log
 *   5. Disables job after max_attempts failures
 *-------------------------------------------------------------------------
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
    /* Lock job row for update */
    SELECT * INTO job_rec 
    FROM scheduler.jobs 
    WHERE job_id = target_job_id
    FOR UPDATE;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job % not found', target_job_id;
    END IF;
    
    /* Notice and skip if job disabled */
    IF NOT job_rec.enabled THEN
        RAISE NOTICE 'Job "%" (ID: %) is disabled. Skipping execution.', 
                 job_rec.job_name, job_rec.job_id;
        RETURN;
    END IF;
    
    /* Initialize execution variables */
    start_time := clock_timestamp();
    result_status := TRUE;
    result_message := 'Success';
    
    /* Execute command based on type */
    BEGIN
        IF job_rec.job_type = 'sql' THEN
            EXECUTE job_rec.command;
        ELSIF job_rec.job_type = 'shell' THEN
            /* Safe execution via COPY PROGRAM */
            sql_cmd := format('COPY (SELECT 1) TO PROGRAM %L', job_rec.command);
            EXECUTE sql_cmd;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            result_status := FALSE;
            result_message := SQLERRM;
    END;
    
    /* Calculate duration */
    end_time := clock_timestamp();
    duration := end_time - start_time;
    
    /* Update job status and compute next run */
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
    
    /* Log execution */
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

COMMENT ON FUNCTION scheduler.execute_job IS 'Internal job execution function';

/*-------------------------------------------------------------------------
 * EXECUTE_SHELL_COMMAND() - Convenience function for shell commands
 * 
 * Parameters:
 *   cmd - Shell command to execute
 * 
 * Uses COPY PROGRAM for safe execution of OS commands.
 *-------------------------------------------------------------------------
 */
CREATE OR REPLACE FUNCTION scheduler.execute_shell_command(cmd TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', cmd);
END;
$$;

COMMENT ON FUNCTION scheduler.execute_shell_command IS 'Execute OS command safely';