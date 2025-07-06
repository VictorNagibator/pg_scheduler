-- contrib/PGScheduler/scheduler--1.0.sql

\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

CREATE SCHEMA scheduler;

CREATE TABLE scheduler.jobs (
    job_id            SERIAL         PRIMARY KEY,
    job_name          TEXT           UNIQUE NOT NULL,
    job_type          TEXT           NOT NULL CHECK (job_type IN ('sql')),
    command           TEXT           NOT NULL,
    schedule_interval INTERVAL       NULL,
    schedule_time     TIMESTAMPTZ    NULL,
    enabled           BOOLEAN        NOT NULL DEFAULT TRUE,
    last_run          TIMESTAMPTZ    NULL,
    next_run          TIMESTAMPTZ    NULL,
    last_status       BOOLEAN        NULL,
    last_message      TEXT           NULL
);

CREATE OR REPLACE FUNCTION scheduler.set_next_run() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.enabled THEN
    NEW.next_run := 
        CASE 
            WHEN NEW.schedule_interval IS NOT NULL THEN now() + NEW.schedule_interval
            ELSE NEW.schedule_time
        END;
  ELSE
    NEW.next_run := NULL;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_set_next_run
BEFORE INSERT OR UPDATE ON scheduler.jobs
FOR EACH ROW EXECUTE FUNCTION scheduler.set_next_run();

CREATE OR REPLACE FUNCTION scheduler.add_job(
    p_name TEXT, 
    p_type TEXT, 
    p_cmd TEXT, 
    p_interval INTERVAL = NULL, 
    p_time TIMESTAMPTZ = NULL
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO scheduler.jobs(job_name, job_type, command, schedule_interval, schedule_time)
  VALUES (p_name, p_type, p_cmd, p_interval, p_time)
  ON CONFLICT (job_name) DO UPDATE SET
    command = EXCLUDED.command,
    schedule_interval = EXCLUDED.schedule_interval,
    schedule_time = EXCLUDED.schedule_time,
    enabled = TRUE;
END;
$$;