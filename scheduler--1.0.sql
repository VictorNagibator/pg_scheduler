-- contrib/PGScheduler/scheduler--1.0.sql

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

-- создаём схему и таблицу
CREATE SCHEMA IF NOT EXISTS scheduler;

CREATE TABLE scheduler.jobs (
    job_id            SERIAL         PRIMARY KEY,
    job_name          TEXT           UNIQUE NOT NULL,
    job_type          TEXT           NOT NULL
                         CHECK (job_type IN ('sql','shell')),
    command           TEXT           NOT NULL,
    schedule_interval INTERVAL      NULL,
    schedule_time     TIMESTAMPTZ    NULL,
    enabled           BOOLEAN        NOT NULL DEFAULT TRUE,
    last_run          TIMESTAMPTZ    NULL,
    next_run          TIMESTAMPTZ    NULL
);

ALTER TABLE scheduler.jobs
  ADD CONSTRAINT chk_schedule
    CHECK (
      (schedule_interval IS NOT NULL AND schedule_time IS NULL)
      OR
      (schedule_time     IS NOT NULL AND schedule_interval IS NULL)
    );

-- триггер на заполнение next_run при INSERT
CREATE OR REPLACE FUNCTION scheduler.set_next_run() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.schedule_interval IS NOT NULL THEN
    NEW.next_run := now() + NEW.schedule_interval;
  ELSE
    NEW.next_run := NEW.schedule_time;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_next_run ON scheduler.jobs;
CREATE TRIGGER trg_set_next_run
  BEFORE INSERT ON scheduler.jobs
  FOR EACH ROW EXECUTE FUNCTION scheduler.set_next_run();

-- функции управления
CREATE OR REPLACE FUNCTION scheduler.add_interval_job(
    p_name TEXT, p_cmd TEXT, p_int INTERVAL
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO scheduler.jobs(job_name, job_type, command, schedule_interval)
    VALUES(p_name, 'sql', p_cmd, p_int)
  ON CONFLICT (job_name) DO UPDATE
    SET command = EXCLUDED.command,
        schedule_interval = EXCLUDED.schedule_interval,
        enabled = TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION scheduler.add_shell_job(
    p_name TEXT, p_cmd TEXT, p_int INTERVAL
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO scheduler.jobs(job_name, job_type, command, schedule_interval)
    VALUES(p_name, 'shell', p_cmd, p_int)
  ON CONFLICT (job_name) DO UPDATE
    SET command = EXCLUDED.command,
        schedule_interval = EXCLUDED.schedule_interval,
        enabled = TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION scheduler.list_jobs() RETURNS TABLE(
    job_id INT, job_name TEXT, job_type TEXT, command TEXT,
    schedule_interval INTERVAL, schedule_time TIMESTAMPTZ,
    enabled BOOLEAN, last_run TIMESTAMPTZ, next_run TIMESTAMPTZ
) LANGUAGE sql AS $$
  SELECT job_id, job_name, job_type, command,
         schedule_interval, schedule_time, enabled, last_run, next_run
    FROM scheduler.jobs
   ORDER BY job_id;
$$;