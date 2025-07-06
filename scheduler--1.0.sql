\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

CREATE SCHEMA scheduler;

-- Основная таблица задач
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

-- Таблица для логов выполнения
CREATE TABLE scheduler.job_logs (
    log_id        SERIAL         PRIMARY KEY,
    job_id        INT            NOT NULL REFERENCES scheduler.jobs(job_id) ON DELETE CASCADE,
    run_time      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    status        BOOLEAN        NOT NULL,
    message       TEXT           NULL,
    duration      INTERVAL       NULL
);

-- Автоматическое обновление временных меток
CREATE OR REPLACE FUNCTION scheduler.update_timestamp() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_timestamp
BEFORE UPDATE ON scheduler.jobs
FOR EACH ROW EXECUTE FUNCTION scheduler.update_timestamp();

-- Установка следующего времени выполнения
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

CREATE TRIGGER trg_set_next_run
BEFORE INSERT OR UPDATE ON scheduler.jobs
FOR EACH ROW EXECUTE FUNCTION scheduler.set_next_run();

-- Добавление/обновление задачи
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

-- Включение/отключение задачи
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

-- Удаление задачи
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

-- Основная функция выполнения задач (ИСПРАВЛЕННАЯ ВЕРСИЯ)
CREATE OR REPLACE FUNCTION scheduler.execute_job(target_job_id INT)  -- Изменили имя параметра
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
    -- Используем новое имя параметра в запросе
    SELECT * INTO job_rec 
    FROM scheduler.jobs 
    WHERE job_id = target_job_id;  -- Теперь нет конфликта имен
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job % not found', target_job_id;
    END IF;
    
    IF NOT job_rec.enabled THEN
        RETURN;
    END IF;
    
    start_time := clock_timestamp();
    result_status := TRUE;
    result_message := 'Success';
    
    BEGIN
        IF job_rec.job_type = 'sql' THEN
            -- Выполнение SQL команды
            EXECUTE job_rec.command;
        ELSIF job_rec.job_type = 'shell' THEN
            -- Используем COPY PROGRAM для выполнения shell-команд
            sql_cmd := format('COPY (SELECT 1) TO PROGRAM %L', job_rec.command);
            EXECUTE sql_cmd;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            result_status := FALSE;
            result_message := SQLERRM;
    END;
    
    end_time := clock_timestamp();
    duration := end_time - start_time;
    
    -- Обновление статуса задачи
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
    
    -- Логирование результата
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

-- Функция для выполнения произвольных команд (обертка для COPY PROGRAM)
CREATE OR REPLACE FUNCTION scheduler.execute_shell_command(cmd TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', cmd);
END;
$$;