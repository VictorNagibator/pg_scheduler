**PostgreSQL Job Scheduler Extension**

`pg_scheduler` is a PostgreSQL extension that enables flexible scheduling and execution of SQL and shell jobs directly within your database.

## Features

* **SQL and Shell Jobs**: Define jobs that execute SQL statements or shell commands safely via `COPY PROGRAM`.
* **Recurring Scheduling**: Schedule jobs at fixed intervals (e.g., every minute, hourly, daily) with optional start times.
* **One-Time Jobs**: Schedule single-run tasks at a specified timestamp.
* **Retry Logic**: Automatically retry failed jobs up to a configurable maximum, then disable on persistent failure.
* **Execution Logs**: Historical job logs with timestamps, durations, status, and error messages.
* **Enable/Disable**: Control job execution without dropping definitions.
* **Background Worker**: Integrated BGWorker polls and dispatches due jobs.
* **Simple API**: PL/pgSQL functions for adding, toggling, deleting, and executing jobs.

## Prerequisites

* PostgreSQL
* C compiler
* PostgreSQL server development headers and libraries

## Installation

1. **Clone the repository**

   ```sh
   git clone https://github.com/VictorNagibator/pg_scheduler.git
   ```
   
2. **Build and install extension**

   ```sh
   make USE_PGXS=1
   sudo make USE_PGXS=1 install
   ```
3. **Enable shared preload library**
   Edit your `postgresql.conf`:

   ```conf
   shared_preload_libraries = 'scheduler'
   ```
4. **Restart PostgreSQL**

   ```sh
   sudo systemctl restart postgresql
   ```
5. **Create extension in your database**

   ```sql
   CREATE EXTENSION scheduler;
   ```

## API Reference

All PL/pgSQL functions reside in the `scheduler` schema:

| Function                                                                           | Description                                 |
| ---------------------------------------------------------------------------------- | ------------------------------------------- |
| `scheduler.add_or_update_job(name, type, cmd, interval, start_time, max_attempts)` | Add or update a job definition.             |
| `scheduler.toggle_job(name, enabled)`                                              | Enable or disable a job by name.            |
| `scheduler.delete_job(name)`                                                       | Remove a job and its logs.                  |
| `scheduler.execute_job(job_id)`                                                    | Manually execute a job (for testing).       |
| `scheduler.execute_shell_command(cmd)`                                             | Execute a shell command via `COPY PROGRAM`. |

### Table: `scheduler.jobs`

Stores job definitions and next-run state:

| Column              | Type          | Description                            |
| ------------------- | ------------- | -------------------------------------- |
| `job_id`            | `SERIAL`      | Unique job identifier                  |
| `job_name`          | `TEXT`        | Human-readable unique name             |
| `job_type`          | `TEXT`        | `'sql'` or `'shell'`                   |
| `command`           | `TEXT`        | SQL or shell command                   |
| `schedule_interval` | `INTERVAL`    | Recurrence interval (nullable)         |
| `schedule_time`     | `TIMESTAMPTZ` | Specific start timestamp (nullable)    |
| `enabled`           | `BOOLEAN`     | Whether the job is active              |
| `last_run`          | `TIMESTAMPTZ` | Timestamp of last execution            |
| `next_run`          | `TIMESTAMPTZ` | Next scheduled execution time          |
| `last_status`       | `BOOLEAN`     | Success of last run                    |
| `last_message`      | `TEXT`        | Error message of last failure (if any) |
| `max_attempts`      | `INT`         | Maximum retries before disabling       |
| `current_attempts`  | `INT`         | Consecutive failures count             |
| `created_at`        | `TIMESTAMPTZ` | Creation timestamp                     |
| `updated_at`        | `TIMESTAMPTZ` | Last modification timestamp            |

## Time Notation Conventions

When configuring scheduling intervals via GUC or SQL functions, time units must follow these rules:

1) Use an integer value immediately followed by a unit abbreviation, without spaces (e.g., `10s`, `5min`, `1h`).

2) Supported units:

   - `s` — seconds
   
   - `min` — minutes (60 seconds)
   
   - `h` — hours (3,600 seconds)
   
   - `d` — days (86,400 seconds)
   
   - `w` — weeks (7 days)
   
   - `mon` — months (30 days approximation)

3) If no unit is provided, seconds are assumed (e.g., `10` is treated as `10s`).

4) Intervals passed to SQL functions must conform to PostgreSQL interval syntax (e.g., `'1 day'`, `'30 minutes'`).

## Additional GUC Parameters

The scheduler extension exposes several custom GUC settings for advanced configuration. These can be set in `postgresql.conf` or via SQL:

- `scheduler.wake_interval` (string, default: '10s')

   Interval between scheduler polling cycles. Accepts same time notation as above.

- `scheduler.database` (string, default: 'postgres')

   Name of the database containing the scheduler schema and job tables.

To customize, add entries such as:

```sh
scheduler.wake_interval = '30s'
scheduler.database = 'analytics'
```

## Usage Examples

### Adding a Recurring SQL Job

```sql
SELECT scheduler.add_or_update_job(
    'cleanup_old_records',
    'sql',
    $$DELETE FROM audit_logs WHERE created_at < NOW() - INTERVAL '30 days'$$,
    '1 day',        -- run every day
    '2025-07-09 00:00:00+00', -- time to start
    3               -- retry up to 3 times on failure
);
```

### Adding a One-Time Shell Job

```sql
SELECT scheduler.add_or_update_job(
    'notify_backup',
    'shell',
    'echo Backup completed | mail -s "Backup" admin@example.com',
    NULL,
    '2025-07-10 15:30:00+00'
);
```

### Disabling a Job

```sql
SELECT scheduler.toggle_job('cleanup_old_records', FALSE);
```

### Deleting a Job

```sql
SELECT scheduler.delete_job('notify_backup');
```

### Inspecting Jobs and Logs

```sql
-- List all jobs and statuses
SELECT job_id, job_name, enabled, next_run, last_status
FROM scheduler.jobs
ORDER BY next_run;

-- View execution history for a job
SELECT run_time, status, duration, message
FROM scheduler.job_logs
WHERE job_id = (SELECT job_id FROM scheduler.jobs WHERE job_name = 'cleanup_old_records')
ORDER BY run_time DESC;
```

## Running Regression Tests

A `scheduler-test.sql` script is provided for regression tests. To execute it:

```sh
make USE_PGXS=1 installcheck
```

**Note**: Ensure that `pg_config` is in your `PATH`, so that make `USE_PGXS=1` can locate PostgreSQL build settings.

This will run `pg_regress` with the test script to validate correct behavior.
