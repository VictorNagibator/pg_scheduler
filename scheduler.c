/*-------------------------------------------------------------------------
 *
 * contrib/pg_scheduler/scheduler.c - PostgreSQL Background Worker for Job Scheduling
 *
 * This module implements a background worker that executes scheduled jobs
 * (SQL commands or shell scripts) at specified intervals.
 *
 * Features:
 *  - Job storage in dedicated scheduler schema
 *  - Support for SQL and shell commands
 *  - Automatic retries and failure handling
 *  - Detailed execution logging
 *
 * IDENTIFICATION
 *    contrib/scheduler/scheduler.c
 *
 *-------------------------------------------------------------------------
*/

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "libpq-fe.h"

PG_MODULE_MAGIC;

/* GUC variables */
static char *scheduler_wake_interval = "10s";
static int64 scheduler_sleep_us = 10 * 1000000L;
static char *scheduler_database = "postgres";  // Default database

/* Function prototypes */
PGDLLEXPORT void scheduler_main(Datum main_arg);
void _PG_init(void);
static void parse_wake_interval_simple(void);

/* Static function for notice processing */
static void notice_processor(void *arg, const char *message) {
    elog(LOG, "[scheduler] Notice: %s", message);
}

/**
 * parse_wake_interval_simple() - Parse interval string into microseconds
 * 
 * Converts human-readable interval strings (e.g., '10s', '5min', '1h')
 * into microseconds for internal sleep timing. Handles common time units
 * and provides fallback to seconds for unknown suffixes.
 */
static void parse_wake_interval_simple(void) {
    char *endptr;
    long sec = strtol(scheduler_wake_interval, &endptr, 10);
    
    if (endptr == scheduler_wake_interval) {
        elog(WARNING, "Invalid interval format '%s', using default 10s", 
             scheduler_wake_interval);
        scheduler_sleep_us = 10 * 1000000L;
        return;
    }
    
    if (strcmp(endptr, "s") == 0 || *endptr == '\0') {
        scheduler_sleep_us = sec * 1000000L;
    } else if (strcmp(endptr, "min") == 0) {
        scheduler_sleep_us = sec * 60 * 1000000L;
    } else if (strcmp(endptr, "h") == 0) {
        scheduler_sleep_us = sec * 3600 * 1000000L;
    } else {
        elog(WARNING, "Unknown interval unit '%s', interpreting as seconds", endptr);
        scheduler_sleep_us = sec * 1000000L;
    }
    
    elog(DEBUG1, "[scheduler] Configured sleep interval: %ld microseconds", scheduler_sleep_us);
}

/**
 * _PG_init() - Module initialization function
 *
 * Registers GUC parameters and configures the background worker.
 * Executed when the module is first loaded by PostgreSQL.
 */
void _PG_init(void) {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));

    /* Register GUC for wake interval configuration */
    DefineCustomStringVariable(
        "scheduler.wake_interval",
        "Interval between job checks (e.g., '10s', '5min', '1h')",
        "Determines how frequently the scheduler checks for due jobs.",
        &scheduler_wake_interval,
        "10s",
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );
    
    /* Register GUC for target database */
    DefineCustomStringVariable(
        "scheduler.database",
        "Database where scheduler jobs are stored",
        "Must contain the 'scheduler' schema with job definitions.",
        &scheduler_database,
        "postgres",
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );

    /* Parse initial interval value */
    parse_wake_interval_simple();

    /* Configure background worker parameters */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;  // Restart after 5 seconds on failure
    strncpy(worker.bgw_function_name, "scheduler_main", BGW_MAXLEN);
    strncpy(worker.bgw_library_name, "scheduler", BGW_MAXLEN);
    strncpy(worker.bgw_name, "PG Job Scheduler", BGW_MAXLEN);
    worker.bgw_notify_pid = 0;
    
    /* Register worker with postmaster */
    RegisterBackgroundWorker(&worker);
    elog(LOG, "[scheduler] Background worker registered");
}

/**
 * scheduler_main() - Main entry point for background worker
 * @main_arg: Unused argument required by background worker API
 *
 * Implements the main scheduler loop:
 * 1. Connects to configured database
 * 2. Periodically checks for due jobs
 * 3. Executes jobs via scheduler.execute_job() function
 * 4. Handles connection failures and server shutdown signals
 */
PGDLLEXPORT void scheduler_main(Datum main_arg) {
    long sleep_ms;
    int event;
    char *conninfo;
    PGconn *conn = NULL;
    int64 current_sleep_us = scheduler_sleep_us;
    
    /* Allow signal handling */
    BackgroundWorkerUnblockSignals();

    /* Validate process context */
    if (MyProc == NULL) {
        elog(ERROR, "[scheduler] Cannot initialize without process context");
        proc_exit(1);
    }

    /* Initialize process latch for event handling */
    InitLatch(&MyProc->procLatch);
    elog(LOG, "[scheduler] Starting job scheduler (database=%s, interval=%s)",
         scheduler_database, scheduler_wake_interval);

    /* Build connection string */
    conninfo = psprintf("dbname='%s'", scheduler_database);

    /* Main scheduler loop */
    for (;;) {
        /* Check for shutdown request */
        if (ProcDiePending) {
            elog(LOG, "[scheduler] Shutdown requested, exiting");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            proc_exit(0);
        }

        /* Calculate sleep time with minimum 10ms threshold */
        sleep_ms = current_sleep_us / 1000;
        if (sleep_ms < 10) sleep_ms = 10;

        /* Wait for events with timeout */
        event = WaitLatch(&MyProc->procLatch,
                          WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                          sleep_ms,
                          0);

        /* Handle postmaster termination */
        if (event & WL_POSTMASTER_DEATH) {
            elog(LOG, "[scheduler] Postmaster terminated, exiting");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            proc_exit(1);
        }

        /* Reset latch if triggered */
        if (event & WL_LATCH_SET) {
            ResetLatch(&MyProc->procLatch);
        }

        /* Establish database connection if needed */
        if (conn == NULL || PQstatus(conn) != CONNECTION_OK) {
            if (conn) {
                elog(WARNING, "[scheduler] Connection lost: %s", PQerrorMessage(conn));
                PQfinish(conn);
            }
            
            elog(LOG, "[scheduler] Connecting to database: %s", scheduler_database);
            conn = PQconnectdb(conninfo);
            
            if (PQstatus(conn) != CONNECTION_OK) {
                elog(WARNING, "[scheduler] Connection failed: %s", PQerrorMessage(conn));
                PQfinish(conn);
                conn = NULL;
                current_sleep_us = 60 * 1000000L;  // Retry after 60s
                continue;
            }
            
            /* Configure notice processor */
            PQsetNoticeProcessor(conn, notice_processor, NULL);
            elog(LOG, "[scheduler] Connected to database successfully");
        }

        PGresult *res;
        
        /* Start transaction for job processing */
        res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            elog(WARNING, "[scheduler] BEGIN failed: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            conn = NULL;
            current_sleep_us = 60 * 1000000L;
            continue;
        }
        PQclear(res);

        /* Check for existence of scheduler schema */
        const char *check_schema_sql = 
            "SELECT 1 FROM pg_namespace WHERE nspname = 'scheduler'";
        
        res = PQexec(conn, check_schema_sql);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            elog(WARNING, "[scheduler] Schema check failed: %s", PQerrorMessage(conn));
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            current_sleep_us = 60 * 1000000L;
            continue;
        }
        
        if (PQntuples(res) == 0) {
            elog(WARNING, 
                 "[scheduler] Schema 'scheduler' not found in database '%s'. "
                 "Create the schema and job tables to enable scheduling.",
                 scheduler_database);
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            current_sleep_us = 60 * 1000000L;
            continue;
        }
        PQclear(res);

        /* Retrieve due jobs using SKIP LOCKED to avoid blocking */
        const char *get_jobs_sql = 
            "SELECT job_id "
            "FROM scheduler.jobs "
            "WHERE enabled AND next_run <= NOW() "
            "ORDER BY next_run "
            "FOR UPDATE SKIP LOCKED";
        
        res = PQexec(conn, get_jobs_sql);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            elog(WARNING, "[scheduler] Job selection failed: %s", PQerrorMessage(conn));
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            continue;
        }

        /* Process due jobs */
        int job_count = PQntuples(res);
        if (job_count > 0) {
            elog(LOG, "[scheduler] Found %d due job(s)", job_count);
        }

        for (int i = 0; i < job_count; i++) {
            char *job_id = PQgetvalue(res, i, 0);
            char *exec_sql = psprintf("SELECT scheduler.execute_job(%s)", job_id);
            
            elog(DEBUG1, "[scheduler] Executing job %s: %s", job_id, exec_sql);
            PGresult *exec_res = PQexec(conn, exec_sql);
            
            if (PQresultStatus(exec_res) != PGRES_TUPLES_OK) {
                elog(WARNING, "[scheduler] Job %s failed: %s", job_id, PQerrorMessage(conn));
            } else {
                elog(DEBUG2, "[scheduler] Job %s executed successfully", job_id);
            }
            
            PQclear(exec_res);
            pfree(exec_sql);
            
            /* Check for shutdown signal between jobs */
            if (ProcDiePending) break;
        }
        
        PQclear(res);
        
        /* Commit transaction */
        res = PQexec(conn, "COMMIT");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            elog(WARNING, "[scheduler] COMMIT failed: %s", PQerrorMessage(conn));
        }
        PQclear(res);
        
        /* Reset sleep interval to configured value */
        current_sleep_us = scheduler_sleep_us;
    }
    
    /* Cleanup (unreachable in normal operation) */
    if (conn) PQfinish(conn);
    pfree(conninfo);
}