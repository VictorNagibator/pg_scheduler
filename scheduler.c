/*-------------------------------------------------------------------------
 *
 * scheduler.c - PostgreSQL Background Worker for Job Scheduling
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
 * Parses interval string into microseconds
 */
static void parse_wake_interval_simple(void) {
    char *endptr;
    long sec = strtol(scheduler_wake_interval, &endptr, 10);
    
    if (endptr == scheduler_wake_interval) {
        elog(WARNING, "Invalid interval format, using default 10s");
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
        elog(WARNING, "Unknown interval unit, using seconds");
        scheduler_sleep_us = sec * 1000000L;
    }
}

void _PG_init(void) {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));

    /* Define wake interval GUC */
    DefineCustomStringVariable(
        "scheduler.wake_interval",
        "Interval between job checks (e.g., '10s', '5min', '1h')",
        NULL,
        &scheduler_wake_interval,
        "10s",
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );
    
    /* Define database GUC */
    DefineCustomStringVariable(
        "scheduler.database",
        "Database where scheduler extension is installed",
        NULL,
        &scheduler_database,
        "postgres",
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );

    parse_wake_interval_simple();

    /* Configure and register background worker */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;
    strncpy(worker.bgw_function_name, "scheduler_main", BGW_MAXLEN);
    strncpy(worker.bgw_library_name, "scheduler", BGW_MAXLEN);
    strncpy(worker.bgw_name, "PG Job Scheduler", BGW_MAXLEN);
    worker.bgw_notify_pid = 0;
    
    RegisterBackgroundWorker(&worker);
}

PGDLLEXPORT void scheduler_main(Datum main_arg) {
    long sleep_ms;
    int event;
    char *conninfo;
    PGconn *conn = NULL;
    int64 current_sleep_us = scheduler_sleep_us;
    
    BackgroundWorkerUnblockSignals();

    if (MyProc == NULL) {
        elog(ERROR, "[scheduler] Cannot initialize without process context");
        proc_exit(1);
    }

    InitLatch(&MyProc->procLatch);
    elog(LOG, "[scheduler] Initializing job scheduler");

    /* Use configured database */
    conninfo = psprintf("dbname='%s'", scheduler_database);
    elog(LOG, "[scheduler] Connecting to database: %s", scheduler_database);

    for (;;) {
        if (ProcDiePending) {
            elog(LOG, "[scheduler] Shutdown requested, exiting");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            proc_exit(0);
        }

        sleep_ms = current_sleep_us / 1000;
        if (sleep_ms < 10) sleep_ms = 10;

        event = WaitLatch(&MyProc->procLatch,
                          WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                          sleep_ms,
                          0);

        if (event & WL_POSTMASTER_DEATH) {
            elog(LOG, "[scheduler] Postmaster terminated, exiting");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            proc_exit(1);
        }

        if (event & WL_LATCH_SET) {
            ResetLatch(&MyProc->procLatch);
        }

        if (conn == NULL || PQstatus(conn) != CONNECTION_OK) {
            if (conn) PQfinish(conn);
            conn = PQconnectdb(conninfo);
            if (PQstatus(conn) != CONNECTION_OK) {
                elog(WARNING, "[scheduler] Connection failed: %s", PQerrorMessage(conn));
                PQfinish(conn);
                conn = NULL;
                current_sleep_us = 60 * 1000000L;
                continue;
            }
            PQsetNoticeProcessor(conn, notice_processor, NULL);
        }

        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            elog(WARNING, "[scheduler] BEGIN failed: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            conn = NULL;
            continue;
        }
        PQclear(res);

        const char *check_schema_sql = 
            "SELECT 1 FROM pg_namespace WHERE nspname = 'scheduler'";
        
        res = PQexec(conn, check_schema_sql);
        if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
            elog(WARNING, "[scheduler] Schema 'scheduler' does not exist in database '%s'", scheduler_database);
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            current_sleep_us = 60 * 1000000L;
            continue;
        }
        PQclear(res);

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

        int job_count = PQntuples(res);
        if (job_count > 0) {
            elog(LOG, "[scheduler] Executing %d due job(s)", job_count);
        }

        for (int i = 0; i < job_count; i++) {
            char *job_id = PQgetvalue(res, i, 0);
            char *exec_sql = psprintf("SELECT scheduler.execute_job(%s)", job_id);
            PGresult *exec_res = PQexec(conn, exec_sql);
            
            if (PQresultStatus(exec_res) != PGRES_TUPLES_OK) {
                elog(WARNING, "[scheduler] Job %s failed: %s", job_id, PQerrorMessage(conn));
            }
            
            PQclear(exec_res);
            pfree(exec_sql);
            
            if (ProcDiePending) break;
        }
        
        PQclear(res);
        
        res = PQexec(conn, "COMMIT");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            elog(WARNING, "[scheduler] COMMIT failed: %s", PQerrorMessage(conn));
        }
        PQclear(res);
        
        current_sleep_us = scheduler_sleep_us;
    }
    
    if (conn) PQfinish(conn);
    pfree(conninfo);
}