/*-------------------------------------------------------------------------
 *
 * contrib/scheduler/scheduler.c - PostgreSQL Background Worker for Job Scheduling
 *
 * Implements a job scheduler with support for SQL and shell commands,
 * flexible scheduling intervals, and detailed execution logging.
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
#include <stdint.h>
#include <limits.h>
#include <string.h>

PG_MODULE_MAGIC;

/* GUC configuration variables */
static char *scheduler_wake_interval = "10s";
static int64 scheduler_sleep_us = 10 * 1000000L;
static char *scheduler_database = "postgres";

/* Function prototypes */
PGDLLEXPORT void scheduler_main(Datum main_arg);
void _PG_init(void);
static void parse_wake_interval(void);
static void notice_processor(void *arg, const char *message);

/**
 * notice_processor() - Process notices from libpq connections

 * Logs all notices from database connections at LOG level.
 */
static void notice_processor(void *arg, const char *message) {
    elog(LOG, "[scheduler] Notice: %s", message);
}

/**
 * parse_wake_interval() - Parse human-readable interval into microseconds
 *
 * Converts interval strings (e.g., '10s', '5min', '1h', '2d', '1w', '1mon')
 * to microseconds for sleep timing. Supports:
 *   's'   - seconds
 *   'min' - minutes (60 seconds)
 *   'h'   - hours (3600 seconds)
 *   'd'   - days (86400 seconds)
 *   'w'   - weeks (7 * 86400 seconds)
 *   'mon' - months (30 * 86400 seconds)
 */
static void parse_wake_interval(void) {
    char *endptr;
    long long sec = strtoll(scheduler_wake_interval, &endptr, 10);
    
    if (endptr == scheduler_wake_interval || sec < 0) {
        elog(WARNING, "Invalid interval format '%s', using default 10s", 
             scheduler_wake_interval);
        scheduler_sleep_us = 10 * 1000000L;
        return;
    }
    
    long long micros = 0;
    const long long US_PER_SEC = 1000000LL;

    /* Handle different interval units */
    if (strcmp(endptr, "s") == 0 || *endptr == '\0') {
        micros = sec * US_PER_SEC;
    } else if (strcmp(endptr, "min") == 0) {
        micros = sec * 60LL * US_PER_SEC;
    } else if (strcmp(endptr, "h") == 0) {
        micros = sec * 3600LL * US_PER_SEC;
    } else if (strcmp(endptr, "d") == 0) {
        micros = sec * 86400LL * US_PER_SEC;
    } else if (strcmp(endptr, "w") == 0) {
        micros = sec * 7LL * 86400LL * US_PER_SEC;
    } else if (strcmp(endptr, "mon") == 0) {
        micros = sec * 30LL * 86400LL * US_PER_SEC; /* Approximate month */
    } else {
        elog(WARNING, "Unknown interval unit '%s', using seconds", endptr);
        micros = sec * US_PER_SEC;
    }
    
    /* Validate and set computed interval */
    if (micros < 0) {
        elog(WARNING, "Negative interval computed, using default");
        scheduler_sleep_us = 10 * 1000000L;
    } else if (micros > INT64_MAX) {
        elog(WARNING, "Interval too large, setting to maximum value");
        scheduler_sleep_us = INT64_MAX;
    } else {
        scheduler_sleep_us = (int64) micros;
    }
    
    elog(DEBUG1, "[scheduler] Wake interval set to %lld microseconds", micros);
}

/**
 * _PG_init() - Module initialization entry point
 *
 * Registers background worker and GUC parameters. Executed when
 * the shared library is loaded by PostgreSQL (shared_preload_libraries = 'scheduler'). 
 */
void _PG_init(void) {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));

    /* Register GUC parameters */
    DefineCustomStringVariable(
        "scheduler.wake_interval",
        "Interval between job checks (e.g., '10s', '5min', '1h', '2d', '1w', '1mon')",
        "Determines scheduler polling frequency. Units: s, min, h, d, w, mon.",
        &scheduler_wake_interval,
        "10s",
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );
    
    DefineCustomStringVariable(
        "scheduler.database",
        "Database containing scheduler jobs",
        "Must have 'scheduler' schema with job definitions.",
        &scheduler_database,
        "postgres",
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );

    /* Parse initial interval value */
    parse_wake_interval();

    /* Configure background worker */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;  /* Restart delay after crash (seconds) */
    strncpy(worker.bgw_function_name, "scheduler_main", BGW_MAXLEN);
    strncpy(worker.bgw_library_name, "scheduler", BGW_MAXLEN);
    strncpy(worker.bgw_name, "PostgreSQL Job Scheduler", BGW_MAXLEN);
    worker.bgw_notify_pid = 0;
    
    /* Register worker with postmaster */
    RegisterBackgroundWorker(&worker);
    elog(LOG, "[scheduler] Background worker registered");
}

/**
 * scheduler_main() - Background worker main function
 * 
 * Implements the core scheduler loop:
 * 1. Connects to configured database
 * 2. Checks for due jobs
 * 3. Executes jobs via scheduler.execute_job()
 * 4. Handles connection failures and shutdown signals
 * 5. Sleeps for configured interval
 */
PGDLLEXPORT void scheduler_main(Datum main_arg) {
    int64 current_sleep_us = scheduler_sleep_us;
    int event;
    char *conninfo;
    PGconn *conn = NULL;
    
    /* Allow signal handling */
    BackgroundWorkerUnblockSignals();

    /* Validate process context */
    if (MyProc == NULL) {
        elog(ERROR, "[scheduler] Missing process context");
        proc_exit(1);
    }

    /* Initialize process latch */
    InitLatch(&MyProc->procLatch);
    elog(LOG, "[scheduler] Starting (database=%s, interval=%s)",
         scheduler_database, scheduler_wake_interval);

    /* Build connection string */
    conninfo = psprintf("dbname='%s'", scheduler_database);

    /* Main scheduler loop */
    for (;;) {
        /* Handle shutdown requests */
        if (ProcDiePending) {
            elog(LOG, "[scheduler] Shutting down");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            proc_exit(0);
        }

        /* Calculate sleep time with 10ms minimum */
        long sleep_ms = current_sleep_us / 1000;
        if (sleep_ms < 10) sleep_ms = 10;

        /* Wait for events */
        event = WaitLatch(&MyProc->procLatch,
                          WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                          sleep_ms,
                          0);

        /* Handle postmaster termination */
        if (event & WL_POSTMASTER_DEATH) {
            elog(LOG, "[scheduler] Postmaster terminated");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            proc_exit(1);
        }

        /* Reset latch if triggered */
        if (event & WL_LATCH_SET) {
            ResetLatch(&MyProc->procLatch);
        }

        /* Establish database connection */
        if (conn == NULL || PQstatus(conn) != CONNECTION_OK) {
            if (conn) {
                elog(WARNING, "[scheduler] Connection lost: %s", PQerrorMessage(conn));
                PQfinish(conn);
            }
            
            elog(LOG, "[scheduler] Connecting to %s", scheduler_database);
            conn = PQconnectdb(conninfo);
            
            if (PQstatus(conn) != CONNECTION_OK) {
                elog(WARNING, "[scheduler] Connection failed: %s", PQerrorMessage(conn));
                PQfinish(conn);
                conn = NULL;
                current_sleep_us = 60 * 1000000L;  /* Retry after 60s */
                continue;
            }
            
            /* Configure notice handling */
            PQsetNoticeProcessor(conn, notice_processor, NULL);
            elog(LOG, "[scheduler] Connection established");
        }

        PGresult *res;
        
        /* Begin transaction block */
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

        /* Verify scheduler schema exists */
        const char *check_schema = "SELECT 1 FROM pg_namespace WHERE nspname = 'scheduler'";
        res = PQexec(conn, check_schema);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            elog(WARNING, "[scheduler] Schema check failed: %s", PQerrorMessage(conn));
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            current_sleep_us = 60 * 1000000L;
            continue;
        }
        
        if (PQntuples(res) == 0) {
            elog(WARNING, 
                 "[scheduler] Schema 'scheduler' missing in database '%s'", 
                 scheduler_database);
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            current_sleep_us = 60 * 1000000L;
            continue;
        }
        PQclear(res);

        /* Retrieve due jobs using SKIP LOCKED */
        const char *get_jobs = 
            "SELECT job_id "
            "FROM scheduler.jobs "
            "WHERE enabled AND next_run <= NOW() "
            "ORDER BY next_run "
            "FOR UPDATE SKIP LOCKED";
        
        res = PQexec(conn, get_jobs);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            elog(WARNING, "[scheduler] Job fetch failed: %s", PQerrorMessage(conn));
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
            
            elog(DEBUG1, "[scheduler] Executing job %s", job_id);
            PGresult *exec_res = PQexec(conn, exec_sql);
            
            if (PQresultStatus(exec_res) != PGRES_TUPLES_OK) {
                elog(WARNING, "[scheduler] Job %s failed: %s", job_id, PQerrorMessage(conn));
            }
            
            PQclear(exec_res);
            pfree(exec_sql);
            
            /* Check for shutdown between jobs */
            if (ProcDiePending) break;
        }
        
        PQclear(res);
        
        /* Commit transaction */
        res = PQexec(conn, "COMMIT");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            elog(WARNING, "[scheduler] COMMIT failed: %s", PQerrorMessage(conn));
        }
        PQclear(res);
        
        /* Reset sleep interval */
        current_sleep_us = scheduler_sleep_us;
    }
    
    /* Cleanup (should never reach here) */
    if (conn) PQfinish(conn);
    pfree(conninfo);
}