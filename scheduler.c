/*-------------------------------------------------------------------------
 *
 * scheduler.c - PostgreSQL Background Worker for Job Scheduling
 *
 * Implements a job scheduler with support for SQL and shell commands,
 * flexible scheduling intervals, and detailed execution logging.
 *
 * IDENTIFICATION
 *    scheduler.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "utils/wait_event_types.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "tcop/utility.h"
#include <stdint.h>
#include <limits.h>
#include <string.h>
#include <errno.h>

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
    errno = 0;  // Reset errno before call
    long long sec = strtoll(scheduler_wake_interval, &endptr, 10);
    
    // Handle parsing errors and negative values
    if (errno == ERANGE || endptr == scheduler_wake_interval || sec < 0) {
        elog(WARNING, "Invalid interval format '%s', using default 10s", 
             scheduler_wake_interval);
        scheduler_sleep_us = 10 * 1000000L;
        return;
    }
    
    const int64 US_PER_SEC = 1000000;
    int64 micros = 0;
    bool overflow = false;

    /* Handle different interval units with overflow protection */
    if (strcmp(endptr, "s") == 0 || *endptr == '\0') {
        // Check for overflow: sec * US_PER_SEC > INT64_MAX
        if (sec > 0 && US_PER_SEC > INT64_MAX / sec) {
            overflow = true;
        } else {
            micros = sec * US_PER_SEC;
        }
    } 
    else if (strcmp(endptr, "min") == 0) {
        const int64 MIN_IN_SEC = 60;
        // First check: sec * MIN_IN_SEC
        if (sec > 0 && MIN_IN_SEC > INT64_MAX / sec) {
            overflow = true;
        } else {
            int64 total_sec = sec * MIN_IN_SEC;
            // Second check: total_sec * US_PER_SEC
            if (total_sec > 0 && US_PER_SEC > INT64_MAX / total_sec) {
                overflow = true;
            } else {
                micros = total_sec * US_PER_SEC;
            }
        }
    } 
    else if (strcmp(endptr, "h") == 0) {
        const int64 HOUR_IN_SEC = 3600;
        if (sec > 0 && HOUR_IN_SEC > INT64_MAX / sec) {
            overflow = true;
        } else {
            int64 total_sec = sec * HOUR_IN_SEC;
            if (total_sec > 0 && US_PER_SEC > INT64_MAX / total_sec) {
                overflow = true;
            } else {
                micros = total_sec * US_PER_SEC;
            }
        }
    } 
    else if (strcmp(endptr, "d") == 0) {
        const int64 DAY_IN_SEC = 86400;
        if (sec > 0 && DAY_IN_SEC > INT64_MAX / sec) {
            overflow = true;
        } else {
            int64 total_sec = sec * DAY_IN_SEC;
            if (total_sec > 0 && US_PER_SEC > INT64_MAX / total_sec) {
                overflow = true;
            } else {
                micros = total_sec * US_PER_SEC;
            }
        }
    } 
    else if (strcmp(endptr, "w") == 0) {
        const int64 WEEK_IN_SEC = 7 * 86400;
        if (sec > 0 && WEEK_IN_SEC > INT64_MAX / sec) {
            overflow = true;
        } else {
            int64 total_sec = sec * WEEK_IN_SEC;
            if (total_sec > 0 && US_PER_SEC > INT64_MAX / total_sec) {
                overflow = true;
            } else {
                micros = total_sec * US_PER_SEC;
            }
        }
    } 
    else if (strcmp(endptr, "mon") == 0) {
        const int64 MONTH_IN_SEC = 30 * 86400;
        if (sec > 0 && MONTH_IN_SEC > INT64_MAX / sec) {
            overflow = true;
        } else {
            int64 total_sec = sec * MONTH_IN_SEC;
            if (total_sec > 0 && US_PER_SEC > INT64_MAX / total_sec) {
                overflow = true;
            } else {
                micros = total_sec * US_PER_SEC;
            }
        }
    } 
    else {
        elog(WARNING, "Unknown interval unit '%s', using seconds", endptr);
        if (sec > 0 && US_PER_SEC > INT64_MAX / sec) {
            overflow = true;
        } else {
            micros = sec * US_PER_SEC;
        }
    }
    
    /* Handle overflow cases */
    if (overflow) {
        elog(WARNING, "Interval value overflow, using maximum");
        scheduler_sleep_us = INT64_MAX;
    } 
    /* Validate and set computed interval */
    else if (micros < 0) {
        elog(WARNING, "Negative interval computed, using default");
        scheduler_sleep_us = 10 * 1000000L;
    } else {
        scheduler_sleep_us = micros;
    }
    
    elog(DEBUG1, "[scheduler] Wake interval set to %lld microseconds", (long long)micros);
}

/**
 * _PG_init() - Module initialization entry point
 *
 * Registers background worker and GUC parameters. Executed when
 * the shared library is loaded by PostgreSQL (shared_preload_libraries = 'scheduler'). 
 */
void _PG_init(void) {
    /* Require loading via shared_preload_libraries */
    if (!process_shared_preload_libraries_in_progress) {
        elog(WARNING, "scheduler module must be loaded via shared_preload_libraries");
        return;
    }

    BackgroundWorker worker;

    /* Register GUC parameters */
    DefineCustomStringVariable(
        "scheduler.wake_interval",
        "Interval between job checks (e.g., '10s', '5min', '1h', '2d', '1w', '1mon')",
        "Determines scheduler polling frequency. Units: s, min, h, d, w, mon.",
        &scheduler_wake_interval,
        "10s",
        PGC_SIGHUP, // Dynamic configuration is available :)
        0,
        NULL, NULL, NULL
    );
    
    DefineCustomStringVariable(
        "scheduler.database",
        "Database containing scheduler jobs",
        "Must have 'scheduler' schema with job definitions.",
        &scheduler_database,
        "postgres",
        PGC_SIGHUP,
        0,
        NULL, NULL, NULL
    );

    /* Parse initial interval value */
    parse_wake_interval();

    /* Configure background worker */
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    /* Allow shared memory access and database connection */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    /* Start after recovery finishes */
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    /* Restart delay after crash (seconds) */
    worker.bgw_restart_time = 5;
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
    
    /* Store current settings to detect changes on reload */
    char *prev_database = pstrdup(scheduler_database);
    char *prev_wake_interval = pstrdup(scheduler_wake_interval);

    /* Establish signal handlers before unblocking signals */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
    /* Clean shutdown handler */
    pqsignal(SIGTERM, die);
    
    /* Allow background worker to handle signals */
    BackgroundWorkerUnblockSignals();
    
    /* Validate process context */
    if (MyProc == NULL) {
        elog(ERROR, "[scheduler] Missing process context");
        proc_exit(1);
    }

    /* Initialize process latch for event waiting */
    InitLatch(&MyProc->procLatch);
    elog(LOG, "[scheduler] Starting (database=%s, interval=%s)",
         scheduler_database, scheduler_wake_interval);

    /* Build connection string for target database */
    conninfo = psprintf("dbname='%s'", scheduler_database);

    /* Main scheduler loop */
    for (;;) {
        /* Critical interrupt check point */
        if (ProcDiePending) {
            elog(LOG, "[scheduler] Shutting down");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            pfree(prev_database);
            pfree(prev_wake_interval);
            proc_exit(0);
        }

        /* Handle configuration reload requests */
        if (ConfigReloadPending) {
            ConfigReloadPending = false;
            elog(LOG, "[scheduler] Reloading configuration");
            
            // Read config
            ProcessConfigFile(PGC_SIGHUP);
            
            // Check interval change
            if (strcmp(prev_wake_interval, scheduler_wake_interval) != 0) {
                pfree(prev_wake_interval);
                /* Update stored interval setting */
                prev_wake_interval = pstrdup(scheduler_wake_interval);
                parse_wake_interval();
                elog(LOG, "[scheduler] Wake interval updated to '%s'", 
                     scheduler_wake_interval);
            }
            
            // Check db change
            if (strcmp(prev_database, scheduler_database) != 0) {
                pfree(prev_database);
                /* Update stored database setting */
                prev_database = pstrdup(scheduler_database);
                elog(LOG, "[scheduler] Database changed to '%s'", 
                     scheduler_database);
                
                pfree(conninfo);
                /* Rebuild connection string with new database */
                conninfo = psprintf("dbname='%s'", scheduler_database);
                
                if (conn) {
                    /* Close existing connection to old database */
                    PQfinish(conn);
                    conn = NULL;
                }
            }
        }

        /* Calculate sleep time with 10ms minimum */
        long sleep_ms = current_sleep_us / 1000;
        if (sleep_ms < 10) sleep_ms = 10;

        /* Wait for events with interrupt checks */
        event = WaitLatch(&MyProc->procLatch,
                          WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                          sleep_ms,
                          PG_WAIT_EXTENSION);

        /* Handle postmaster termination */
        if (event & WL_POSTMASTER_DEATH) {
            elog(LOG, "[scheduler] Postmaster terminated");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            pfree(prev_database);
            pfree(prev_wake_interval);
            proc_exit(1);
        }

        /* Reset latch for next wait cycle */
        ResetLatch(&MyProc->procLatch);
        
        /* Process any pending interrupts */
        CHECK_FOR_INTERRUPTS();

        /* Establish/reconnect database connection */
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
                /* Retry after 60s on connection failure */
                current_sleep_us = 60 * 1000000L;
                continue;
            }
            
            /* Configure notice processor for connection logs */
            PQsetNoticeProcessor(conn, notice_processor, NULL);
            elog(LOG, "[scheduler] Connection established");
        }

        PGresult *res;
        
        /* Begin transaction block for job processing */
        res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            elog(WARNING, "[scheduler] BEGIN failed: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            conn = NULL;
            /* Retry after 60s on transaction failure */
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
        
        /* Handle missing scheduler schema */
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

        /* Process due jobs with interrupt checks */
        int job_count = PQntuples(res);
        if (job_count > 0) {
            elog(LOG, "[scheduler] Found %d due job(s)", job_count);
        }

        for (int i = 0; i < job_count; i++) {
            /* Critical interrupt check before each job */
            if (ProcDiePending) {
                break;
            }
            
            char *job_id = PQgetvalue(res, i, 0);
            /* Build execution command for current job */
            char *exec_sql = psprintf("SELECT scheduler.execute_job(%s)", job_id);
            
            elog(DEBUG1, "[scheduler] Executing job %s", job_id);
            PGresult *exec_res = PQexec(conn, exec_sql);
            
            if (PQresultStatus(exec_res) != PGRES_TUPLES_OK) {
                elog(WARNING, "[scheduler] Job %s failed: %s", job_id, PQerrorMessage(conn));
            }
            
            PQclear(exec_res);
            pfree(exec_sql);
            
            /* Check for shutdown between jobs */
            if (ProcDiePending) {
                break;
            }
        }
        
        PQclear(res);
        
        /* Finalize transaction */
        res = PQexec(conn, "COMMIT");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            elog(WARNING, "[scheduler] COMMIT failed: %s", PQerrorMessage(conn));
        }
        PQclear(res);
        
        /* Reset sleep interval after successful cycle */
        current_sleep_us = scheduler_sleep_us;
    }
    
    /* Cleanup (should never reach here) */
    if (conn) PQfinish(conn);
    pfree(conninfo);
    pfree(prev_database);
    pfree(prev_wake_interval);
}