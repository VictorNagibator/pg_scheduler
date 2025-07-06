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

static char *scheduler_wake_interval = "10s";
static char *scheduler_database = NULL;
static int64 scheduler_sleep_us = 10 * 1000000L;

PGDLLEXPORT void scheduler_main(Datum main_arg);
void _PG_init(void);

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

    DefineCustomStringVariable(
        "scheduler.wake_interval",
        "Interval between scheduler checks",
        NULL,
        &scheduler_wake_interval,
        "10s",
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );

    DefineCustomStringVariable(
        "scheduler.database_name",
        "Database for scheduler jobs",
        NULL,
        &scheduler_database,
        "postgres",
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );

    parse_wake_interval_simple();

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;
    strncpy(worker.bgw_function_name, "scheduler_main", BGW_MAXLEN);
    strncpy(worker.bgw_library_name, "scheduler", BGW_MAXLEN);
    strncpy(worker.bgw_name, "PG Scheduler", BGW_MAXLEN);
    worker.bgw_notify_pid = 0;
    
    RegisterBackgroundWorker(&worker);
}

PGDLLEXPORT void scheduler_main(Datum main_arg) {
    long sleep_ms;
    int event;
    char *conninfo;
    PGconn *conn = NULL;
    
    BackgroundWorkerUnblockSignals();

    if (MyProc == NULL) {
        elog(ERROR, "[scheduler] MyProc is NULL - cannot initialize latch");
        proc_exit(1);
    }

    InitLatch(&MyProc->procLatch);
    elog(LOG, "[scheduler] Latch initialized");

    conninfo = psprintf("dbname='%s'", scheduler_database);
    elog(LOG, "[scheduler] Connection string: %s", conninfo);

    for (;;) {
        if (ProcDiePending) {
            elog(LOG, "[scheduler] ProcDiePending detected, exiting");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            proc_exit(0);
        }

        sleep_ms = scheduler_sleep_us / 1000;
        if (sleep_ms < 10) sleep_ms = 10;

        event = WaitLatch(&MyProc->procLatch,
                          WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                          sleep_ms,
                          0);

        if (event & WL_POSTMASTER_DEATH) {
            elog(LOG, "[scheduler] Postmaster death detected, exiting");
            if (conn) PQfinish(conn);
            pfree(conninfo);
            proc_exit(1);
        }

        if (event & WL_LATCH_SET) {
            ResetLatch(&MyProc->procLatch);
        }

        // Переподключение при необходимости
        if (conn == NULL || PQstatus(conn) != CONNECTION_OK) {
            if (conn) PQfinish(conn);
            conn = PQconnectdb(conninfo);
            if (PQstatus(conn) != CONNECTION_OK) {
                elog(WARNING, "[scheduler] Connection failed: %s", PQerrorMessage(conn));
                PQfinish(conn);
                conn = NULL;
                continue;
            }
        }

        // Начинаем транзакцию для обработки задач
        PGresult *res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            elog(WARNING, "[scheduler] BEGIN failed: %s", PQerrorMessage(conn));
            PQclear(res);
            PQfinish(conn);
            conn = NULL;
            continue;
        }
        PQclear(res);

        // Получаем задачи для выполнения
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
            PQfinish(conn);
            conn = NULL;
            continue;
        }

        int job_count = PQntuples(res);
        elog(LOG, "[scheduler] Found %d jobs to execute", job_count);

        for (int i = 0; i < job_count; i++) {
            char *job_id = PQgetvalue(res, i, 0);
            
            // Выполняем задачу
            char *exec_sql = psprintf("SELECT scheduler.execute_job(%s)", job_id);
            PGresult *exec_res = PQexec(conn, exec_sql);
            
            if (PQresultStatus(exec_res) != PGRES_TUPLES_OK) {
                elog(WARNING, "[scheduler] Job %s execution failed: %s", 
                     job_id, PQerrorMessage(conn));
            } else {
                elog(LOG, "[scheduler] Job %s executed successfully", job_id);
            }
            
            PQclear(exec_res);
            pfree(exec_sql);
            
            // Проверка прерывания после каждой задачи
            if (ProcDiePending) break;
        }
        
        PQclear(res);
        
        // Фиксация транзакции
        res = PQexec(conn, "COMMIT");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            elog(WARNING, "[scheduler] COMMIT failed: %s", PQerrorMessage(conn));
        }
        PQclear(res);
    }
    
    if (conn) PQfinish(conn);
    pfree(conninfo);
}