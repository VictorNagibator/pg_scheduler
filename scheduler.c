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
#include "utils/timestamp.h"  // Для интервалов

static char *scheduler_wake_interval = "10s";
static char *scheduler_database = NULL;
static int64 scheduler_sleep_us = 10 * 1000000L;

PG_MODULE_MAGIC;

PGDLLEXPORT void scheduler_main(Datum main_arg);
void _PG_init(void);

// Упрощенная функция парсинга интервала
static void parse_wake_interval_simple(void) {
    char *endptr;
    long sec = strtol(scheduler_wake_interval, &endptr, 10);
    
    if (endptr == scheduler_wake_interval) {
        // Не удалось распознать число, используем значение по умолчанию
        elog(WARNING, "Invalid interval format, using default 10s");
        scheduler_sleep_us = 10 * 1000000L;
        return;
    }
    
    // Определяем единицы измерения
    if (strcmp(endptr, "s") == 0 || *endptr == '\0') {
        scheduler_sleep_us = sec * 1000000L;
    } else if (strcmp(endptr, "min") == 0) {
        scheduler_sleep_us = sec * 60 * 1000000L;
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

    // Используем упрощенный парсер
    parse_wake_interval_simple();

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;
    strncpy(worker.bgw_function_name, "scheduler_main", BGW_MAXLEN);
    strncpy(worker.bgw_library_name, "scheduler", BGW_MAXLEN);
    strncpy(worker.bgw_name, "PG Scheduler", BGW_MAXLEN);
    
    RegisterBackgroundWorker(&worker);
}


PGDLLEXPORT void scheduler_main(Datum main_arg) {
    long sleep_ms;
    int event;
    
    BackgroundWorkerUnblockSignals();

    if (MyProc == NULL) {
        elog(ERROR, "[scheduler] MyProc is NULL - cannot initialize latch");
        proc_exit(1);
    }

    elog(LOG, "[scheduler] Initializing latch");
    InitLatch(&MyProc->procLatch);
    elog(LOG, "[scheduler] Latch initialized successfully");

    // Формируем строку подключения
    char *conninfo = psprintf("dbname='%s'", scheduler_database);
    elog(LOG, "[scheduler] Connection string: %s", conninfo);

    for (;;) {
        // Проверка сигнала завершения
        if (ProcDiePending) {
            elog(LOG, "[scheduler] ProcDiePending detected, exiting");
            pfree(conninfo);
            proc_exit(0);
        }

        sleep_ms = scheduler_sleep_us / 1000;
        if (sleep_ms < 10) sleep_ms = 10;

        elog(LOG, "[scheduler] Waiting for event (timeout: %ld ms)", sleep_ms);
        event = WaitLatch(&MyProc->procLatch,
                          WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                          sleep_ms,
                          0);
        elog(LOG, "[scheduler] WaitLatch returned event: %d", event);

        if (event & WL_POSTMASTER_DEATH) {
            elog(LOG, "[scheduler] Postmaster death detected, exiting");
            pfree(conninfo);
            proc_exit(1);
        }

        if (event & WL_LATCH_SET) {
            elog(LOG, "[scheduler] Latch set detected, resetting latch");
            ResetLatch(&MyProc->procLatch);
        }

        // Устанавливаем подключение к базе данных
        elog(LOG, "[scheduler] Connecting to database");
        PGconn *conn = PQconnectdb(conninfo);
        
        if (PQstatus(conn) != CONNECTION_OK) {
            elog(WARNING, "[scheduler] Connection failed: %s", PQerrorMessage(conn));
            PQfinish(conn);
            continue;
        }
        elog(LOG, "[scheduler] Successfully connected to database");

        // Выполняем простой запрос
        elog(LOG, "[scheduler] Executing test query: SELECT 1");
        PGresult *res = PQexec(conn, "SELECT 1");
        
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            elog(WARNING, "[scheduler] Query failed: %s", PQerrorMessage(conn));
        } else {
            elog(LOG, "[scheduler] Query executed successfully. Returned %d rows", PQntuples(res));
        }
        
        // Очищаем результат
        PQclear(res);
        
        // Закрываем подключение
        elog(LOG, "[scheduler] Closing connection");
        PQfinish(conn);
    }
    
    // Очистка (никогда не выполнится, но для полноты)
    pfree(conninfo);
}