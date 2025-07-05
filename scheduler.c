/* scheduler.c */

#include <string.h>                /* memset */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"             /* ProcDiePending */
#include "postmaster/bgworker.h"   /* BackgroundWorker, RegisterBackgroundWorker */
#include "utils/guc.h"             /* DefineCustomStringVariable */
#include "executor/spi.h"          /* SPI_connect, SPI_execute, SPI_finish */
#include "utils/elog.h"            /* elog */
#include "utils/builtins.h"        /* DirectFunctionCall3, CStringGetDatum */
#include "utils/timestamp.h"        /* interval_in, Interval */
#include "utils/datetime.h"        /* interval_in, Interval */
#include "storage/ipc.h"           /* ProclDiePending, etc. */
#include "storage/proc.h"          /* ProcDiePending */

PG_MODULE_MAGIC;

/* GUC‑переменные */
static char *scheduler_wake_interval = NULL;
static char *scheduler_database      = NULL;

/* Экспортируемая точка входа */
PGDLLEXPORT void scheduler_main(Datum main_arg) pg_attribute_noreturn();

/* Прототип init‑функции */
void _PG_init(void);

/*
 * _PG_init: определяем GUC и регистрируем BGWorker
 */
void
_PG_init(void)
{
    BackgroundWorker worker;

    /* обнуляем всю структуру перед заполнением */
    memset(&worker, 0, sizeof(worker));

    /* GUC: интервал пробуждения */
    DefineCustomStringVariable(
        "scheduler.wake_interval",
        "Time between scheduler wakeups (e.g. '1min','30s')",
        NULL,
        &scheduler_wake_interval,
        "1min",
        PGC_POSTMASTER, 0,
        NULL, NULL, NULL
    );

    /* GUC: имя БД для подключения */
    DefineCustomStringVariable(
        "scheduler.database_name",
        "Database in which scheduler jobs will run",
        NULL,
        &scheduler_database,
        "",  /* если пусто — current_database */
        PGC_POSTMASTER, 0,
        NULL, NULL, NULL
    );

    /* Заполняем bgworker-поля, доступные в PG 16 */
    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS
                            | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;

    /* Имя .so и функция внутри него */
    snprintf(worker.bgw_library_name,  BGW_MAXLEN, "scheduler");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "scheduler_main");
    snprintf(worker.bgw_name,          BGW_MAXLEN, "pg_scheduler");

    /* Регистрируем воркер */
    RegisterBackgroundWorker(&worker);

    elog(LOG, "[scheduler] background worker registered");
}

/*
 * scheduler_main: тело фонового воркера
 */
PGDLLEXPORT void
scheduler_main(Datum main_arg)
{
    /* Разрешаем обработку сигналов */
    BackgroundWorkerUnblockSignals();

    /* Подключаемся к указанной БД или к текущей, если GUC пуст */
    BackgroundWorkerInitializeConnection(
        (scheduler_database[0] ? scheduler_database : NULL),
        NULL,
        0
    );

    elog(LOG, "[scheduler] started");

    for (;;)
    {
        /* Выход по SIGTERM */
        if (ProcDiePending)
            proc_exit(0);

        /* Заснуть на заданный интервал */
        {
            Datum      iv = DirectFunctionCall3(
                interval_in,
                CStringGetDatum(scheduler_wake_interval),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(-1)
            );
            Interval  *interval = DatumGetIntervalP(iv);
            uint64     usec = (uint64)interval->day * 86400 * 1000000L
                            + (uint64)interval->time;
            pg_usleep(usec);
        }

        if (ProcDiePending)
            proc_exit(0);

        /* Тик для отладки: замените на логику SPI/выборки jobs */
        elog(LOG, "[scheduler] tick");

        /* Здесь вызовете SPI_connect(), SPI_execute(...) и т.д. */
    }
}
