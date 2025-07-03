/* contrib/pg_scheduler/scheduler.c */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"            /* ProcDiePending */
#include "postmaster/bgworker.h"
#include "utils/guc.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "storage/ipc.h"

/* Explicit declaration */
extern void proc_exit(int code);

#ifndef PGDLLEXPORT
#define PGDLLEXPORT extern
#endif

PG_MODULE_MAGIC;

/* GUCs */
static char *scheduler_wake_interval = NULL;
static char *scheduler_database      = NULL;

/* Prototypes */
void    _PG_init(void);
PGDLLEXPORT void scheduler_main(Datum) pg_attribute_noreturn();

/*
 * _PG_init: define GUC and register the BGWorker.
 */
void
_PG_init(void)
{
    BackgroundWorker worker;

    DefineCustomStringVariable(
        "scheduler.wake_interval",
        "Time between scheduler wakeups (e.g. '1min','30s')",
        NULL,
        &scheduler_wake_interval,
        "1min",
        PGC_POSTMASTER, 0,
        NULL, NULL, NULL
    );

    /* База, в которой создавали расширение */
    DefineCustomStringVariable(
        "scheduler.database_name",
        "Database in which scheduler jobs will run",
        NULL,
        &scheduler_database,
        "",                     /* если пусто — устанавливать сами */
        PGC_POSTMASTER,
        0, NULL, NULL, NULL
    );
    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS
                            | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    snprintf(worker.bgw_library_name,  BGW_MAXLEN, "scheduler");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "scheduler_main");
    snprintf(worker.bgw_name,          BGW_MAXLEN, "pg_scheduler");

    RegisterBackgroundWorker(&worker);
    elog(LOG, "[scheduler] background worker registered");
}

/*
 * scheduler_main: entry point of the BGWorker
 */
PGDLLEXPORT void
scheduler_main(Datum main_arg)
{
    /* allow interruptions */
    BackgroundWorkerUnblockSignals();

    /* connect to the configured database */
    BackgroundWorkerInitializeConnection(
        scheduler_database[0] ? scheduler_database : NULL,
        NULL,
        0
    );

    for (;;)
    {
        /* exit on SIGTERM */
        if (ProcDiePending)
            proc_exit(0);

        /* sleep */
        {
            Datum iv = DirectFunctionCall3(
                interval_in,
                CStringGetDatum(scheduler_wake_interval),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(-1)
            );
            Interval *interval = DatumGetIntervalP(iv);
            uint64 usec = (uint64) interval->day * 86400 * 1000000L
                        + (uint64) interval->time;
            pg_usleep(usec);
        }

        if (ProcDiePending)
            proc_exit(0);

        PG_TRY();
        {
            /* connect to SPI */
            if (SPI_connect() != SPI_OK_CONNECT)
            {
                elog(WARNING, "[scheduler] SPI_connect failed");
                /* nothing to finish */
                continue;
            }

            /* fetch due jobs */
            if (SPI_execute(
                "SELECT job_id, job_type, command, schedule_interval "
                "  FROM scheduler.jobs "
                " WHERE enabled AND next_run <= now()",
                true, 0) < 0)
            {
                elog(WARNING, "[scheduler] could not fetch jobs");
                SPI_finish();
                continue;
            }

            /* process each job */
            for (int i = 0; i < SPI_processed; i++)
            {
                TupleDesc tupdesc = SPI_tuptable->tupdesc;
                HeapTuple tuple   = SPI_tuptable->vals[i];
                int       job_id  = DatumGetInt32(
                                    SPI_getbinval(tuple, tupdesc, 1, NULL));
                char    *job_type = TextDatumGetCString(
                                    SPI_getbinval(tuple, tupdesc, 2, NULL));
                char    *command  = TextDatumGetCString(
                                    SPI_getbinval(tuple, tupdesc, 3, NULL));
                bool     isnull;
                Datum    dint     = SPI_getbinval(tuple, tupdesc, 4, &isnull);
                Interval *interval= isnull ? NULL : DatumGetIntervalP(dint);

                /* execute job */
                if (strcmp(job_type, "sql") == 0)
                {
                    if (SPI_execute(command, false, 0) != SPI_OK_SELECT)
                        elog(WARNING, "[scheduler] SQL job %d failed: %s",
                             job_id, command);
                }
                else /* shell */
                {
                    int rc = system(command);
                    if (rc != 0)
                        elog(WARNING, "[scheduler] Shell job %d failed (%d): %s",
                             job_id, rc, command);
                }

                /* update last_run and next_run or disable */
                {
                    StringInfoData upd;
                    initStringInfo(&upd);
                    appendStringInfoString(&upd,
                        "UPDATE scheduler.jobs SET last_run = now(), ");
                    if (interval)
                    {
                        appendStringInfo(&upd,
                            "next_run = now() + interval '%s' ",
                            TextDatumGetCString(dint));
                    }
                    else
                    {
                        appendStringInfoString(&upd,
                            "enabled = FALSE, next_run = NULL ");
                    }
                    appendStringInfo(&upd,
                        "WHERE job_id = %d", job_id);

                    if (SPI_execute(upd.data, false, 0) < 0)
                        elog(WARNING, "[scheduler] could not update job %d",
                             job_id);
                }
            }

            SPI_finish();
        }
        PG_CATCH();
        {
            /* log and continue */
            ErrorData *err = CopyErrorData();
            FlushErrorState();
            elog(WARNING, "[scheduler] iteration failed: %s", err->message);
            FreeErrorData(err);
            /* ensure SPI is cleaned up */
            SPI_finish();
        }
        PG_END_TRY();
    }
}
