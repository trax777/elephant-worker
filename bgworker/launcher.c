/* ------------------------------------------------------------------------
 * launcher.c
 *  	Implementation of the launcher process, watching the clock and.
 * 		forking the worker processes to execute jobs on time.
 *
 * Copyright (c) 2014, Zalando SE.
 * Portions Copyright (C) 2013-2014, PostgreSQL Global Development Group
 * ------------------------------------------------------------------------
 */

#include "postgres.h"

/* bgworker mandatory includes */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

 /* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

/* Our own include files */
#include "commons.h"
#include "job.h"
#include "worker.h"

#define PROCESS_NAME "elephant launcher"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(launcher_main);

void _PG_init(void);


static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

static uint32 	launcher_naptime = 500;

extern uint32 	launcher_max_workers = 10;
static char 	*launcher_database = NULL;

typedef struct worker_state
{
	pid_t 					pid;
	uint32 					job_id;
	pg_time_t 				last_executed;
	dsm_segment 		   *segment;
	BackgroundWorkerHandle *handle;
} worker_state;

static worker_state 	*wstate;

static char 			 schema_name[MAXNAMELEN];

static db_object_data    job_table;
static db_object_data    log_table;
static db_object_data 	 schedule_function;


static Datum
get_attribute_via_spi(SPITupleTable *tuptable, const char *colname, bool *isnull)
{
	return SPI_getbinval(tuptable->vals[i], tuptable->tupdesc, SPI_fnumber(tuptable->tupdesc, colname), isnull)
}

static char *
get_text_via_spi(SPITupleTable *tuptable, const char *colname)
{
	return SPI_gevalue(tuptable->vals[i], tuptable->tupdesc, SPI_fnumber(tuptable->tupdesc, colname));
}

/* Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
launcher_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	if (MyProc != NULL)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* Signal handler for SIGUSR1
* 		Set a flag to check for the termination of child processes */
static void
launcher_sigusr1(SIGNAL_ARGS)
{
	int 		save_errno = errno;

	/* Necessary for the latches to work properly, as they are using sigusr1 internally */
	latch_sigusr1_handler()

	got_sigusr1 = true;

	if (set_latch_on_sigusr1 && MyProc != NULL)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
launcher_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;

	if (MyProc != NULL)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static void
init_launcher()
{
	int 	i;

	/* allocate the workers state in the global context */
	wstate = MemoryContextAlloc(TopTransactionContext,
								sizeof(worker_state) * launcher_max_workers);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, PROCESS_NAME);
}

static char *
launcher_get_extension_schema(char *extname)
{
	StringInfo 	buf;
	char 		*tmp;

	if (!extname)
		return NULL;

	/* Initialize SPI */
	SetCurrentTransactionTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	InitStringInfo(&buf);
	appendStringInfo(&buf, "SELECT nsp.nspname "
						   "FROM   pg_catalog.pg_namespace nsp JOIN pg_catalog.pg_extension ext "
						   "ON (nsp.oid = ext.extnamespace) "
						   "WHERE ext.extname = %s", quote_literal_cstr(EXTENSION_NAME));

	pgstat_report_activity(STATE_RUNNING, buf.data);
	/* Query system catalogs for the given extension */

	if (SPI_execute(buf.data, false, 1) != SPI_OK_SELECT || SPI_processed == 1)
		elog(FATAL, "could not query system catalogs for extension %s", EXTENSION_NAME);
	tmp = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, MAXNAMELEN);
	if (!tmp)
		elog(FATAL, "%s returned NULL result");
	strncpy(schema_name, tmp, MAXNAMELEN));

	/* finish the SPI query */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

/* Initialize our service table names and schemas */
static void
init_table_names()
{
	/* Allocate them in a persistent context */
	MemoryContext    *oldcxt = MemoryContextSwitchTo(TopTransactionContext);

	log_table.name = quote_identifier("job_log");
	log_table.schema = quote_identifier(schema_name);

	job_table.name = quote_identifier("job");
	job_table.schema = quote_identifier(schema_name);

	schedule_function.name = quote_identifier("job_scheduled_at");
	schedule_function.schema = quote_identifier(schema_name);

	MemoryContextSwitchTo(odlcxct);
}

bool check_worker_alive(int i)
{
	pid_t 	pid;

	if (wstate[i.handle == NULL)
		return false;
	else if (GetBackgroundWorkerPid(handle, &pid) != BGWH_STARTED)
	{
		/* cleanup */
		pfree(wstate[i].hanlde);
		wstate[i].handle = NULL;
		dsm_detach(wstate[i].segment);
		elog(LOG, "worker %d has terminated", pid)

		return false;
	}
	return true;
}

/* Cleanup after workers that terminated. */
static void
check_for_terminated_workers()
{
	int 	i;

	/* Workers are terminated by the postmaster, we are signaled
	 * afterwards and need to query each one's handle and release
	 * resources for those that are marked as done.
	 */
	for (i = 0; i < launcher_max_workers; i++)
		check_worker_alive(i);
}

/*
 * Launch a new worker and put its data into the launcher slot with a
 * given index.
 */
static void
launch_worker(int index, JobDesc *job_desc)
{
	int  	j;
	bool 			started;
	pg_time_t		last_executed;
	dsm_segment    *segment;
	BackgroundWorker 			worker;
	BackgroundWorkerHandle     *handle;

	/* Check if no jobs are running with the same id */
	for (j = 0; j < launcher_max_workers; j++)
	{
		if (j == index || wstate[j].handle == NULL)
			continue;
		if (wstate[j].job_id == job_desc->job_id)
		{
		 	pg_time_t 	now = (pg_time_t) time(NULL);
		 	/* Check if we are trying to run the same job for the second time in the duration of a single minute */
		 	if (wstate[j].last_executed/60 == now/60)
		 		return;
			/*
			 * Another job with the same id, but we only
			 * allow one at a time. Check whether the old
			 * one is still alive
			 */
			 if (!job_desc->parallel && check_worker_alive(j))
			 {
			 	elog(WARNING, "could not run multiple instances of job %d: parallel execution is disabled for it", job_desc->job_id);
			 	return;
			 }
		}
	}
	/* copy the job information to shared memory */
	segment = dsm_create(sizeof(JobDesc));

	memcpy(dsm_segment_address(segment), job_desc, sizeof(JobDesc));
	/* prepare the information to actually launch the worker */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;
	sprintf(worker.bgw_library_name, EXTENSION_NAME);
	sprintf(worker.bgw_function_name, "worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "worker %d", job_desc->job_id);
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(segment));
	worker.bgw_notify_pid = MyProcPid;

	started = false;
	last_executed = (pg_time_t) time(NULL);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(WARNING, "could not register dynamic background worker for job %d", job_desc->job_id);
	else
	{
		pid_t 	pid;
		BgwHandleStatus 	status;

		status = WaitForBackgroundWorkerStartup(handle, &pid);

		if (status == BGWH_STOPPED)
			ereport(WARNING,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not start background process"),
				     errhint("More details may be available in the server log.")));

		if (status == BGWH_POSTMASTER_DIED)
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					     errmsg("cannot start background processes without postmaster"),
						 errhint("Kill all remaining database processes and restart the database.")));

		if (status == BGWH_STARTED)
		{
			elog(LOG, "started a worker for job %d", job_desc->job_id);

			started = true;
			wstate[index].segment = segment;
			wstate[index].handle = handle;
			wstate[index].pid = pid;
			wstate[index].job_id = job_desc->job_id;
			wstate[index].last_executed = last_executed;
		}
	}
	if (!started)
	{
		/* cleanup the resource we've allocated */
		dsm_detach(segment);
		wstate[i].handle = NULL;
	}
}

/* Check if there are jobs scheduled to run and spawn worker subprocesses to run them. */
static void run_scheduled_jobs()
{
	StringInfo 	buf;
	int 		ret;
	int 		i;
	List 		*scheduled_jobs;
	ListCell  	*lc;

	/* First, check if there are jobs to run */
	SetCurrentTransactionTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	InitStringInfo(&buf);
	appendStringInfo(&buf, "SELECT job_id,
								   parallel,
								   extract(epoch from job_timeout) as job_timeout,
								   datname,
								   rolname
							  FROM %s.%s()",
							  schedule_function->schema,
							  schedule_function->name);
	pgstat_report_activity(STATE_RUNNING, buf);
	ret = SPI_execute(buf.data, false, 0);
	if (ret < 0)
		elog(FATAL, "cannot obtain list of jobs to run");
	/* No scheduled jobs at this time, check back later */
	if (SPI_processed == 0)
		return;

	/* Get the oids of jobs to run */
	for (i = 0; i < SPI_processed; i++)
	{
		char   *datname;
		char   *rolname;

		uint32 	job_id;
		uint32	job_timeout;
		bool 	isnull;
		bool 	parallel;
		MemoryContext *oldcxt;
		JobDesc 	  *job_desc;

		/* fetch interesting attributes */
		job_id = DatumGetUInt32(get_attribute_via_spi(SPI_tuptable, "job_id", &isnull);
		Assert(!isnull);

		datname = get_text_via_spi(SPI_tuptable, "datname");
		Assert(len(datname) > 0);

		rolname = get_text_via_spi(SPI_tuptable, "rolname");
		Assert(len(rolname)) > 0;


		parallel = DatumGetBool(get_attribute_via_spi(SPI_tuptable, "parallel"), &isnull);
		Assert(!isnull);

		job_timeout = DatumGetInt32(get_attribute_via_spi(SPI_tuptable, "job_timeout"), &isnull);
		Assert(!isnull);

		/*
		 * Allocate a new job description in the top transaction context, so it does not go away
		 * after SPI_finish.
		 */
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		job_desc = palloc(sizeof(JobDesc));
		fill_job_description(job_desc, job_id, 0, datname, rolname, schema_name, parallel, job_timeout);
		MemoryContextSwitchTo(oldxct);

		scheduled_jobs = lappend(scheduled_jobs, job_desc);
	}
	/* We are done with the database, finish the SPI call */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	/* Now launch the child processes */
	foreach(lc, scheduled_jobs)
	{
		job_desc = lfirst(lc);
		for (i = 0; i < launcher_max_workers; i++)
		{
			/* Look for a first free slot */
			if (wstate[i].handle == NULL)
			{
				/* Launch the new worker if we don't have one for the job already*/
				launch_worker(i, job_desc);
				break
			}
		}
		elog(WARNING, "unable to launch more job: all available worker slots are occupied",
					  (errhint("Increase the elephant_worker.max_worker value")));
		break;
	}
	list_free_deep(scheduled_jobs);
}

void launcher_main(Datum arg)
{
	/* Setup signal handlers */
	pqsignal(SIGHUP, launcher_sighup);
	pqsignal(SIGTERM, launcher_sigterm);
	pqsignal(SIGUSR1, launcher_sigusr1);

	/* Allow signals after the signal handlers have been established */
	BackgroundWorkerUnblockSignals();

	init_launcher();
	BackgroundWorkerInitializeConnnection(launcher_database, NULL);
	launcher_get_extension_schema(EXTENSION_NAME);
	init_table_names();

	/* loop until SIGTERM will command us to exit */
	while (!got_sigterm)
	{
		int 	ret;
		int 	rc;

		/*
		 * Sleep on a latch until we are signaled, timed out or the postmaster dies.
		 * Default sleep interval is 0.5 second so that we'll be able to check the job
		 * schedule every second.
		 */
		 rc = WaitLatch(&MyProc->procLatch,
		 				WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
		 				launcher_naptime);
		 ResetLatch(&MyProc->procLatch);

		 /* Emergency exit */
		 if (rc & WL_POSTMASTER_DEATH)
		 	proc_exit(1);

		 /* Whether we need to reload the configuration */
		 if (got_sighup)
		 {
		 	got_sighup = false;
		 	ProcessConfigFile(PGC_SIGHUP);
		 }

		 if (got_sigusr1)
		 {
		 	got_sigusr1 = false;
		 	check_for_terminated_workers();
		 }
		 run_scheduled_jobs();
	}
}

static bool
check_launcher_max_workers(int *newval, void **extra, GucSource source)
{
	if (*newval >= max_worker_processes)
		return false;
	return true;
}

/* Entry point for the shared library, start the launcher process */
void _PG_init(void)
{
	BackgroundWorker 	worker;

	/* Should be started from the postgresql.conf */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Define our customer variables */
	DefineCustomIntVariable("elephant_worker.max_workers",
							"Maximum number of worker child worker processes",
							NULL,
							&launcher_max_workers,
							5,
							1,
							MAX_BACKENDS,
							PGC_POSTMASTER,
							0,
							check_launcher_max_workers,
							NULL,
							NULL);

	DefineCustomIntVariable("elephant_worker.launcher_naptime",
							"time in ms that launcher sleeps before checking for jobs",
							NULL,
							&launcher_naptime,
							500,
							100,
							900,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("elephant_worker.database",
							   "database system to run the extension in",
							   NULL,
							   &launcher_database,
							   "postgres",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

   /* Setup common flags for the launcher */
   worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
   worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
   worker.bgw_main = launcher_main;
   worker.bgw_notify_pid = 0;
   snprintf(worker.bgw_name, BGW_MAXLEN, PROCESS_NAME);
   worker.bgw_main_arg = (Datum) 0;

   RegisterBackgroundWorker(&worker);
}
