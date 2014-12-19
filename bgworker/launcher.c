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
#include "postmaster/pgworker.h"
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
include "job.h"

#define PROCESS_NAME "elephant launcher"

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

static int 		launcher_naptime = 500;

extern uint32 	launcher_max_workers = 10;
extern char 	*launcher_database;

static struct worker_state
{
	pid_t 					pid;
	dsm_segment 		   *segment;
	BackgroundWorkerHandle *handle;

} worker_state;

worker_state 	*wstate;

static struct db_object_data
{
	const char 	*schema;
	const char 	*namespace;
}

db_object_data    job_table;
db_object_data    log_table;
db_object_data 	  schedule_function;

/* Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
launcher_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	if (MyProc !=NULL)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static void
launcher_sigusr1(SIGNAL_ARGS)
{
	int 		save_errno = errno;

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
	CurrentResourceOwner = ResourceOwnerCreate(NULL, EXTENSION_NAME);
}

static char *
launcher_get_extension_schema(char *extname)
{
	StringInfo 	buf;
	char 		*tmp;
	char 		*schema_name;

	if (!extname)
		return NULL;

	/*
	 * Allocate it outside of the SPI context, so that it's not vanished
	 * after SPI_finish.
	 */
	schema_name = palloc(MAXNAMELEN);

	/* Initialize SPI */
	SetCurrentTransactionTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	InitStringInfo(&buf);
	appendStringInfo(&buf, "SELECT nsp.nspname "
						   "FROM   pg_catalog.pg_namespace nsp JOIN pg_extension ext "
						   "ON (nsp.oid = ext.extnamespace) "
						   "WHERE ext.extname = '%s'", EXTENSION_NAME);

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
	return schema_name;
}

/* Initialize our service table names and schemas */
static void
init_table_names(const char *schema_name)
{
	/* Allocate them in a persistent context */
	MemoryContext    *oldcxt = MemoryContextSwitch(TopTransactionContext);

	log_table.name = quote_identifier(pstrdup("job_log"));
	log_table.schema = quote_identifier(pstrdup(schema_name));

	job_table.name = quote_identifier(pstrdup("job"));
	job_table.schema = quote_identifier(pstrdup(schema_name));

	schedule_function.name = quote_identifier(pstrdup("job_scheduled_at"));
	schedule_function.schema = quote_identifier(pstrdup(schema_name));

	MemoryContextSwitch(odlcxct);
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
		elog(LOG, "worker %d is registered as terminated", pid)

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

/*
 * Launch a new worker and put its data into the launcher slot with a
 * given index.
 */
static void
launch_worker(int index, JobDesc *job_desc)
{
	int  	i;
	BackgroundWorkerHandle  	*handle;
	/* Check if no jobs are running with the same id */
	if (job_desc->parallel == false)
	{
		for (i = 0; i < launcher_max_workers; i++)
		{
			if (i == index || wstate[i].handle == NULL)
				continue;
			if (wstate[i].job_id == job_desc->job_id)
			{
				/*
				 * Another job with the same id, but we only
				 * allow one at a time. Check whether the old
				 * one is still alive
				 */
				 if (check_worker_alive(i))
				 {
				 	elog(WARNING, "could not run multiple instances of job %d: parallel execution is disabled for it");
				 	return;
				 }
			}
		}
	}
	/* prepare the structure to run the job */
	wstate[i].segment = dsm_create(offset(command, JobDesc) + strlen(job_desc->command) + 1);
	fill_job_description(&wstate[i].segment,
						 job_desc->job_id,
						 job_desc->command,
						 job_desc->datname,
						 job_desc->rolname,
						 job_desc->parallel,
						 job_desc->job_timeout);
	wstate[i].job_id = job_desc->job_id;
	/* code to actually launch the worker */

}

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
							  FROM %s.%s()", schedule_function->schema, schedule_function->name);
	pgstat_report_activity(STATE_RUNNING, buf);
	ret = SPI_execute(buf, false, 0);
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
		char   *job_command;

		uint32 	job_id;
		uint32	job_timeout;
		bool 	isnull;
		bool 	parallel;
		MemoryContext *oldcxt;
		JobDesc 	  *job_desc;

		/* fetch interesting attributes */
		job_id = DatumGetUInt32(get_attribute_via_spi(SPI_tuptable, "job_id", &isnull);
		Assert(!isnull);

		job_command = get_text_via_spi(SPI_tuptable, "job_command");
		Assert(len(job_command) > 0);

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
		oldcxt = MemoryContextSwitch(TopTransactionContext);
		job_desc = palloc(offsetof(command, JobDesc) + strlen(job_command) + 1);§
		fill_job_desription(job_desc, job_id, job_command, datname, rolname, parallel, job_timeout);
		MemoryContextSwitch(oldxct);

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
			if (wstate[i].handle == NULL)
			{
				BackgroundWorkerHandle  	*handle;
				/* We found a free slot, let's use it */
				wstate[i].segment = dsm_create(offset(command, JobDesc) + strlen(job_desc->command) + 1);
				
				/* Launch the new worker if we don't have one for the job already*/
				launch_worker(i, job_desc);
			}
		}
	}
}

void launcher_main(Datum arg)
{
	char    *schema_name;
	/* Setup signal handlers */
	pqsignal(SIGHUP, launcher_sighup);
	pqsignal(SIGTERM, launcher_sigterm);
	pqsignal(SIGUSR1, launcher_sigusr1);

	/* Allow signals after the signal handlers have been established */
	BackgroundWorkerUnblockSignals();

	init_launcher();
	BackgroundWorkerInitializeConnnection(launcher_database, NULL);
	schema_name = launcher_get_extension_schema(EXTENSION_NAME);
	if (!schema_name)
		elog(FATAL, "cannot locate %s extension in database %s", EXTENSION_NAME, launcher_database);
	init_table_names(schema_name);

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
		 	check_for_terminated_workers()
		 }
		 run_scheduled_jobs()
	}
}