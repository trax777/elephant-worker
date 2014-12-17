/* -------------------------------------------------------------------------
 *
 * worker_spi.c
 *		Sample background worker code that demonstrates various coding
 *		patterns: establishing a database connection; starting and committing
 *		transactions; using GUC variables, and heeding SIGHUP to reread
 *		the configuration file; reporting to pg_stat_activity; using the
 *		process latch to sleep and exit in case of postmaster death.
 *
 * This code connects to a database, creates a schema and table, and summarizes
 * the numbers contained therein.  To see it working, insert an initial value
 * with "total" type and some initial value; then insert some other rows with
 * "delta" type.  Delta rows will be deleted by this worker and their values
 * aggregated into the total.
 *
 * Copyright (C) 2013-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/worker_spi/worker_spi.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
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
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/resowner_private.h"
#include "tcop/utility.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(worker_spi_launch);

/* Identifier for shared memory segments used by this extension */
#define WORKER_SPI_SHM_MAGIC 	0x9fa529e1
/* Maximum length of the error message */
#define MAX_ERROR_MESSAGE_LEN 	1024

void		_PG_init(void);
void		worker_spi_main(Datum) __attribute__((noreturn));

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

/* GUC variables */
static int	worker_spi_naptime = 10;
static int	worker_spi_total_workers = 2;
static int  launcher_spi_naptime = 500;
static int launcher_max_workers = 5;
/* whether we were run by the launcher and not directly by the user */
static bool LauncherChild = false;


typedef struct worktable
{
	const char *schema;
	const char *name;
} worktable;

typedef struct
{
	slock_t		mutex;
	uint32 	slotno;
	uint32 	index;
	bool  	consumed;
} WorkerCallHeader;


/*
 * A worker should communicate to us about the exit status, SQL state and the
 * error message in case an error occured. For this, we could use a queue, but
 * that would involve dealing with quite complex and poorly documented shm_mq
 * data structure. Instead, we can use a single array of char per each worker,
 * keeping track of the error code, SQL state and the error message. 
 * We don't need to deal with locks, since only worker is supposed to write to
 * the array and only launcher will read from it, and only when it knows that
 * the worker has already terminated, thus, no given processes will read the
 * data from it simultaneously.
 */
typedef struct
{
	int 	workers_total;
	int 	workers_active;
	dsm_segment   *data;
	shm_toc 	  *toc;
	WorkerCallHeader *hdr;
	BackgroundWorkerHandle *worker_handles[FLEXIBLE_ARRAY_MEMBER];
} LauncherState;

static LauncherState   *launcher;

/*
 * Each background worker can propagate its exitcode, sqlstate and errormessage.
 * The launcher will read the slot only if the contents is not consimed, setting
 * the consumed flag right after it. The worker will reset the consumed flag
 * upon writing to the slot.
 */
typedef struct
{
	bool 	consumed;
	int 	exitcode;
	int 	sqlstate;
	char 	errormessage[MAX_ERROR_MESSAGE_LEN];
} WorkerState;

/* Feedback data area per each worker to return sql codes and error messages to the launcher */
static WorkerState  *worker;

/* Forward static declarations */
static LauncherState *setup_launcher(dsm_segment *seg, shm_toc *toc, WorkerCallHeader *hdr, int nworkers);
static bool launch_workers(int count, int *indexes);
static void terminate_workers();
static void cleanup_on_workers_exit();
static BackgroundWorkerHandle *worker_spi_launch_internal(uint32 segment, int index, pid_t *retpid);

/*
 * Setup the shared memory segment. Creates toc and allocates nworkers * worker states
 */
static void
setup_dynamic_shared_memory(int nworkers)
{
	shm_toc_estimator e;
	int 			i;
	dsm_segment    *seg;
	shm_toc  	   *toc;
	WorkerCallHeader *hdr;
	WorkerState *ptr;

	Size 			segsize;
	Size 			data_size = sizeof(WorkerState);
	Size 			header_size = sizeof(WorkerCallHeader);

	/* Estimate how much shared memory we need */

	shm_toc_initialize_estimator(&e);

	/* Because the TOC machinery may choose to insert padding of oddly-sized
	 * requests we must estimate each chunk separately. We need to register
	 * nworkers keys to track the same number of shared segments.
	 */
	shm_toc_estimate_chunk(&e, header_size);
	for (i = 0; i < nworkers; i++)
		shm_toc_estimate_chunk(&e, data_size);

	shm_toc_estimate_keys(&e, nworkers + 2);
	segsize = shm_toc_estimate(&e);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "spi_launcher");
	/* Create the shared memory and establish a table of contents */
	seg = dsm_create(segsize);
	/* clear the memory, so that the consumed flag is set to false by default */
	memset(dsm_segment_address(seg), 0, segsize);
	toc = shm_toc_create(WORKER_SPI_SHM_MAGIC, dsm_segment_address(seg), segsize);
	hdr = shm_toc_allocate(toc, header_size);
	SpinLockInit(&hdr->mutex);
	shm_toc_insert(toc, 0, hdr);
	for (i = 1; i <= nworkers; i++)
	{
		/* allocate a space for a given slot */
		ptr = shm_toc_allocate(toc, data_size);
		shm_toc_insert(toc, i, ptr);
	}
	launcher = setup_launcher(seg, toc, hdr, nworkers);
}

/*
 * Setup the initial worker state, no workers are active */
static LauncherState *
setup_launcher(dsm_segment *seg, shm_toc *toc, WorkerCallHeader *hdr, int nworkers)
{
	int 	i;
	LauncherState *result = palloc(offsetof(LauncherState, worker_handles) +
								 nworkers * sizeof(BackgroundWorkerHandle *));
	result->workers_total = nworkers;
	result->workers_active = 0;
	result->data = seg;
	result->hdr = hdr;
	result->toc = toc;
	for (i = 0; i < nworkers; i++)
		result->worker_handles[i] = NULL;
	return result;
}

/* Attach to the shared memory segment from the worker perspective */
static void
worker_attach_to_shared_memory(int segmentno, int *index)
{
	WorkerState    		 *result;
	volatile WorkerCallHeader *hdr;
	dsm_segment 		  *seg;
	shm_toc 			  *toc;
	int  				  slotno;

	/* shared memory is not used in a stand-alone worker */
	if (!LauncherChild)
		return;

	/* Connect to dynamic shared memory segment
	 *
	 * In order to attach a dynamic shared memory, we need a resource owner.
	 * Once we've mapped the segment in our address space, attach to the table
	 * of contents so we can locate the feedback data area within the segment.
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "spi_worker");
	seg = dsm_attach(segmentno);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unable to map dynamic shared memory segment")));
	toc = shm_toc_attach(WORKER_SPI_SHM_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	hdr = shm_toc_lookup(toc, 0);
	SpinLockAcquire(&hdr->mutex);
	*index = hdr->index;
	slotno = hdr->slotno;
	hdr->consumed = true;
	SpinLockRelease(&hdr->mutex);
	elog(LOG, "attaching to shared memory segment: %d index: %d", segmentno, *index);
	/* get our feedback data area */
	result = shm_toc_lookup(toc, slotno + 1);
	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unable to fetch worker feedback state area from the dynamic shared memory segment")));
	worker = result;
	/*
	 * Initialize the worker structure.
	 * The state is set consumed, no news from the worker to the launcher at start.
	 */
	worker->consumed = true;
	worker->exitcode = 0;
	worker->sqlstate = 0;
	worker->errormessage[0] = '\0';
}

static void
worker_report_feedback(int exitcode, int sqlstate, char *msg)
{
	Assert(worker != NULL);
	worker->consumed = false;
	worker->exitcode = exitcode;
	worker->sqlstate = sqlstate;
	if (msg)
		snprintf(worker->errormessage, MAX_ERROR_MESSAGE_LEN - 1, "%s", msg);
}

/* attach to the given slot and fetch the feedback from a worker */
static WorkerState *
get_worker(int slotno)
{
	Assert(launcher != NULL);
	WorkerState *result = shm_toc_lookup(launcher->toc, slotno + 1);
	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unable to fetch worker feedback state area from the dynamic shared memory segment")));
	return result;
}

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
worker_spi_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
worker_spi_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static void
launcher_spi_sigusr1(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigusr1 = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Initialize workspace for a launcher process: create the schema if it doesn't
 * already exist.
 */
 static void
 initialize_launcher_spi(worktable *table)
 {
 	int 		ret;
 	int 		ntup;
 	bool 		isnull;
 	StringInfoData buf;

 	SetCurrentStatementStartTimestamp();
 	StartTransactionCommand();
 	SPI_connect();
 	PushActiveSnapshot(GetTransactionSnapshot());
 	pgstat_report_activity(STATE_RUNNING, "initialize spi launcher schema");

 	initStringInfo(&buf);
 	appendStringInfo(&buf,
 					 "CREATE SCHEMA IF NOT EXISTS %s;"
 					 "CREATE TABLE IF NOT EXISTS %s.%s ("
 					  "process_id INTEGER PRIMARY KEY CHECK (process_id >= 0))",
					 table->schema, table->schema, table->name);
 	/* set statement star time */
 	SetCurrentStatementStartTimestamp();

 	ret = SPI_execute(buf.data, false, 0);
 	if (ret != SPI_OK_UTILITY)
 		elog(FATAL, "failed to create launcher schema");

 	SPI_finish();
 	PopActiveSnapshot();
 	CommitTransactionCommand();
 	pgstat_report_activity(STATE_IDLE, NULL);
 }

 void launcher_spi_main(Datum arg)
 {
 	worktable  *table;
 	StringInfoData buf;
 	char 		name[20];
 	int 	   *to_launch;
 	List 	   *worker_list = NIL;
 	dsm_segment *segment;
 	ListCell   *lc;

 	table = palloc(sizeof(worktable));
 	sprintf(name, "public");
 	table->schema = quote_identifier(name);
 	table->name = quote_identifier("launcher_child");

 	/* Establish signal handlers before unblocking signals. */
 	pqsignal(SIGHUP, worker_spi_sighup);
 	pqsignal(SIGTERM, worker_spi_sigterm);
 	pqsignal(SIGUSR1, launcher_spi_sigusr1);

 	/* We are now ready to receive signals */
 	BackgroundWorkerUnblockSignals();

 	setup_dynamic_shared_memory(launcher_max_workers);

 	/* Connect to the postgres database as a superuser*/
 	BackgroundWorkerInitializeConnection("postgres", NULL);

 	elog(LOG, "%s initialized with %s.%s",
 		 MyBgworkerEntry->bgw_name, table->schema, table->name);
 	pgstat_report_appname(MyBgworkerEntry->bgw_name);
 	initialize_launcher_spi(table);

 	initStringInfo(&buf);
 	appendStringInfo(&buf,
 					  "DELETE FROM %s.%s RETURNING process_id",
 					 table->schema, table->name);
 	/*
 	 * Main loop: do this, until the SIGTERM handler tells us to terminate
 	 */
 	while (!got_sigterm)
 	{
 		int 		ret;
 		int 		rc;
 		int 		i;
 		int 		processed;

 		/*
 		 * Background workers mustn' call usleep() or any direct equivalent:
 		 * instead, they may wait on their process latch, which sleeps as
 		 * necessary, but is awakened if postmaster dies. That way the
 		 * background process goes away immediately in an emergency.
 		 */
 		 rc = WaitLatch(&MyProc->procLatch,
 		 				WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
 		 				launcher_spi_naptime);
 		 ResetLatch(&MyProc->procLatch);

 		 /* emergency bailout if postmaster has died */
 		 if (rc & WL_POSTMASTER_DEATH)
 		 	proc_exit(1);

 		 /*
 		  * In case of a SIGHUP, just reload the configuration
 		  */
 		 if (got_sighup)
 		 {
 		 	got_sighup = false;
 		 	ProcessConfigFile(PGC_SIGHUP);
 		 }
 		 /* Start a transaction on which we can run queries */
 		 SetCurrentStatementStartTimestamp();
 		 StartTransactionCommand();
 		 SPI_connect();
 		 PushActiveSnapshot(GetTransactionSnapshot());
 		 pgstat_report_activity(STATE_RUNNING, buf.data);

 		 /* We can now execute queries via SPI */
 		 ret = SPI_execute(buf.data, false, 0);

 		 if (ret != SPI_OK_DELETE_RETURNING)
 		 	elog(FATAL, "cannot select from table %s.%s: error code %d",
 		 		 table->schema, table->name, ret);

 		 if (SPI_processed > 0)
 		 {
 		 	bool 		isnull;
 		 	int32 		val;

 		 	to_launch = SPI_palloc(sizeof(int) * SPI_processed);

 		 	for (i = 0; i < SPI_processed; i++)
 		 	{
			 	val = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
			 									  SPI_tuptable->tupdesc,
			 									  1, &isnull));
			 	/* The attribute is declared as primary key */
			 	Assert(!isnull);
			 	/* The CHECK on the table permits only non-negative values */
			 	Assert(val >= 0);
			 	to_launch[i] = val;
			}
 		 }
 		 /*
 		  * Store the value of SPI variable, since it might go away
 		  * after we call SPI_finish.
 		  */
 		 processed = SPI_processed;

 		/*
 		 * And finish our transaction
 		 */
 		 SPI_finish();
 		 PopActiveSnapshot();
 		 CommitTransactionCommand();

 		 /* Check if we have to start new workers */
 		 if (processed == 0 && !got_sigusr1)
 		 {
 		 	pgstat_report_activity(STATE_IDLE, NULL);
 		  	continue;
 		 }
 		 /* Launch all new worker processes */
 		 if (processed > 0)
 		 {
		 	pgstat_report_activity(STATE_RUNNING, "launching child processes");
		 	if (!launch_workers(processed, to_launch))
		 		elog(WARNING, "unable to launch child processes: no available child slots");
			pfree(to_launch);
		 }
		 if (got_sigusr1)
		 {
		 	got_sigusr1 = false;
		 	/* recheck if we should remove some processes from the list */
		 	pgstat_report_activity(STATE_RUNNING, "removing stopped child processes");
		 	cleanup_on_workers_exit();
		 }
		 pgstat_report_activity(STATE_IDLE, NULL);
 	}
 	terminate_workers();
 	cleanup_on_workers_exit();

 	/*
 	* Don't bother with freeing the memory for the workers list,
 	 * it will be all gone when we terminate the process
 	 */
 	proc_exit(1);
 }

static void
fill_launch_area(slotno, indexno)
{
	volatile WorkerCallHeader *hdr = launcher->hdr;
	SpinLockAcquire(&hdr->mutex);
	hdr->consumed = false;
	hdr->slotno = slotno;
	hdr->index = indexno;
	SpinLockRelease(&hdr->mutex);
}

/* Launch given number of workers */
static bool
launch_workers(int count, int *indexes)
{
	int 	i;
	int  	j = 0;
	int 	t = 0;

	if (launcher->workers_active + count > launcher->workers_total)
		return false;

	for (i = 0; i < count; i++)
	{
		while (j < launcher->workers_total)
		{
			if (launcher->worker_handles[j] == NULL)
			{
				pid_t 	pid;
				bool 	consumed;
				/* Use this slot and launch a new worker */
				elog(LOG, "launching worker with index %d", indexes[i]);
				fill_launch_area(j, indexes[i]);
				BackgroundWorkerHandle   *handle = worker_spi_launch_internal(dsm_segment_handle(launcher->data), indexes[i], NULL);
				volatile WorkerCallHeader *hdr = launcher->hdr;
				/* Wait for the process to attach to shared memory */
				for (t = 0; t < 5; t++)
				{
					SpinLockAcquire(&hdr->mutex);
					consumed = hdr->consumed;
					SpinLockRelease(&hdr->mutex);
					if (consumed)
						break;
					/* XXX: this may loose signals and not-notice if the Postmaster dies */
					pg_usleep(1000L);

				}
				if (!consumed)
				{
					elog(LOG, "timeout out waiting for backend with index %d to attach to shared memory", indexes[i]);
					TerminateBackgroundWorker(launcher->worker_handles[i]);
				}
				else
				{
					launcher->worker_handles[j++] = handle;
					launcher->workers_active++;
					break;
				}
			}
			j++;
		}
		if (j == launcher->workers_total)
			return false;
	}
	return true;
}

static void
cleanup_on_workers_exit()
{
	int 	i;
	pid_t 	pid;

	for (i = 0; i < launcher->workers_total; i++)
	{
		/* Check a given worker, free its slot if it has terminated and get its last will as well */
		if (launcher->worker_handles[i] != NULL && GetBackgroundWorkerPid(launcher->worker_handles[i], &pid) != BGWH_STARTED)
		{
			WorkerState *state = get_worker(i);
			if (!state->consumed)
			{
				elog(LOG, "worker %d has stopped", pid);
				elog(LOG, "exit code: %d", state->exitcode);
				elog(LOG, "SQL state: %s", unpack_sql_state(state->sqlstate));
				elog(LOG, "last will: %s", state->errormessage);
				launcher->worker_handles[i] = NULL;
			}
			launcher->workers_active--;
		}
	}
}

static void
terminate_workers()
{
	int 	i;
	pid_t 	pid;
	for (i = 0; i < launcher->workers_total; i++)
	{
		if (launcher->worker_handles[i] != NULL)
			if (GetBackgroundWorkerPid(launcher->worker_handles[i], &pid) == BGWH_STARTED)
			{
				elog(LOG, "terminating worker %d because of the launcher exit", pid);
				TerminateBackgroundWorker(launcher->worker_handles[i]);
			}
	}
}

/*
 * Initialize workspace for a worker process: create the schema if it doesn't
 * already exist.
 */
static void
initialize_worker_spi(worktable *table)
{
	int			ret;
	int			ntup;
	bool		isnull;
	StringInfoData buf;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "initializing spi_worker schema");

	/* XXX could we use CREATE SCHEMA IF NOT EXISTS? */
	initStringInfo(&buf);
	appendStringInfo(&buf, "select count(*) from pg_namespace where nspname = '%s'",
					 table->schema);

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	if (SPI_processed != 1)
		elog(FATAL, "not a singleton result");

	ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1, &isnull));
	if (isnull)
		elog(FATAL, "null result");

	if (ntup == 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE SCHEMA \"%s\" "
						 "CREATE TABLE \"%s\" ("
			   "		type text CHECK (type IN ('total', 'delta')), "
						 "		value	integer, last_modified timestamptz)"
				  "CREATE UNIQUE INDEX \"%s_unique_total\" ON \"%s\" (type) "
						 "WHERE type = 'total'",
					   table->schema, table->name, table->name, table->name);

		/* set statement start time */
		SetCurrentStatementStartTimestamp();

		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to create my schema");
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
worker_spi_main(Datum main_arg)
{
	int segment = UInt32GetDatum(main_arg);
	worktable  *table;
	StringInfoData buf;
	int 		index;
	char		name[20];

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, worker_spi_sighup);
	pqsignal(SIGTERM, worker_spi_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	if (segment != 0)
	{
		LauncherChild = true;
		worker_attach_to_shared_memory(segment, &index);
	}
	else
	{
		LauncherChild = false;
		index = 0;
	}

	table = palloc(sizeof(worktable));
	sprintf(name, "schema%d", index);
	table->schema = pstrdup(name);
	table->name = pstrdup("counted");

	PG_TRY();
	{
		/* Connect to our database */
		BackgroundWorkerInitializeConnection("postgres", NULL);

		elog(LOG, "%s initialized with %s.%s",
			 MyBgworkerEntry->bgw_name, table->schema, table->name);
		pgstat_report_appname(MyBgworkerEntry->bgw_name);
		initialize_worker_spi(table);

		/*
		 * Quote identifiers passed to us.  Note that this must be done after
		 * initialize_worker_spi, because that routine assumes the names are not
		 * quoted.
		 *
		 * Note some memory might be leaked here.
		 */
		table->schema = quote_identifier(table->schema);
		table->name = quote_identifier(table->name);

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "WITH deleted AS (DELETE "
						 "FROM %s.%s "
						 "WHERE type = 'delta' RETURNING value), "
						 "total AS (SELECT coalesce(sum(value), 0) as sum "
						 "FROM deleted) "
						 "UPDATE %s.%s "
						 "SET value = %s.value + total.sum, "
						 "last_modified = CASE WHEN total.sum != 0 THEN now() ELSE last_modified END "
						 "FROM total WHERE type = 'total' "
						 "RETURNING %s.value, %s.last_modified",
						 table->schema, table->name,
						 table->schema, table->name,
						 table->name,
						 table->name,
						 table->name);

		/*
		 * Main loop: do this until the SIGTERM handler tells us to terminate
		 */
		while (!got_sigterm)
		{
			int			ret;
			int			rc;

			/*
			 * Background workers mustn't call usleep() or any direct equivalent:
			 * instead, they may wait on their process latch, which sleeps as
			 * necessary, but is awakened if postmaster dies.  That way the
			 * background process goes away immediately in an emergency.
			 */
			rc = WaitLatch(&MyProc->procLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   worker_spi_naptime * 1000L);
			ResetLatch(&MyProc->procLatch);

			/* emergency bailout if postmaster has died */
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);

			/*
			 * In case of a SIGHUP, just reload the configuration.
			 */
			if (got_sighup)
			{
				got_sighup = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			/*
			 * Start a transaction on which we can run queries.  Note that each
			 * StartTransactionCommand() call should be preceded by a
			 * SetCurrentStatementStartTimestamp() call, which sets both the time
			 * for the statement we're about the run, and also the transaction
			 * start time.  Also, each other query sent to SPI should probably be
			 * preceded by SetCurrentStatementStartTimestamp(), so that statement
			 * start time is always up to date.
			 *
			 * The SPI_connect() call lets us run queries through the SPI manager,
			 * and the PushActiveSnapshot() call creates an "active" snapshot
			 * which is necessary for queries to have MVCC data to work on.
			 *
			 * The pgstat_report_activity() call makes our activity visible
			 * through the pgstat views.
			 */
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());
			pgstat_report_activity(STATE_RUNNING, buf.data);

			/* We can now execute queries via SPI */
			ret = SPI_execute(buf.data, false, 0);

			if (ret != SPI_OK_UPDATE_RETURNING)
				elog(FATAL, "cannot select from table %s.%s: error code %d",
					 table->schema, table->name, ret);

			if (SPI_processed > 0)
			{
				bool		isnull;
				int32		val;
				char       *modified;

				val = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc,
												  1, &isnull));
				modified = SPI_getvalue(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc,
										2);

				if (!isnull)
					elog(LOG, "%s: count in %s.%s is now %d, last_modified: %s",
						 MyBgworkerEntry->bgw_name,
						 table->schema, table->name,
						 val, modified);
			}

			/*
			 * And finish our transaction.
			 */
			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
			pgstat_report_activity(STATE_IDLE, NULL);
		}
	}
	PG_CATCH();
	{
		if (LauncherChild)
		{
			ErrorData 	*errdata;
			/* Save the error in shared memory */
			errdata = CopyErrorData();
			worker_report_feedback(errdata->saved_errno, errdata->sqlerrcode, errdata->message);
		}
	 	PG_RE_THROW();
	}
	PG_END_TRY();

	proc_exit(1);
}

/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker launcher;

	/* get the configuration */
	DefineCustomIntVariable("worker_spi.naptime",
							"Duration between each check (in seconds).",
							NULL,
							&worker_spi_naptime,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("worker_spi.total_workers",
							"Number of workers.",
							NULL,
							&worker_spi_total_workers,
							2,
							1,
							100,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("launcher_spi.naptime",
						    "Duration between each launcher check (in milliseconds).",
						    NULL,
						    &launcher_spi_naptime,
						    500,
						    10,
						    1000,
						    PGC_SIGHUP,
						    0,
						    NULL,
						    NULL,
						    NULL);

	DefineCustomIntVariable("launcher_spi.max_workers",
							"Maximum number of workers that can be launched dynammically.",
							NULL,
							&launcher_max_workers,
							5,
							1,
							max_worker_processes - 1,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	/* set up common data for all our workers */
	launcher.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	launcher.bgw_start_time = BgWorkerStart_RecoveryFinished;
	launcher.bgw_restart_time = BGW_NEVER_RESTART;
	launcher.bgw_main = launcher_spi_main;
	launcher.bgw_notify_pid = 0;
	snprintf(launcher.bgw_name, BGW_MAXLEN, "background worker launcher");
	launcher.bgw_main_arg = Int32GetDatum(0);

	RegisterBackgroundWorker(&launcher);
}

/*
 * Dynamically launch an SPI worker.
 */
static BackgroundWorkerHandle *
worker_spi_launch_internal(uint32 segment, int index, pid_t *retpid)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t 			pid;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;		/* new worker might not have library loaded */
	sprintf(worker.bgw_library_name, "worker_spi");
	sprintf(worker.bgw_function_name, "worker_spi_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "worker %d", index);
	worker.bgw_main_arg = UInt32GetDatum(segment);
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		return NULL;

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
			   errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			  errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);
	if (retpid)
		*retpid = pid;

	return handle;
}

/* External wrapper for worker_spi_launch_internal */
Datum
worker_spi_launch(PG_FUNCTION_ARGS)
{
	int32		i = PG_GETARG_INT32(0);
	BackgroundWorkerHandle *handle;
	pid_t		pid;

	handle = worker_spi_launch_internal(0, 0, &pid);
	if (handle == NULL)
		PG_RETURN_NULL();
	PG_RETURN_INT32(pid);
}
