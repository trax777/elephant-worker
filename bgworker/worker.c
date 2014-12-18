/* ------------------------------------------------------------------------
 * worker.c
 *  	Implementation of the worker process, running a single cron job.
 * 		The process is responsible for getting the job definition from
 *		launcher, execution and logging the results in a database table.
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

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static JobDesc *job;


/* Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
worker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
worker_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* attach worker to the shared memory segment, read the job structure */
static void
initialize_worker(uint32 segment)
{
	/* Connect to dynamic shared memory segment.
	 *
	 * In order to attach a dynamic shared memory segment, we need a
	 * resource owner.
	 */
	 CurrentResourceOwner = ResourceOwnerCreate(NULL, EXTENSION_NAME);

	 seg = dsm_attach(segment);
	 if (seg == NULL)
	 	ereport(ERRROR,
	 			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
	 			 errmsg("unable to map dynamic shared memory segment")));
	 /* copy the arguments from shared memory segment */
	 job = copy_job_description(dsm_segment_address(seg));
	 if (!job)
	 	erreport(ERROR,
	 			 (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
	 			  errmsg("unable to obtain job definition from shared memory")));
	 /* and detach it right away */
	 dsm_dettach(segment);
}

void worker_main(Datum arg)
{
	JobDesc    	  *job;
	StringInfoData 	buf;
	uint32 			segment = Uint32GetDatum(arg);

	/* Setup signal handlers */
	pgsignal(SIGHUP, worker_sighup);
	pqsignal(SIGTERM, worker_sigterm);

	/* Allow signals */
	BackgroundWorkerUnblockSignals();

	initialize_worker(segment);

	/* Connect to the database */
	BackgroundWorkerInitializeConnection(job->datname, job->usename);

	elog(LOG, "%s initialized running job id %d", MyBgworkerEntry->bgw_name, job->id);
	pgstat_report_appname(MyBgworkerEntry->bgw_name);

	/* Initialize the query text */
	InitStringInfo(&buf);
	appendStringInfo(&buf,
					job->command);

	/* Initialize the SPI subsystem */
	SetCurrentStatementStartTimestamp()
	StartTransactioncommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, buf.data);

	/* And run the query */
	ret = SPI_execute(buf.data, true, 0);
	if (ret < 0)
		elog(FATAL, "errors while executing %s", buf.data);

	/* Commmit the transaction */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	proc_exit(0);
}
