/* ------------------------------------------------------------------------
 * jobs.h
 *  	Common definitions for jobs executed by the worker process.
 *
 * Copyright (c) 2014, Zalando SE.
 * Portions Copyright (C) 2013-2014, PostgreSQL Global Development Group
 * ------------------------------------------------------------------------
 */

#ifndef _JOBS_H
#define _JOBS_H

#include "postgres.h"

typedef struct JobDesc
{
	uint32 	job_id;
	uint32	job_log_id;
	uint32 	job_timeout;
	bool    parallel;
	char 	datname[NAMELEN];
	char 	rolname[NAMELEN];
	char 	schemaname[NAMELEN];
} JobDesc;

void fill_job_description(JobDesc *desc, uint32 id,
						  char *datname, char *rolname,
						  char *schema, bool parallel,
						  uint32 timeout);
JobDesc * copy_job_description(JobDesc *source);

#endif /* _JOBS_H */