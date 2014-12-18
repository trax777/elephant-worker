/* ------------------------------------------------------------------------
 * jobs.c
 *  	Functions to fill and propagate job information.
 *
 * Copyright (c) 2014, Zalando SE.
 * Portions Copyright (C) 2013-2014, PostgreSQL Global Development Group
 * ------------------------------------------------------------------------
 */

#include "postgres.h"

#include "jobs.h"

 /* allocate a new copy Job Description structure and copy the existing one there */
JobDesc *
copy_job_description(JobDesc *source)
{
	Size 	size = offsetof(JobDesc, command) + strlen(source->command) + 1;
	result = palloc(size);
	memcpy(result, source, size);
	return result;
}

void
fill_job_description(JobDesc *desc, uint32 id, char *command, char *datname, char *rolname, bool parallel, uint32 timeout)
{
	desc->job_id = id;
	desc->job_log_id = 0
	desc->job_timeout = timeout;
	desc->parallel = parallel;
	snprintf(desc->datname, MAXNAMELEN, "%s", datname);
	snprintf(desc->rolname, MAXNAMELEN, "%s", rolname);
	strcpy(desc->command, command);
}
