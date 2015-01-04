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


void
fill_job_description(JobDesc *desc,
					 uint32 id, uint32 log_id,
					 char *datname, char *rolname,
					 char *schema, bool parallel,
					 uint32 timeout)
{
	desc->job_id = id;
	desc->job_log_id = log_id;
	desc->job_timeout = timeout;
	desc->parallel = parallel;
	snprintf(desc->datname, NAMEDATALEN, "%s", datname);
	snprintf(desc->rolname, NAMEDATALEN, "%s", rolname);
	snprintf(desc->schemaname, NAMEDATALEN, "%s", schema);
}
