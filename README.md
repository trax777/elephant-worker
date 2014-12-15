Introduction
============
Among those new features of PostgreSQL 9.4 there is support for dynamic background workers. These autovacuum-like processes can do everything a normal backend can. Those are launched by PostgreSQL itself and can perform any actions programmed by the user, from simple, like killing idle transactions, to as complex as predicting column values or collecting data for DB monitoring . In PostgreSQL 9.4 they can also start additional background processes.

This opens up quite a lot of possibilities. In fact, PostgreSQL Global Development Group is building up parallel query implementation based on background workers.

There is also a patch to run background tasks from SQL and PL/pgSQL functions.

The good part for us is that the background worker code can be isolated as an extension to run on a pristine unpatched PostgreSQL server.

The idea of this project is to get hands on with this feature and build something interesting with it. For instance, we can implement in-database cron-like periodic tasks execution engine.

There are some already existing implementations of a cron job for databases, i.e. pgAgent coming as a a part of pgAdmin3 or pgsidekick. The downside is that they are external to the database system and tend to be not big improvements in terms of convenience comparing to the usual unix cron.

One will be able to configure this job using a customisable database table, which holds the same attributes as a normal crontab, but offering a query instead of a shell command to run. With 9.4 dynamic background workers we can run multiple processes at once to execute jobs when necessary.

Some design ideas can borrowed from the blog post on pg\_cron.
http://michael.otacoo.com/postgresql-2/and-what-about-pg\_cron-with-background-workers-2/

Background workers are written in C and linked against PostgreSQL, so this task would require your knowledge of C and interest in database internals if you want to be a developer. Alternatively, you can wear a PM hat and participate in the feature design. Or, if you are a QA person, we can use your skills to make sure the module works as expected even in corner cases.

Don't let C and low-level stuff frighten you off, to help us, there is a sample implementation of a background worker, a documentation page explaining the basic data structures and functions, and a blog post describing the background worker improvements in 9.4.

https://github.com/postgres/postgres/tree/master/src/test/modules/worker\_spi
http://www.postgresql.org/docs/9.4/static/bgworker.html
http://michael.otacoo.com/postgresql-2/postgres-9-4-feature-highlight-dynamic-background-workers/

Installation
============
To install this extension you need to install the extension files first.

To load the extension in the database do:

	psql=> CREATE EXTENSION elephant_scheduler [WITH SCHEMA scheduler];
	psql=> GRANT job_scheduler TO very_important_application;


Usage
=====
To be able to use the scheduler you should be granted the `job_scheduler` role.

The following views list the jobs you have permissions for:

- `my_job` Will show only jobs which are owned by you
- `member_job` Will show all the jobs owned by roles of which you are a member

For the job logs you can use the views:

- `my_job_log` Will show only job logs which are owned by you
- `member_job_log` Will show all the job logs owned by roles of which you are a member

For your convenenience a simple api is provided which will not require oids to schedule a job.
Using the api is the preferred way of scheduling jobs.

Defining a new job
------------------

	insert_job(job_command, datname, schedule, rolname, job_description, enabled, job_timeout, parallel);
Examples:

	SELECT insert_job('SELECT 1', current_catalog);
	
	SELECT *
	  FROM insert_job(job_command := 'DELETE FROM order_archive WHERE o_closed < now() - interval '2 weeks'),
					  datname 	  := 'weborder',
				      schedule    := '{"@daily"}'
					 );
	
Updating a job definition
-------------------------

	update_job(job_id, job_command, datname, schedule, rolname, job_description, enabled, job_timeout, parallel);
`job_id` is mandatory, all other arguments are optional
Examples:

	SELECT update_job(1, job_description := 'Testing a job scheduler');
	
	SELECT * FROM update_job(10, enabled := false);

Deleting a job definition
-------------------------

	delete_job(job_id);
Examples:

	SELECT * FROM delete_job(1);

	SELECT delete(job_id)
	  FROM my_job
	 WHERE job_description = 'Temporary workaround';
