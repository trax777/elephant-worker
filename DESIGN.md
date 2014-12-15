Introduction
============
We would like to remember why we used a specific design pattern.
This file is there to read design considerations and decisions.

Job planning and execution: Security
====================================
The worker process will attach to a database using a given username.
We therefore will not do any permission checks, as the security context
is the user's.

To be able to schedule a job you must:
- be member of the owner role of the function
- have the job_scheduler role

We must however ensure that users cannot schedule jobs for roles which
they are not a member of.
We can enforce this using a check constraint on the job definition table.

Job logging and reporting
=========================
We want to provide a simple interface for users of the jobs.
We do not want everybody to be able to see everyone's jobs.

To enable this we could implement a very simple row-based-security using the security barrier.
Basically:

- No grants will be issued on the tables
- Views filtering the rows will be created with security_barrier
- Grants will be given on the views

A simple security_barrier view could have the following WHERE clause:
  pg_has_role(current_role, user_name, 'MEMBER')

Capturing the output of the functions is out of scope: We may receive a huge
amount of data which will be stored in scheduler tables/logs. This confuses matters.
We leave it up to the writer of the jobs to store their output in their own tables.

We will try to capture something useful in case of failure.
Logging to the server log can be done by the worker; but the worker may not be
attached to the database containing the scheduler extension.
It is therefore not for the worker to write into a log table but the launcher.

Static resolving or dynamic resolving
=====================================
As schema names, user names, search_paths etc. can change we need to decide
whether we are going to use dynamic resolving of functions vs static.

Basically: Do we store oid's, which will survive renames of users and schema's?
We cannot however create foreign keys to the system tables, so that would be a bit iffy.

Or do we want to use dynamic resolving, which will survive a dump- and restore, which
will honour search_path changes?

We decide to use static resolving for database names and user names.
The job_command will not be parsed or interpreted by the scheduler; it is therefore
dynamically resolvable.

Schema
======
The schema for this extension will exist in a single database, but it may serve all database in the cluster.

A job has all kinds of attributes which must be reflected in the schema:

- database oid to run the job on
- user oid
- a schedule
- enabled yes/no
- number of failures
- number of successes
- allowed to be run in parrallel
- the command to execute
- a comment or description
- a timeout
- when was it executed

For the schedule we decide to implement the cron-style syntax:
- it is understood by many
- it makes transitioning jobs easy

We may decide to add other styles in the future as well, we can think of:
- one of jobs (think of linux atd for example)
- interval schedule
- sleep schedule

Processes
=========

Launcher
--------
The launcher's task is to decide which job to execute at which given time.
It will therefore periodically (every minute) scan the full job table to find jobs
which have to run now. It can then launch workers which will process 1 job each.

- check clock
- for each job: check schedule
- manage workers
-- which are terminated
-- check if scheduled job is still running
-- check if job is not running longer than timeout

Optional:
- Shout on a LISTEN/NOTIFY channel when something happens

If there are more jobs to run than there are worker processes available, we let postgres
handle the problems for now.

Worker
------
The worker will be given a row from the job table and attach to a given database using a given user.
It will execute the provided command(s) and return success or a failure message.

It may return a record containing useful information.


DECISIONS TO MAKE
=================
WONT: We will only accept security definer functions
WONT: We will check whether the job to be executed is deemed safe
WONT: These checks will be made during scheduling
WONT: These checks will be made during executing
WONT: We will capture the full output from the executed functions
- Users can only schedule jobs which are owned by one of their roles
- We will use security barrier views to enable row level security
- We will use "dynamic" resolving of function names and user names
- We will have 1 database in the cluster which manages the jobs
- We will have 1 set of processes in the cluster which will serve all databases
- We will provide an api to schedule, remove or alter jobs
