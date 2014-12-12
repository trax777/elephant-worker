Introduction
============
We would like to remember why we used a specific design pattern.
This file is there to read design considerations and decisions.

Job planning and execution: Security
====================================
When we are going to run jobs on a schedule we will need a way to run the job
in a secure way. Basically we want to make sure that someone will not be able
to do something which he shouldn't be able to do.

To safeguard against superuser exploits we decide to only allow security definer
functions for the moment. This is because the job executor will probably have
superuser rights.

To be able to schedule a job you must:
- be member of the owner role of the function
- have the job_scheduler role

As there will be changes (transactions, ddl, dml) between the moment of
defining a job and executing a job we must check all security related conditions during execution.
Some example exploits are listed here:

Example exploit 1
-----------------
Regular user joe:
- creates a function: block_creditcard(customer_id integer), security definer
- creates a scheduled job calling this function, this succeeds:
-- He owns the function
-- It is a security definer function

Now the exploit:
Joe drops his function. Another function with the same signature exists
in the secure_creditcard schema. If this schema is in the search_path during execution,
this function would be executed.

Example exploit 2
-----------------
Regular user joe:
- Creates a security definer function: pg_read_file(text) in schema joe.
- Schedules all kinds of jobs with different file names:
-- pg_read_file('server.key')
-- pg_read_file('server.cert')
-- pg_read_file('certificate.private')
- Drops his pg_read_file function
- Examines the job log to see which one of these files exist

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
We do however return the number of rows affected, and if an error occurs, we will log
as much information about the error as possible.

Static resolving or dynamic resolving
=====================================
As schema names, user names, search_paths etc. can change we need to decide
whether we are going to use dynamic resolving of functions vs static.

Basically: Do we store oid's, which will survive renames of users and schema's?
We cannot however create foreign keys to the system tables, so that would be a bit iffy.

Or do we want to use dynamic resolving, which will survive a dump- and restore, which
will honour search_path changes?

We can also use intelligent dynamic resolving: during insert we check whether the function signature is valid,
for example with a check ( function_signature::regprocedure::text = function_signature ) on the column, which
will check the existence of the function during insert.

DECISIONS TO MAKE
=================
- We will only accept security definer functions
- Users can only schedule jobs which are owned by one of their roles
- We will check whether the job to be executed is deemed safe
- These checks will be made during scheduling
- These checks will be made during executing
- We will not capture the full output from the executed functions
- We will use security barrier views to enable row level security
- We will use "dynamic" resolving of function names and user names
