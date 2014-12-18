CREATE TABLE @extschema@.job_log (
    jl_id               serial primary key,
    job_id              integer not null,
    rolname             name not null,
    datname             name not null,
    job_started         timestamptz not null,
    job_finished        timestamptz,
    job_command         text not null,
    job_sqlstate        character varying(5),
    exception_message   text,
    exception_detail    text,
    exception_hint      text,
    exception_context   text
);
CREATE INDEX ON @extschema@.job_log (job_started);
CREATE INDEX ON @extschema@.job_log (job_finished);
CREATE INDEX ON @extschema@.job_log (job_sqlstate);
-- We decide not to add a foreign key referencing the job table, jobs may be deleted (we could use ON DELETE SET NULL)
-- or the job log is imported somewhere else for processing

-- Make sure the contents of this table is dumped when pg_dump is called
SELECT pg_catalog.pg_extension_config_dump('job_log', '');

COMMENT ON TABLE @extschema@.job_log IS
'All the job logs are stored in this table.';

CREATE VIEW @extschema@.my_job_log WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job_log
 WHERE rolname = (SELECT rolname FROM pg_catalog.pg_roles WHERE rolname=current_user)
  WITH CASCADED CHECK OPTION;
COMMENT ON VIEW @extschema@.my_job_log IS
'All the job logs for the current_user';

CREATE VIEW @extschema@.member_job_log WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job_log jl
 WHERE pg_has_role (current_user, (SELECT rolname FROM pg_catalog.pg_roles pr WHERE pr.rolname=jl.rolname), 'MEMBER')
  WITH CASCADED CHECK OPTION;
COMMENT ON VIEW @extschema@.member_job_log IS
'Shows all the job logs for the jobs run by roles of which current_user is a member';



GRANT SELECT, DELETE, INSERT, UPDATE(job_finished,job_sqlstate,exception_context,exception_message,exception_detail,exception_hint) ON @extschema@.my_job_log TO job_scheduler;
GRANT SELECT, DELETE, INSERT, UPDATE(job_finished,job_sqlstate,exception_context,exception_message,exception_detail,exception_hint) ON @extschema@.member_job_log TO job_scheduler;
GRANT SELECT ON @extschema@.job_log TO job_monitor;

DO
$$
DECLARE
    relnames text [] := '{"job_log","member_job_log","my_job_log"}';
    relname  text;
BEGIN
    FOREACH relname IN ARRAY relnames
    LOOP
        EXECUTE format($format$


            COMMENT ON COLUMN %1$I.%2$I.jl_id  IS
                    'Surrogate primary key to uniquely identify this job log entry.';
            COMMENT ON COLUMN %1$I.%2$I.job_id IS
                    'The job_id for this run.';
            COMMENT ON COLUMN %1$I.%2$I.rolname IS
                    'The role who ran this job.';
            COMMENT ON COLUMN %1$I.%2$I.datname IS
                    'The database where this job ran.';
            COMMENT ON COLUMN %1$I.%2$I.job_started IS
                    'When was this job started.';
            COMMENT ON COLUMN %1$I.%2$I.job_finished IS
                    E'When did this job finish.\n   If NULL, the job is still running or failed catastrophically.';
            COMMENT ON COLUMN %1$I.%2$I.job_command IS
                    'The command that was executed';
            COMMENT ON COLUMN %1$I.%2$I.job_sqlstate IS
                    E'The sqlstate at the end of the command.\n   ''00000'' means success\n   If NULL, the job is still running or failed catastrophically.\n   See: http://www.postgresql.org/docs/current/static/errcodes-appendix.html';
            COMMENT ON COLUMN %1$I.%2$I.exception_message IS
                    'The message of the raised exception';
            COMMENT ON COLUMN %1$I.%2$I.exception_detail IS
                    'Details for the raised exception';
            COMMENT ON COLUMN %1$I.%2$I.exception_hint IS
                    'Hint for the raised exception';

                   $format$,
                   '@extschema@',
                   relname);
    END LOOP;
END;
$$;
