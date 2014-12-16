CREATE TABLE @extschema@.job_log (
    jl_id               serial primary key,
    job_id              integer not null,
    rolname             name not null,
    datname             name not null,
    job_started         timestamptz not null,
    job_finished        timestamptz not null,
    job_command         text not null,
    job_sqlstate        character varying(5),
    exception_message   text,
    exception_detail    text,
    exception_hint      text
);
-- We decide not to add a foreign key referencing the job table, jobs may be deleted (we could use ON DELETE SET NULL)
-- or the job lob is imported somewhere else for processing

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



GRANT SELECT, DELETE, INSERT, UPDATE ON @extschema@.my_job_log TO job_scheduler;
GRANT SELECT, DELETE, INSERT, UPDATE ON @extschema@.member_job_log TO job_scheduler;
GRANT SELECT ON @extschema@.job_log TO job_monitor;
