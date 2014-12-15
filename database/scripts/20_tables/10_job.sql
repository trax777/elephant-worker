CREATE TABLE @extschema@.job (
    job_id              serial primary key,
    datoid              oid not null,
    useroid             oid not null,
    schedule            text [] not null,
    enabled             boolean not null default true,
    failure_count       integer not null default 0 check ( failure_count>=0 ),
    success_count       integer not null default 0 check ( success_count>=0 ),
    parallel            boolean not null default false,
    job_command         text not null,
    job_description     text,
    job_timeout         interval not null default '6 hours'::interval,
    last_executed       timestamptz,
    check ( pg_has_role(current_user, useroid, 'MEMBER') ),
    check ( array_length(schedule, 1) = 5)
);
SELECT pg_catalog.pg_extension_config_dump('job', '');
COMMENT ON TABLE @extschema@.job IS
'This table holds all the job definitions.';

-- Sequence is allowed to be used by all,
-- so get the sequence name and grant access to job_scheduler
DO
$BODY$
DECLARE
    seqname text;
BEGIN
    SELECT pg_catalog.pg_get_serial_sequence('@extschema@.job', 'job_id')
      INTO seqname;
    EXECUTE format('GRANT USAGE ON %s TO job_scheduler;', seqname);
END;
$BODY$;



CREATE VIEW @extschema@.my_job WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job
 WHERE useroid = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user);
COMMENT ON VIEW @extschema@.my_job IS
'This view shows all the job definitions of the current_user.
This view is a filter of @extschema@.job, for more details, look at the comments of that table.';

CREATE VIEW @extschema@.member_job WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job
 WHERE pg_has_role(current_user, (SELECT oid FROM pg_catalog.pg_roles WHERE oid=useroid), 'MEMBER');
COMMENT ON VIEW @extschema@.member_job IS
'This view shows all the job definitions of the users of which the current_user is a member.
This view is a filter of @extschema@.job, for more details, look at the comments of that table.';

COMMENT ON COLUMN @extschema@.job.useroid IS
'The user this job should be run as. To schedule a job you must be a member of this role.';

REVOKE ALL ON TABLE @extschema@.job FROM PUBLIC;

-- Needs more finegraining
GRANT SELECT, DELETE ON @extschema@.my_job TO job_scheduler;
GRANT SELECT, DELETE ON @extschema@.member_job TO job_scheduler;
GRANT SELECT ON @extschema@.job TO job_monitor;



--minute AND hour AND month AND (dom OR dow)
