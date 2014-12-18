CREATE TABLE @extschema@.job (
    job_id              serial primary key,
    datoid              oid not null,
    roloid              oid not null,
    schedule            @extschema@.schedule,
    enabled             boolean not null default true,
    failure_count       integer not null default 0 check ( failure_count>=0 ),
    success_count       integer not null default 0 check ( success_count>=0 ),
    parallel            boolean not null default false,
    job_command         text not null,
    job_description     text,
    job_timeout         interval not null default '6 hours'::interval,
    last_executed       timestamptz
);
CREATE UNIQUE INDEX job_unique_definition_and_schedule ON @extschema@.job(datoid, roloid, coalesce(schedule,''::text), job_command);
COMMENT ON TABLE @extschema@.job IS
'This table holds all the job definitions.

The schedule indexes on this table are used to
quickly identify which jobs should be running on a specific moment.';
SELECT pg_catalog.pg_extension_config_dump('job', '');


CREATE VIEW @extschema@.my_job WITH (security_barrier) AS
SELECT *,
       (SELECT datname FROM pg_catalog.pg_database WHERE oid=datoid) AS datname,
       (SELECT rolname FROM pg_catalog.pg_roles    WHERE oid=roloid) AS rolname
  FROM @extschema@.job
 WHERE roloid = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user)
  WITH CASCADED CHECK OPTION;
COMMENT ON VIEW @extschema@.my_job IS
'This view shows all the job definitions of the current_user';

CREATE VIEW @extschema@.member_job WITH (security_barrier) AS
SELECT *,
       (SELECT datname FROM pg_catalog.pg_database WHERE oid=datoid) AS datname,
       (SELECT rolname FROM pg_catalog.pg_roles    WHERE oid=roloid) AS rolname
  FROM @extschema@.job
 WHERE pg_has_role(current_user, (SELECT oid FROM pg_catalog.pg_roles WHERE oid=roloid), 'MEMBER')
  WITH CASCADED CHECK OPTION;
COMMENT ON VIEW @extschema@.member_job IS
'This view shows all the job definitions of the roles of which the current_user is a member.';

DO
$$
DECLARE
    relnames text [] := '{"job","my_job","member_job"}';
    relname  text;
BEGIN
    FOREACH relname IN ARRAY relnames
    LOOP
        EXECUTE format($format$
            COMMENT ON COLUMN %1$I.%2$I.job_id  IS
                    'Surrogate primary key to uniquely identify this job.';
            COMMENT ON COLUMN %1$I.%2$I.datoid  IS
                    'The oid of the database this job should be run at.';
            COMMENT ON COLUMN %1$I.%2$I.roloid IS
                    'The oid of the user who should run this job.';
            COMMENT ON COLUMN %1$I.%2$I.schedule IS
                    E'The schedule for this job, Hint: \\dD+ @extschema@.schedule';
            COMMENT ON COLUMN %1$I.%2$I.enabled IS
                    'Whether or not this job is enabled';
            COMMENT ON COLUMN %1$I.%2$I.failure_count IS
                    'The number of times this job has failed since the last time it ran succesfully.';
            COMMENT ON COLUMN %1$I.%2$I.success_count IS
                    'The number of times this job has run succesfully.';
            COMMENT ON COLUMN %1$I.%2$I.parallel IS
                    'If true, allows multiple job instances to be active at the same time.';
            COMMENT ON COLUMN %1$I.%2$I.job_command IS
                    'The list of commands to execute. This will be a single transaction.';
            COMMENT ON COLUMN %1$I.%2$I.job_description IS
                    'The description of the job for human reading or filtering.';
            COMMENT ON COLUMN %1$I.%2$I.job_timeout IS
                    'The maximum amount of time this job will be allowed to run before it is killed.';
            COMMENT ON COLUMN %1$I.%2$I.last_executed IS
                    'The last time this job was started.';


                   $format$,
                   '@extschema@',
                   relname);
        IF relname <> 'job'
        THEN
            EXECUTE format($format$
            COMMENT ON COLUMN %1$I.%2$I.datname IS
                    'The name of the database this job should be run at.';
            COMMENT ON COLUMN %1$I.%2$I.rolname IS
                    'The name of the user/role who should run this job.';
                       $format$,
                       '@extschema@',
                       relname);
        END IF;
    END LOOP;
END;
$$;

-- Needs more finegraining
GRANT SELECT, DELETE, INSERT, UPDATE ON @extschema@.my_job TO job_scheduler;
GRANT SELECT, DELETE, INSERT, UPDATE ON @extschema@.member_job TO job_scheduler;
GRANT SELECT ON @extschema@.job TO job_monitor;

CREATE INDEX schedule_crontab_minute ON @extschema@.job USING GIN (((parse_crontab(schedule)).minute)) WHERE (parse_crontab(schedule)).minute IS NOT NULL;
CREATE INDEX schedule_crontab_hour ON @extschema@.job USING GIN (((parse_crontab(schedule)).hour)) WHERE (parse_crontab(schedule)).hour IS NOT NULL;
CREATE INDEX schedule_crontab_dom ON @extschema@.job USING GIN (((parse_crontab(schedule)).dom)) WHERE (parse_crontab(schedule)).dom IS NOT NULL;
CREATE INDEX schedule_timestamps ON @extschema@.job USING GIN (parse_truncate_timestamps(schedule)) WHERE parse_truncate_timestamps(schedule) IS NOT NULL;
