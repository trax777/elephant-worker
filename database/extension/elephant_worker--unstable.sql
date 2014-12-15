DO
$$
DECLARE
    roles text[] := '{"job_scheduler","job_monitor"}';
    role  text;
BEGIN
    FOREACH role IN ARRAY roles LOOP
        PERFORM 1
           FROM pg_catalog.pg_roles
          WHERE rolname=role;
        IF NOT FOUND
        THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', role);
            EXECUTE format('COMMENT ON ROLE %I IS ''This role was created for elephant_worker on %s''', role, clock_timestamp()::timestamp(0));
        END IF;
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', '@extschema@', role);
    END LOOP;
END;
$$;
CREATE TABLE @extschema@.job (
    job_id              serial primary key,
    datoid              oid not null,
    roloid             oid not null,
    schedule            text [],
    enabled             boolean not null default true,
    failure_count       integer not null default 0 check ( failure_count>=0 ),
    success_count       integer not null default 0 check ( success_count>=0 ),
    parallel            boolean not null default false,
    job_command         text not null,
    job_description     text,
    job_timeout         interval not null default '6 hours'::interval,
    last_executed       timestamptz,
    check ( schedule IS NULL OR array_length(schedule, 1) = 5)
);
CREATE UNIQUE INDEX job_unique_definition_and_schedule ON @extschema@.job(datoid, roloid, coalesce(schedule,'{}'::text[]), job_command);
COMMENT ON TABLE @extschema@.job IS
'This table holds all the job definitions.';
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
                    'The schedule for this job. For now a subset of crontab like syntax is allowed.';
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
-- We explicitly name the sequence, as we use it in function calls also
CREATE TABLE @extschema@.run_log (
    rl_id               serial primary key,
    rl_job_id           integer references @extschema@.job(job_id) ON DELETE SET NULL,
    user_name           name not null,
    function_signature  text not null,
    function_arguments  text[] not null default '{}'::text[],
    run_started         timestamptz,
    run_finished        timestamptz,
    rows_returned       bigint,
    run_sqlstate        character varying(5),
    exception_message   text,
    exception_detail    text,
    exception_hint      text
);
-- Make sure the contents of this table is dumped when pg_dump is called
SELECT pg_catalog.pg_extension_config_dump('run_log', '');
CREATE FUNCTION @extschema@.insert_job(
        job_command text,
        datname name,
        schedule text[]         default null,
        rolname name            default current_user,
        job_description text    default null,
        enabled boolean         default true,
        job_timeout interval    default '6 hours',
        parallel boolean        default false)
RETURNS @extschema@.my_job
LANGUAGE SQL
AS
$BODY$
    INSERT INTO @extschema@.member_job (
        job_command,
        schedule,
        job_description,
        enabled,
        job_timeout,
        parallel,
        roloid,
        datoid)
    VALUES (
        insert_job.job_command,
        insert_job.schedule,
        insert_job.job_description,
        insert_job.enabled,
        insert_job.job_timeout,
        insert_job.parallel,
        (SELECT oid FROM pg_catalog.pg_roles    pr WHERE pr.rolname= insert_job.rolname),
        (SELECT oid FROM pg_catalog.pg_database pd WHERE pd.datname = insert_job.datname)
    )
    RETURNING *;
$BODY$;
CREATE FUNCTION @extschema@.update_job(
		job_id integer,
        job_command text default null,
        datname name default null,
        schedule text[] default null,
        rolname name default null,
        job_description text default null,
        enabled boolean default null,
        job_timeout interval default null,
        parallel boolean default null)
RETURNS @extschema@.my_job
LANGUAGE SQL
AS
$BODY$
	UPDATE @extschema@.member_job mj SET
		job_command     = coalesce(update_job.job_command,     job_command),
		schedule        = coalesce(update_job.schedule,        schedule),
		job_description = coalesce(update_job.job_description, job_description),
		enabled         = coalesce(update_job.enabled,         enabled),
		job_timeout     = coalesce(update_job.job_timeout,     job_timeout),
		parallel        = coalesce(update_job.parallel,        parallel),
		roloid         = (SELECT oid FROM pg_catalog.pg_roles    pr WHERE pr.rolname = coalesce(update_job.rolname, mj.rolname)),
		datoid          = (SELECT oid FROM pg_catalog.pg_database pd WHERE pd.datname = coalesce(update_job.datname, mj.datname))
	WHERE job_id     = update_job.job_id
    RETURNING *;
$BODY$;
CREATE FUNCTION @extschema@.delete_job(job_id integer)
RETURNS @extschema@.my_job
LANGUAGE SQL
AS
$BODY$
    DELETE FROM @extschema@.member_job mj
    WHERE mj.job_id=delete_job.job_id
    RETURNING *;
$BODY$;
CREATE FUNCTION @extschema@.validate_job_definition() RETURNS TRIGGER AS
$BODY$
DECLARE
    schedule_length integer;
BEGIN
    IF NEW.roloid IS NULL
    THEN
        SELECT oid
          INTO NEW.roloid
          FROM pg_catalog.pg_roles
         WHERE rolname=current_user;
    END IF;

    IF NOT pg_catalog.pg_has_role(current_user, NEW.roloid, 'MEMBER')
    THEN
        RAISE SQLSTATE '42501' USING
        MESSAGE = 'Insufficient privileges',
        DETAIL  = format('You are not a member of role "%s"', user_name);
    END IF;

    schedule_length := array_length( NEW.schedule, 1 );
    IF schedule_length <> 5
    THEN
        IF schedule_length <> 1
        THEN
            RAISE SQLSTATE '22023' USING
            MESSAGE = 'Invalid crontab entry',
            DETAIL  = format('You provided an array with %s elements, we require 1 or 5', schedule_length ),
            HINT    = E'Use valid crontab syntax; for example:\n*/2 1,2,[4-8], * * *\n@monthly';
        END IF;
    END IF;

    RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

COMMENT ON FUNCTION @extschema@.validate_job_definition() IS
$$We want to maintain some sanity on the @extschema@.job table. We could do this with check constraints, for example:

 CONSTRAINT is_current_user_member_of_user_name  check ( pg_catalog.pg_has_role(current_user, user_name, 'MEMBER') ),
 CONSTRAINT does_function_exist                  check ( function_signature::regprocedure::text IS NOT NULL ),
 CONSTRAINT correct_number_of_function_arguments check ( array_length( string_to_array(function_signature::regprocedure::text, ','), 1 )
                                                          =
                                                          array_length( function_arguments, 1) )
However, these check constraints will always be evaluated, even if we want to disable a job for example (because it cannot be resolved during runtime).
We also do not wish to have a foreign key relation with the system catalogs (if it were possible).
But we want to temporary disable a job which is temporarily invalid. Therefore we use a trigger to enforce the sanity of this table.

We also convert scheduler entries if needed.
This trigger does not alter any data, it only validates.$$;

CREATE TRIGGER validate_job_definition BEFORE INSERT OR UPDATE ON @extschema@.job
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.validate_job_definition();
DO
$$
DECLARE
    relation_cursor CURSOR FOR
        SELECT relname
          FROM pg_catalog.pg_depend    pd
          JOIN pg_catalog.pg_extension pe ON (pd.refobjid = pe.oid)
          JOIN pg_catalog.pg_class     pc ON (pd.objid    = pc.oid)
         WHERE deptype='e'
           AND extname='elephant_worker'
           AND relkind in ('r', 'v', 'm');

    sequence_cursor CURSOR FOR
        SELECT relname
          FROM pg_catalog.pg_depend    pd
          JOIN pg_catalog.pg_extension pe ON (pd.refobjid = pe.oid)
          JOIN pg_catalog.pg_class     pc ON (pd.objid    = pc.oid)
         WHERE deptype='e'
           AND extname='elephant_worker'
           AND relkind in ('S');

    function_cursor CURSOR FOR
        SELECT proname,
               pg_catalog.pg_get_function_identity_arguments(pp.oid) as identity_arguments
          FROM pg_catalog.pg_depend    pd
          JOIN pg_catalog.pg_extension pe ON (pd.refobjid = pe.oid)
          JOIN pg_catalog.pg_proc      pp ON (pd.objid    = pp.oid)
         WHERE deptype='e'
           AND extname='elephant_worker';
BEGIN
    FOR object IN relation_cursor
    LOOP
        EXECUTE format('REVOKE ALL ON %I.%I FROM PUBLIC', '@extschema@', object.relname);
    END LOOP;

    FOR object IN sequence_cursor
    LOOP
        EXECUTE format('REVOKE ALL ON SEQUENCE %I.%I FROM PUBLIC', '@extschema@', object.relname);
        EXECUTE format('GRANT USAGE ON SEQUENCE %I.%I TO job_scheduler', '@extschema@', object.relname);
    END LOOP;

    FOR object IN function_cursor
    LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION %I.%I(%s) FROM PUBLIC', '@extschema@', object.proname, object.identity_arguments);
        EXECUTE format('GRANT EXECUTE ON FUNCTION %I.%I(%s) TO job_scheduler', '@extschema@', object.proname, object.identity_arguments);
    END LOOP;
END;
$$;
