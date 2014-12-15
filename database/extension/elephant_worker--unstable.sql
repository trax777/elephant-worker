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
    END LOOP;
END;
$$;
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
-- We explicitly name the sequence, as we use it in function calls also
CREATE SEQUENCE @extschema@.run_log_rl_id_seq;
CREATE TABLE @extschema@.run_log (
    rl_id               integer not null default nextval('run_log_rl_id_seq') primary key,
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
ALTER SEQUENCE @extschema@.run_log_rl_id_seq OWNED BY run_log.rl_id;
GRANT USAGE ON @extschema@.run_log_rl_id_seq TO elephant_scheduler;
GRANT INSERT ON @extschema@.run_log TO elephant_scheduler;

-- Make sure the contents of this table is dumped when pg_dump is called
SELECT pg_catalog.pg_extension_config_dump('run_log', '');
CREATE FUNCTION @extschema@.validate_job_definition() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF      TG_OP = 'INSERT'
         OR OLD.function_signature != NEW.function_signature
         OR OLD.function_arguments != NEW.function_arguments
    THEN
        PERFORM @extschema@.validate_job_definition(NEW);
    END IF;
    RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.validate_job_definition(job @extschema@.job) RETURNS VOID AS
$BODY$
DECLARE
    function_nargs smallint;
    provided_nargs smallint;
    function_owner name;
    function_secdef boolean;
BEGIN
    IF NOT pg_catalog.pg_has_role(current_user, job.user_name, 'MEMBER')
    THEN
        RAISE SQLSTATE '42501' USING
        MESSAGE = 'Insufficient privileges',
        DETAIL  = format('You are not a member of role "%s"', user_name);
    END IF;

    BEGIN
        SELECT pronargs,
               rolname,
               prosecdef,
               coalesce( array_length(job.function_arguments, 1), 0)
          INTO function_nargs,
               function_owner,
               function_secdef,
               provided_nargs
          FROM pg_catalog.pg_proc  pp
          JOIN pg_catalog.pg_roles pr ON (proowner=pr.oid)
        WHERE pp.oid = job.function_signature::regprocedure;
    EXCEPTION
        WHEN undefined_function THEN
            RAISE SQLSTATE '42883' USING
            MESSAGE = format('function "%s" does not exist', job.function_signature),
            HINT    = 'Make sure the search_path is correct, or fully qualify your function signature.';
    END;

    IF function_nargs <> provided_nargs
    THEN
        RAISE SQLSTATE '22023' USING
        MESSAGE = 'Number of arguments provided differs from number of arguments of function.',
        DETAIL  = format('Function arguments: %s, arguments provided: %s', function_nargs, provided_nargs);
    END IF;

    IF job.user_name <> function_owner THEN
        RAISE SQLSTATE '42501' USING
        MESSAGE = 'Insufficient privileges',
        DETAIL  = format('Owner of the function does not equal the job user_name. Owner: %s, user_name: %s', function_owner, job.user_name),
        HINT    = 'Schedule the job using the user_name of the function owner.';
    END IF;

    IF NOT function_secdef THEN
        RAISE SQLSTATE '42P13' USING
        MESSAGE = 'Invalid function definition',
        DETAIL  = 'Function is not a security definer function.',
        HINT    = 'Alter the function into a security definer function.';
    END IF;

END;
$BODY$
LANGUAGE plpgsql;

COMMENT ON FUNCTION @extschema@.validate_job_definition(@extschema@.job) IS
'See comments for @extschema@.validate_job_definition()';

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

This trigger does not alter any data, it only validates.$$;

CREATE TRIGGER validate_job_definition BEFORE INSERT OR UPDATE ON @extschema@.job
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.validate_job_definition();
