CREATE TABLE @extschema@.job (
    job_id              serial primary key,
    user_name           name not null default current_user,
    function_signature  text not null,
    function_arguments  text [],
    schedule            text,
    enabled             boolean not null default true,
    failure_count       integer not null default 0  check ( failure_count>=0 ),
    last_executed       timestamptz
);
CREATE UNIQUE INDEX job_unique ON @extschema@.job (user_name, function_signature, coalesce(function_arguments, '{}'::text[]), coalesce(schedule,''));
CREATE FUNCTION @extschema@.validate_job_definition() RETURNS TRIGGER AS
$BODY$
DECLARE
    function_nargs smallint;
    provided_nargs smallint;
BEGIN
    IF NOT pg_catalog.pg_has_role(current_user, NEW.user_name, 'MEMBER') THEN
        RAISE SQLSTATE '42501' USING
        MESSAGE = 'Insufficient privileges',
        DETAIL  = format('You are not a member of role "%s"', user_name);
    END IF;

    -- If function signature or arguments change, they should be checked
    IF      TG_OP = 'INSERT'
         OR OLD.function_signature != NEW.function_signature
         OR OLD.function_arguments != NEW.function_arguments
    THEN
       -- The number of provided arguments should match the number of arguments of the function
        SELECT pronargs,
               coalesce( array_length(NEW.function_arguments, 1), 0)
          INTO function_nargs,
               provided_nargs
          FROM pg_catalog.pg_proc
         WHERE oid = NEW.function_signature::regprocedure;

        IF function_nargs <> provided_nargs
        THEN
            RAISE SQLSTATE '22023' USING
            MESSAGE = 'Number of arguments provided differs from number of arguments of function.',
            DETAIL  = format('Function arguments: %s, arguments provided: %s', function_nargs, provided_nargs);
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

This trigger does not alter any data, it only validates.$$;

CREATE TRIGGER validate_job_definition BEFORE INSERT OR UPDATE ON @extschema@.job
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.validate_job_definition();

SELECT pg_catalog.pg_extension_config_dump('job', '');
COMMENT ON TABLE @extschema@.job IS
'This table holds all the job definitions.';

-- Sequence is allowed to be used by all
DO
$BODY$
DECLARE
    seqname text;
BEGIN
    SELECT pg_catalog.pg_get_serial_sequence('@extschema@.job', 'job_id')
      INTO seqname;
    EXECUTE format('GRANT USAGE ON %s TO PUBLIC;', seqname);
END;
$BODY$;



CREATE VIEW @extschema@.my_job WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job
 WHERE user_name=current_user;
COMMENT ON VIEW @extschema@.my_job IS
'This view shows all the job definitions of the current_user.
This view is a filter of @extschema@.job, for more details, look at the comments of that table.';

CREATE VIEW @extschema@.member_job WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job
 WHERE pg_has_role(current_user, (SELECT rolname FROM pg_catalog.pg_roles WHERE rolname=user_name), 'MEMBER');
COMMENT ON VIEW @extschema@.member_job IS
'This view shows all the job definitions of the users of which the current_user is a member.
This view is a filter of @extschema@.job, for more details, look at the comments of that table.';

COMMENT ON COLUMN @extschema@.job.user_name IS
'The user this job should be run as. To schedule a job you must be a member of this role.';

COMMENT ON COLUMN @extschema@.job.function_signature IS
'The function signature, examples:
  abc()
  test.abc()
  test.abc(int, timestamptz)
  test.abc(integer)';

COMMENT ON COLUMN @extschema@.job.function_arguments IS
'The arguments for the function. The amount of arguments should match the number of arguments in the signature.';

REVOKE ALL ON TABLE @extschema@.job FROM PUBLIC;

-- Needs more finegraining
GRANT SELECT, INSERT, UPDATE, DELETE ON @extschema@.my_job TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON @extschema@.member_job TO PUBLIC;
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
