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
