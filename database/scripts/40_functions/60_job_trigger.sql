CREATE FUNCTION @extschema@.validate_job_definition() RETURNS TRIGGER AS
$BODY$
DECLARE
    schedule_length integer;
BEGIN
    IF NEW.useroid IS NULL
    THEN
        SELECT oid
          INTO NEW.useroid
          FROM pg_catalog.pg_roles
         WHERE rolname=current_user;
    END IF;

    IF NOT pg_catalog.pg_has_role(current_user, NEW.useroid, 'MEMBER')
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