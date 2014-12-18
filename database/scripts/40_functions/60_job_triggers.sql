CREATE FUNCTION @extschema@.validate_job_definition() RETURNS TRIGGER AS
$BODY$
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

    IF NEW.schedule IS NOT NULL AND @extschema@.parse_crontab(NEW.schedule) IS NULL THEN
        -- We convert the user provided timestamps into 'YYYY-MM-DD HH24:MI OF'
        NEW.schedule := @extschema@.parse_truncate_timestamps(NEW.schedule);

        -- Special case: provided timestamp matches current moment, we bump it 1 minute, so
        -- it will be executed asap.
        IF NEW.schedule = to_char(clock_timestamp() at time zone 'utc', '{\"YYYY-MM-DD HH24:MI OF\"}') THEN
            NEW.schedule := @extschema@.parse_truncate_timestamps((NEW.schedule::timestamptz + interval '1 minute')::text);
        END IF;
    END IF;

    IF TG_OP = 'UPDATE' AND NEW.job_id <> OLD.job_id THEN
        RAISE SQLSTATE '42501' USING
        MESSAGE = 'Permission denied for relation @extschema@.job',
        DETAIL  = 'Update of primary key is disallowed';
    END IF;

    RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

COMMENT ON FUNCTION @extschema@.validate_job_definition() IS
$$We want to maintain some sanity on the @extschema@.job table.

Many checks are taken care of by check constraints
(on the @extschema@.schedule DOMAIN, and the @extschema@.job TABLE).

We do some extra checks here and if a schedule consisting of timestamps is provided
we convert it into timestaps at utc with a granularity of 1 minute.
$$;

CREATE TRIGGER validate_job_definition BEFORE INSERT OR UPDATE ON @extschema@.job
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.validate_job_definition();
