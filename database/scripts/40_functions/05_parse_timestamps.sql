CREATE FUNCTION @extschema@.parse_timestamps(schedule text)
RETURNS text[]
RETURNS null ON null input
LANGUAGE plpgsql
AS
$BODY$
DECLARE
    utc_times timestamptz[];
    utc_strings text[];
BEGIN
    -- If this already is a valid crontab, do nothing
    IF (@extschema@.parse_crontab(schedule)).minute IS NOT NULL THEN
        RETURN null;
    END IF;

    BEGIN
        utc_times := ARRAY[schedule::timestamptz at time zone 'utc'];
        -- If only 1 time was specified which is very close to the current timestamp, we bump it with 1 minute
        -- This will ensure people can schedule tasks using "now()" and clock_timestamp, it will then start
        -- the next minute
        IF extract(epoch FROM clock_timestamp() at time zone 'utc' - utc_times[1]) < 0.3 THEN
            utc_times[1] := utc_times[1] + interval '1 minute';
        END IF;
    EXCEPTION
        WHEN invalid_datetime_format THEN
            BEGIN
                utc_times := schedule::timestamptz[];
            EXCEPTION
                WHEN others THEN
                    -- This parse failed, this means it is not a valid (array of) timestamps
                    -- but it is not an error
                    RETURN null;
            END;
    END;

    -- Convert the timestamp to utc, convert to string
    utc_strings := array( SELECT to_char(unnest at time zone 'utc', 'YYYY-MM-DD HH24:MI OF')
                            FROM unnest(utc_times)
                        ORDER BY 1);
    RETURN utc_strings;
END;
$BODY$
IMMUTABLE
SECURITY INVOKER
COST 10;

ALTER TABLE @extschema@.job ADD CONSTRAINT is_valid_schedule CHECK (
    @extschema@.parse_crontab(schedule) IS NOT NULL
    OR
    @extschema@.parse_timestamps(schedule) IS NOT NULL
);

CREATE INDEX schedule_timestamps ON @extschema@.job USING GIN (parse_timestamps(schedule)) WHERE parse_timestamps(schedule) IS NOT NULL;
