CREATE FUNCTION @extschema@.parse_truncate_timestamps(schedule text)
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

    utc_times := ('{'||schedule||'}')::timestamptz[];
    --RAISE NOTICE '%', utc_times;

    -- Convert the timestamp to utc, convert to string
    utc_strings := array( SELECT to_char(unnest at time zone 'utc', 'YYYY-MM-DD HH24:MI OF')
                            FROM unnest(utc_times)
                        ORDER BY 1);
    RETURN utc_strings;

EXCEPTION
    WHEN others THEN
        -- This parse failed, this means it is not a valid (array of) timestamps
        -- but it is not an error in itself
        RETURN null;
END;
$BODY$
IMMUTABLE
SECURITY INVOKER
COST 10;

COMMENT ON FUNCTION @extschema@.parse_truncate_timestamps(text) IS
'Parses the provided schedule into a text[] of UTC timestamps.

Truncates given timestamp(s) on the minute.

Useful as a structure for indexing.';
