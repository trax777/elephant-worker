-- Casts
CREATE TYPE @extschema@.schedule_matcher AS (
    minute int [],
    hour   int [],
    dom    int [],
    month  int [],
    dow    int [],
    utc_string text []
);

CREATE FUNCTION @extschema@.schedule_matcher(timestamptz)
RETURNS @extschema@.schedule_matcher
RETURNS NULL ON NULL INPUT
LANGUAGE SQL
AS
$BODY$
    SELECT ARRAY[ extract(minute from $1 ) ]::int[],
           ARRAY[ extract(hour   from $1 ) ]::int[],
           ARRAY[ extract(day    from $1 ) ]::int[],
           ARRAY[ extract(month  from $1 ) ]::int[],
           ARRAY[ extract(dow    from $1 ) ]::int[],
           ARRAY[to_char($1 at time zone 'utc', 'YYYY-MM-DD HH24:MI OF')]::text[]
$BODY$
SECURITY INVOKER
STABLE;

CREATE CAST (timestamptz AS @extschema@.schedule_matcher)
    WITH FUNCTION @extschema@.schedule_matcher(timestamptz)
    AS IMPLICIT;

CREATE FUNCTION @extschema@.timestamptz(@extschema@.schedule_matcher)
RETURNS timestamptz[]
RETURNS NULL ON NULL INPUT
LANGUAGE SQL
AS
$BODY$
    SELECT array(SELECT unnest($1.utc_string)::timestamptz);
$BODY$
SECURITY INVOKER
STABLE;

CREATE CAST (@extschema@.schedule_matcher AS timestamptz[])
    WITH FUNCTION @extschema@.timestamptz(@extschema@.schedule_matcher)
    AS IMPLICIT;

CREATE FUNCTION @extschema@.parse_cronfield (cronfield text, minvalue int, maxvalue int)
RETURNS int []
RETURNS NULL ON NULL INPUT
LANGUAGE plpgsql
AS
$BODY$
DECLARE
    entry text;
    cronvalues int [] := ARRAY[]::int[];
    -- Example entries : 0-4,5-9/3,8   */3    11-12
    entry_regexp text := '^(\*|(\d{1,2})(-(\d{1,2}))?)(\/(\d{1,2}))?$';
    entry_groups text [];
    min int;
    max int;
    step int;
    result int[];
BEGIN
    FOREACH entry IN ARRAY string_to_array(cronfield, ',')
    LOOP
        entry_groups := regexp_matches(entry, entry_regexp);
        min := entry_groups[2];
        step := coalesce(entry_groups[6]::int,1);
        IF entry_groups[1] = '*' THEN
            min := minvalue;
            max := maxvalue;
        ELSE
            max := coalesce(entry_groups[4]::int,min);
        END IF;

        IF max < min OR max > maxvalue OR min < minvalue THEN
            RAISE SQLSTATE '22P02' USING
                MESSAGE = 'Invalid syntax for single crontab parameter.',
                DETAIL  = format('Range start: %s (%s), End range: %s (%s), Step: %s for crontab field: %s', min, minvalue, max, maxvalue, step, cronfield),
                HINT    = 'Ensure range is ascending and that the ranges is within allowed bounds';
        END IF;

        cronvalues := cronvalues || array(SELECT generate_series(min, max, step));
    END LOOP;

    result := array(SELECT DISTINCT * FROM unnest(cronvalues) ORDER BY 1);
    IF result = '{}'::int[] THEN
        RETURN null;
    END IF;

    RETURN result;
END;
$BODY$
SECURITY INVOKER
IMMUTABLE;

COMMENT ON FUNCTION @extschema@.parse_cronfield (text, int, int) IS
'Parses a single crontab field. Raises an error if the field is definitely invalid, or null when it is unknown';

-- Parsing a crontab entry seems tedious, but it is usefull to do
-- it as part of a check constraint. We ensure that there are only
-- valid entries in the job table.
-- Main source for decicions is man 5 crontab
CREATE FUNCTION @extschema@.schedule_matcher(schedule text)
RETURNS @extschema@.schedule_matcher
RETURNS NULL ON NULL INPUT
LANGUAGE plpgsql
AS
$BODY$
DECLARE
    entries text [] := regexp_split_to_array(schedule, '\s+');
    matcher @extschema@.schedule_matcher;
    utc_times timestamptz[];
BEGIN
    -- Allow some named entries, we transform them into the documented equivalent
    IF array_length (entries, 1) = 1 THEN
        IF entries[1] = '@yearly' OR entries[1] = '@annually' THEN
            entries := ARRAY['0','0','1','1','*'];
        ELSIF entries[1] = '@monthly' THEN
            entries := ARRAY['0','0','1','*','*'];
        ELSIF entries[1] = '@weekly' THEN
            entries := ARRAY['0','0','*','*','0'];
        ELSIF entries[1] = '@daily' OR entries[1] = '@midnight' THEN
            entries := ARRAY['0','0','*','*','*'];
        ELSIF entries[1] = '@hourly' THEN
           entries := ARRAY['0','*','*','*','*'];
        END IF;
    END IF;

    IF array_length(entries, 1) = 5 THEN
        matcher.minute := parse_cronfield(entries[1],0,59);
        matcher.hour   := parse_cronfield(entries[2],0,23);
        matcher.dom    := parse_cronfield(entries[3],1,31);
        matcher.month  := parse_cronfield(entries[4],1,12);
        matcher.dow    := parse_cronfield(entries[5],0,7);

        -- if all entries are not null, the crontab is assumed valid
        IF         matcher.minute IS NOT NULL
               AND matcher.hour   IS NOT NULL
               AND matcher.month  IS NOT NULL
               AND matcher.dom    IS NOT NULL
               AND matcher.dow    IS NOT NULL
        THEN
            -- Convert day 7 to day 0 (Sunday)
            matcher.dow :=  array(SELECT DISTINCT unnest(matcher.dow)%7 ORDER BY 1);

            -- To model the logic of cron, we empty on of the dow or dom arrays
            -- Logic (man 5 crontab):
            -- If both fields are restricted (ie, are not *), the command will be run when
            --     either field matches the current time.
            IF entries[5] = '*' AND entries[3] != '*' THEN
                matcher.dow := '{}'::int[];
            END IF;
            IF entries[3] = '*' AND entries[5] != '*' THEN
                matcher.dom := '{}'::int[];
            END IF;

            RETURN matcher;
        END IF;
    END IF;

    -- We couldn't validate this entry as a crontab entry, so we try timestamps now
    -- We do not use to_timestamptz functionality, as this would render our function
    -- STABLE instead of IMMUTABLE and therefore not indexable.
    -- We require the arry to be in the 'YYYY-MM-DD HH24:MI OF' format, OF being +00

    -- Convert the timestamp to utc, convert to string, sort
    matcher.utc_string := array(SELECT unnest FROM unnest(format('{%s}', schedule)::text[]) ORDER BY 1);

    RETURN matcher;
EXCEPTION
    --WHEN data_exception THEN
    WHEN division_by_zero THEN
        RAISE SQLSTATE '22P02' USING
            MESSAGE = 'Invalid syntax for schedule',
            DETAIL  = format('"%s" cannot be converted into a schedule', schedule),
            HINT    = 'Allowed is: a valid crontab-style entry, a(n array of) "YYYY-MM-DD HH24:MI +00" timestamp(s)';
END;
$BODY$
SECURITY INVOKER
IMMUTABLE;

COMMENT ON FUNCTION @extschema@.schedule_matcher (schedule text) IS
'Tries to parse a string into 5 int[] containing the expanded values for this entry.
Most crontab style entries are allowed.

Returns null on non-crontab format, raises exception on invalid crontab format.

Expanding 7-55/9 would for example become: {7,16,25,34,43,52}

This structure is useful for building an index which can be used for quering which job
should be run at a specific time.';

CREATE CAST (text AS @extschema@.schedule_matcher)
    WITH FUNCTION @extschema@.schedule_matcher(text)
    AS IMPLICIT;
