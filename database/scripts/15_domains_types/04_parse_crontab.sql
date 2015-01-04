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
            RAISE SQLSTATE '22023' USING
                MESSAGE = 'Invalid crontab parameter.',
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
CREATE FUNCTION @extschema@.parse_crontab (schedule text, OUT minute int [], OUT hour int [], OUT dom int[], OUT month int[], OUT dow int[])
LANGUAGE plpgsql
AS
$BODY$
DECLARE
    entries text [] := regexp_split_to_array(schedule, '\s+');
    entry   text;
    empty int[] := '{}'::int[];
BEGIN
    -- Allow some named entries, we transform them into the documented equivalent
    IF array_length (entries, 1) <> 5 THEN
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
            ELSE
                RETURN ;
            END IF;
        ELSE
            RETURN;
        END IF;
    END IF;

    minute := @extschema@.parse_cronfield(entries[1],0,59);
    hour   := @extschema@.parse_cronfield(entries[2],0,23);
    dom    := @extschema@.parse_cronfield(entries[3],1,31);
    month  := @extschema@.parse_cronfield(entries[4],1,12);
    dow    := @extschema@.parse_cronfield(entries[5],0,7);

    -- if any entry is null, the crontab is invalid
    IF minute IS NULL OR hour IS NULL OR month IS NULL OR dom IS NULL or dow IS NULL THEN
        minute := null;
        hour   := null;
        month  := null;
        dom    := null;
        dow    := null;
        RETURN;
    END IF;

    -- Convert day 7 to day 0 (Sunday)
    dow :=  array(SELECT DISTINCT unnest(dow)%7 ORDER BY 1);

    -- To model the logic of cron, we empty on of the dow or dom arrays
    -- Logic (man 5 crontab):
    -- If both fields are restricted (ie, are not *), the command will be run when
    --     either field matches the current time.
    IF entries[5] = '*' AND entries[3] != '*' THEN
        dow := '{}'::int[];
    END IF;
    IF entries[3] = '*' AND entries[5] != '*' THEN
        dom := '{}'::int[];
    END IF;

    RETURN;
END;
$BODY$
SECURITY INVOKER
IMMUTABLE;

COMMENT ON FUNCTION @extschema@.parse_crontab (schedule text) IS
'Tries to parse a string into 5 int[] containing the expanded values for this entry.
Most crontab style entries are allowed.

Returns null on non-crontab format, raises exception on invalid crontab format.

Expanding 7-55/9 would for example become: {7,16,25,34,43,52}

This structure is useful for building an index which can be used for quering which job
should be run at a specific time.';
