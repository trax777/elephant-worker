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
SECURITY INVOKER;

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
SECURITY INVOKER;

CREATE CAST (@extschema@.schedule_matcher AS timestamptz[])
    WITH FUNCTION @extschema@.timestamptz(@extschema@.schedule_matcher)
    AS IMPLICIT;
