
CREATE FUNCTION @extschema@.job_scheduled_at(runtime timestamptz default clock_timestamp())
RETURNS SETOF @extschema@.member_job
RETURNS NULL ON NULL INPUT
LANGUAGE plpgsql
AS
$BODY$
DECLARE
    minute      int[] := ARRAY[extract(minute from runtime)::int];
    hour        int[] := ARRAY[extract(hour   from runtime)::int];
    month       int[] := ARRAY[extract(month  from runtime)::int];
    dom         int[] := ARRAY[extract(day    from runtime)::int];
    dow         int[] := ARRAY[extract(dow    from runtime)::int];
    utc_string text[] := ARRAY[to_char(runtime at time zone 'utc', 'YYYY-MM-DD HH24:MI OF')];
BEGIN
    RETURN QUERY
    SELECT job.*,
           datname,
           rolname
      FROM @extschema@.job
      JOIN pg_catalog.pg_roles    pr ON (job.roloid = pr.oid)
      JOIN pg_catalog.pg_database pd ON (job.datoid = pd.oid)
     WHERE pg_has_role(session_user, roloid, 'MEMBER')
       AND (@extschema@.schedule_matcher(@extschema@.schedule)).minute @> minute
       AND (@extschema@.schedule_matcher(@extschema@.schedule)).hour   @> hour
       AND (@extschema@.schedule_matcher(@extschema@.schedule)).month  @> month
       AND (
                (@extschema@.schedule_matcher(@extschema@.schedule)).dom @> dom
                OR
                (@extschema@.schedule_matcher(@extschema@.schedule)).dow @> dow
           )
       AND enabled = true

    UNION

    SELECT job.*,
           datname,
           rolname
      FROM @extschema@.job
      JOIN pg_catalog.pg_roles    pr ON (job.roloid = pr.oid)
      JOIN pg_catalog.pg_database pd ON (job.datoid = pd.oid)
     WHERE pg_has_role(session_user, roloid, 'MEMBER')
       AND parse_truncate_timestamps(@extschema@.schedule) @> utc_string
       AND enabled = true;
END;
$BODY$
SECURITY DEFINER
ROWS 3;

COMMENT ON FUNCTION @extschema@.job_scheduled_at(timestamptz) IS
'Returns all the jobs that should be running this minute according to their @extschema@.schedule.
When no value is provided for runtime, the clock_timestamp() will be used.

This is a function accessing the @extschema@.job table directly, and therefore
needs to be defined as a security definer function. The where clauses should however
safely limit the output.';
