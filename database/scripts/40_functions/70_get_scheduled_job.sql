CREATE FUNCTION @extschema@.job_scheduled_at(scheduled timestamptz default clock_timestamp())
RETURNS SETOF @extschema@.my_job
RETURNS NULL ON NULL INPUT
LANGUAGE plpgsql
AS
$BODY$
DECLARE
    minute      int[] := ARRAY[extract(minute from scheduled)::int];
    hour        int[] := ARRAY[extract(hour   from scheduled)::int];
    month       int[] := ARRAY[extract(month  from scheduled)::int];
    dom         int[] := ARRAY[extract(day    from scheduled)::int];
    dow         int[] := ARRAY[extract(dow    from scheduled)::int];
    utc_string text[] := ARRAY[to_char(scheduled at time zone 'utc', 'YYYY-MM-DD HH24:MI OF')];
BEGIN
    RETURN QUERY
    SELECT job.*,
           datname,
           rolname
      FROM @extschema@.job
      JOIN pg_catalog.pg_roles    pr ON (job.roloid = pr.oid)
      JOIN pg_catalog.pg_database pd ON (job.datoid = pd.oid)
     WHERE pg_has_role(session_user, roloid, 'MEMBER')
       AND (parse_crontab(schedule)).minute @> minute
       AND (parse_crontab(schedule)).hour   @> hour
       AND (parse_crontab(schedule)).month  @> month
       AND (
                (parse_crontab(schedule)).dom @> dom
                OR
                (parse_crontab(schedule)).dow @> dow
           )

    UNION

    SELECT job.*,
           datname,
           rolname
      FROM @extschema@.job
      JOIN pg_catalog.pg_roles    pr ON (job.roloid = pr.oid)
      JOIN pg_catalog.pg_database pd ON (job.datoid = pd.oid)
     WHERE pg_has_role(session_user, roloid, 'MEMBER')
       AND parse_timestamps(schedule) @> utc_string;
END;
$BODY$
SECURITY DEFINER
ROWS 3;
