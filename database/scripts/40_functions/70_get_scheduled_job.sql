CREATE FUNCTION @extschema@.job_scheduled_at(scheduled timestamptz default clock_timestamp())
RETURNS SETOF @extschema@.my_job
RETURNS NULL ON NULL INPUT
LANGUAGE plpgsql
AS
$BODY$
BEGIN
    RETURN QUERY
    WITH time_elements (minute, hour, dow, month, dom, utc_string) AS (
        SELECT ARRAY[extract(minute  from scheduled)::int],
               ARRAY[extract(hour    from scheduled)::int],
               ARRAY[extract(dow     from scheduled)::int],
               ARRAY[extract(month   from scheduled)::int],
               ARRAY[extract(day     from scheduled)::int],
               ARRAY[to_char(scheduled at time zone 'utc', 'YYYY-MM-DD HH24:MI OF')]
    )
    SELECT job.*,
           datname,
           rolname
      FROM @extschema@.job
      JOIN pg_catalog.pg_roles    pr ON (job.roloid = pr.oid)
      JOIN pg_catalog.pg_database pd ON (job.datoid = pd.oid)
CROSS JOIN time_elements te
     WHERE pg_has_role(session_user, roloid, 'MEMBER')
       AND (parse_crontab(schedule)).minute @> te.minute
       AND (parse_crontab(schedule)).hour   @> te.hour
       AND (parse_crontab(schedule)).month  @> te.month
       AND (
                (parse_crontab(schedule)).dom @> te.dom
                OR
                (parse_crontab(schedule)).dow @> te.dow
           )

    UNION

    SELECT job.*,
           datname,
           rolname
      FROM @extschema@.job
      JOIN pg_catalog.pg_roles    pr ON (job.roloid = pr.oid)
      JOIN pg_catalog.pg_database pd ON (job.datoid = pd.oid)
CROSS JOIN time_elements te
     WHERE pg_has_role(session_user, roloid, 'MEMBER')
       AND parse_timestamps(schedule) @> te.utc_string;
END;
$BODY$
SECURITY DEFINER
ROWS 3;
