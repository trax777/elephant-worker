CREATE FUNCTION @extschema@.job_scheduled_at(scheduled timestamptz default clock_timestamp())
RETURNS SETOF @extschema@.my_job
RETURNS NULL ON NULL INPUT
LANGUAGE SQL
AS
$BODY$
WITH time_elements (minute, hour, dom, month, dow) AS (
        SELECT extract(minute from scheduled)::int,
               extract(hour   from scheduled)::int,
               extract(day    from scheduled)::int,
               extract(month  from scheduled)::int,
               extract(dow    from scheduled)::int
    )
SELECT job.*,
       datname,
       rolname
  FROM @extschema@.job
  JOIN pg_catalog.pg_roles pr    ON (job.roloid = pr.oid)
  JOIN pg_catalog.pg_database pd ON (job.datoid = pd.oid)
CROSS JOIN time_elements AS te
 WHERE pg_has_role(session_user, roloid, 'MEMBER')
   AND (parse_crontab(schedule)).minute @> ARRAY[te.minute]
   AND (parse_crontab(schedule)).hour   @> ARRAY[te.hour]
   AND (parse_crontab(schedule)).month  @> ARRAY[te.month]
   AND (
            (parse_crontab(schedule)).dom @> ARRAY[te.dom]
            OR
            (parse_crontab(schedule)).dow @> ARRAY[te.dow]
       );
$BODY$
SECURITY DEFINER;
