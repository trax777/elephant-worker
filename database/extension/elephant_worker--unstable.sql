DO
$$
DECLARE
    roles text[] := '{"job_scheduler","job_monitor"}';
    role  text;
BEGIN
    FOREACH role IN ARRAY roles LOOP
        PERFORM 1
           FROM pg_catalog.pg_roles
          WHERE rolname=role;
        IF NOT FOUND
        THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', role);
            EXECUTE format('COMMENT ON ROLE %I IS ''This role was created for elephant_worker on %s''', role, clock_timestamp()::timestamp(0));
        END IF;
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', '@extschema@', role);
    END LOOP;
END;
$$;
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
CREATE DOMAIN @extschema@.schedule AS TEXT
CONSTRAINT is_valid_schedule CHECK (
    parse_crontab(VALUE) IS NOT NULL
    OR
    parse_truncate_timestamps(VALUE) IS NOT NULL
);

COMMENT ON DOMAIN @extschema@.schedule IS
'A schedule can contain either:
- a valid crontab schedule, examples: "0 0 1 1 3", "*/3 12-22/5 * * *", "@daily"
- a (n array of) timestamp(s), as a text representation at UTC, examples: 
    ''{"2042-12-05 13:37 +00","2014-01-01 12:31 +00"}''
    ''1982-08-06 09:30 +02''';
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
CREATE TABLE @extschema@.job (
    job_id              serial primary key,
    datoid              oid not null,
    roloid              oid not null,
    schedule            @extschema@.schedule,
    enabled             boolean not null default true,
    failure_count       integer not null default 0 check ( failure_count>=0 ),
    success_count       integer not null default 0 check ( success_count>=0 ),
    parallel            boolean not null default false,
    job_command         text not null,
    job_description     text,
    job_timeout         interval not null default '6 hours'::interval,
    last_executed       timestamptz
);
CREATE UNIQUE INDEX job_unique_definition_and_schedule ON @extschema@.job(datoid, roloid, coalesce(schedule,''::text), job_command);
COMMENT ON TABLE @extschema@.job IS
'This table holds all the job definitions.

The schedule indexes on this table are used to
quickly identify which jobs should be running on a specific moment.';
SELECT pg_catalog.pg_extension_config_dump('job', '');


CREATE VIEW @extschema@.my_job WITH (security_barrier) AS
SELECT *,
       (SELECT datname FROM pg_catalog.pg_database WHERE oid=datoid) AS datname,
       (SELECT rolname FROM pg_catalog.pg_roles    WHERE oid=roloid) AS rolname
  FROM @extschema@.job
 WHERE roloid = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user)
  WITH CASCADED CHECK OPTION;
COMMENT ON VIEW @extschema@.my_job IS
'This view shows all the job definitions of the current_user';

CREATE VIEW @extschema@.member_job WITH (security_barrier) AS
SELECT *,
       (SELECT datname FROM pg_catalog.pg_database WHERE oid=datoid) AS datname,
       (SELECT rolname FROM pg_catalog.pg_roles    WHERE oid=roloid) AS rolname
  FROM @extschema@.job
 WHERE pg_has_role(current_user, (SELECT oid FROM pg_catalog.pg_roles WHERE oid=roloid), 'MEMBER')
  WITH CASCADED CHECK OPTION;
COMMENT ON VIEW @extschema@.member_job IS
'This view shows all the job definitions of the roles of which the current_user is a member.';

DO
$$
DECLARE
    relnames text [] := '{"job","my_job","member_job"}';
    relname  text;
BEGIN
    FOREACH relname IN ARRAY relnames
    LOOP
        EXECUTE format($format$
            COMMENT ON COLUMN %1$I.%2$I.job_id  IS
                    'Surrogate primary key to uniquely identify this job.';
            COMMENT ON COLUMN %1$I.%2$I.datoid  IS
                    'The oid of the database this job should be run at.';
            COMMENT ON COLUMN %1$I.%2$I.roloid IS
                    'The oid of the user who should run this job.';
            COMMENT ON COLUMN %1$I.%2$I.schedule IS
                    E'The schedule for this job, Hint: \\dD+ @extschema@.schedule';
            COMMENT ON COLUMN %1$I.%2$I.enabled IS
                    'Whether or not this job is enabled';
            COMMENT ON COLUMN %1$I.%2$I.failure_count IS
                    'The number of times this job has failed since the last time it ran succesfully.';
            COMMENT ON COLUMN %1$I.%2$I.success_count IS
                    'The number of times this job has run succesfully.';
            COMMENT ON COLUMN %1$I.%2$I.parallel IS
                    'If true, allows multiple job instances to be active at the same time.';
            COMMENT ON COLUMN %1$I.%2$I.job_command IS
                    'The list of commands to execute. This will be a single transaction.';
            COMMENT ON COLUMN %1$I.%2$I.job_description IS
                    'The description of the job for human reading or filtering.';
            COMMENT ON COLUMN %1$I.%2$I.job_timeout IS
                    'The maximum amount of time this job will be allowed to run before it is killed.';
            COMMENT ON COLUMN %1$I.%2$I.last_executed IS
                    'The last time this job was started.';


                   $format$,
                   '@extschema@',
                   relname);
        IF relname <> 'job'
        THEN
            EXECUTE format($format$
            COMMENT ON COLUMN %1$I.%2$I.datname IS
                    'The name of the database this job should be run at.';
            COMMENT ON COLUMN %1$I.%2$I.rolname IS
                    'The name of the user/role who should run this job.';
                       $format$,
                       '@extschema@',
                       relname);
        END IF;
    END LOOP;
END;
$$;

-- Needs more finegraining
GRANT SELECT, DELETE, INSERT, UPDATE ON @extschema@.my_job TO job_scheduler;
GRANT SELECT, DELETE, INSERT, UPDATE ON @extschema@.member_job TO job_scheduler;
GRANT SELECT ON @extschema@.job TO job_monitor;

CREATE INDEX schedule_crontab_minute ON @extschema@.job USING GIN (((parse_crontab(schedule)).minute)) WHERE (parse_crontab(schedule)).minute IS NOT NULL;
CREATE INDEX schedule_crontab_hour ON @extschema@.job USING GIN (((parse_crontab(schedule)).hour)) WHERE (parse_crontab(schedule)).hour IS NOT NULL;
CREATE INDEX schedule_crontab_dom ON @extschema@.job USING GIN (((parse_crontab(schedule)).dom)) WHERE (parse_crontab(schedule)).dom IS NOT NULL;
CREATE INDEX schedule_timestamps ON @extschema@.job USING GIN (parse_truncate_timestamps(schedule)) WHERE parse_truncate_timestamps(schedule) IS NOT NULL;
CREATE TABLE @extschema@.job_log (
    jl_id               serial primary key,
    job_id              integer not null,
    rolname             name not null,
    datname             name not null,
    job_started         timestamptz not null,
    job_finished        timestamptz,
    job_command         text not null,
    job_sqlstate        character varying(5),
    exception_message   text,
    exception_detail    text,
    exception_hint      text,
    exception_context   text
);
CREATE INDEX ON @extschema@.job_log (job_started);
CREATE INDEX ON @extschema@.job_log (job_finished);
CREATE INDEX ON @extschema@.job_log (job_sqlstate);
-- We decide not to add a foreign key referencing the job table, jobs may be deleted (we could use ON DELETE SET NULL)
-- or the job log is imported somewhere else for processing

-- Make sure the contents of this table is dumped when pg_dump is called
SELECT pg_catalog.pg_extension_config_dump('job_log', '');

COMMENT ON TABLE @extschema@.job_log IS
'All the job logs are stored in this table.';

CREATE VIEW @extschema@.my_job_log WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job_log
 WHERE rolname = (SELECT rolname FROM pg_catalog.pg_roles WHERE rolname=current_user)
  WITH CASCADED CHECK OPTION;
COMMENT ON VIEW @extschema@.my_job_log IS
'All the job logs for the current_user';

CREATE VIEW @extschema@.member_job_log WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job_log jl
 WHERE pg_has_role (current_user, (SELECT rolname FROM pg_catalog.pg_roles pr WHERE pr.rolname=jl.rolname), 'MEMBER')
  WITH CASCADED CHECK OPTION;
COMMENT ON VIEW @extschema@.member_job_log IS
'Shows all the job logs for the jobs run by roles of which current_user is a member';



GRANT SELECT, DELETE, INSERT, UPDATE(job_finished,job_sqlstate,exception_context,exception_message,exception_detail,exception_hint) ON @extschema@.my_job_log TO job_scheduler;
GRANT SELECT, DELETE, INSERT, UPDATE(job_finished,job_sqlstate,exception_context,exception_message,exception_detail,exception_hint) ON @extschema@.member_job_log TO job_scheduler;
GRANT SELECT ON @extschema@.job_log TO job_monitor;

DO
$$
DECLARE
    relnames text [] := '{"job_log","member_job_log","my_job_log"}';
    relname  text;
BEGIN
    FOREACH relname IN ARRAY relnames
    LOOP
        EXECUTE format($format$


            COMMENT ON COLUMN %1$I.%2$I.jl_id  IS
                    'Surrogate primary key to uniquely identify this job log entry.';
            COMMENT ON COLUMN %1$I.%2$I.job_id IS
                    'The job_id for this run.';
            COMMENT ON COLUMN %1$I.%2$I.rolname IS
                    'The role who ran this job.';
            COMMENT ON COLUMN %1$I.%2$I.datname IS
                    'The database where this job ran.';
            COMMENT ON COLUMN %1$I.%2$I.job_started IS
                    'When was this job started.';
            COMMENT ON COLUMN %1$I.%2$I.job_finished IS
                    E'When did this job finish.\n   If NULL, the job is still running or failed catastrophically.';
            COMMENT ON COLUMN %1$I.%2$I.job_command IS
                    'The command that was executed';
            COMMENT ON COLUMN %1$I.%2$I.job_sqlstate IS
                    E'The sqlstate at the end of the command.\n   ''00000'' means success\n   If NULL, the job is still running or failed catastrophically.\n   See: http://www.postgresql.org/docs/current/static/errcodes-appendix.html';
            COMMENT ON COLUMN %1$I.%2$I.exception_message IS
                    'The message of the raised exception';
            COMMENT ON COLUMN %1$I.%2$I.exception_detail IS
                    'Details for the raised exception';
            COMMENT ON COLUMN %1$I.%2$I.exception_hint IS
                    'Hint for the raised exception';

                   $format$,
                   '@extschema@',
                   relname);
    END LOOP;
END;
$$;
CREATE FUNCTION @extschema@.schedule_matches(schedule @extschema@.schedule, matcher @extschema@.schedule_matcher)
RETURNS BOOLEAN
RETURNS NULL ON NULL INPUT
LANGUAGE SQL
AS
$BODY$
    
    SELECT true;
$BODY$
SECURITY INVOKER
IMMUTABLE;
CREATE FUNCTION @extschema@.insert_job(
        job_command text,
        datname name,
        schedule @extschema@.schedule  default null,
        rolname name            default current_user,
        job_description text    default null,
        enabled boolean         default true,
        job_timeout interval    default '6 hours',
        parallel boolean        default false)
RETURNS @extschema@.member_job
LANGUAGE SQL
AS
$BODY$
    INSERT INTO @extschema@.member_job (
        job_command,
        schedule,
        job_description,
        enabled,
        job_timeout,
        parallel,
        roloid,
        datoid)
    VALUES (
        insert_job.job_command,
        insert_job.schedule,
        insert_job.job_description,
        insert_job.enabled,
        insert_job.job_timeout,
        insert_job.parallel,
        (SELECT oid FROM pg_catalog.pg_roles    pr WHERE pr.rolname= insert_job.rolname),
        (SELECT oid FROM pg_catalog.pg_database pd WHERE pd.datname = insert_job.datname)
    )
    RETURNING *;
$BODY$;

COMMENT ON FUNCTION @extschema@.insert_job(text, name, @extschema@.schedule, name,text, boolean,interval,boolean) IS
'Creates a job entry. Returns the record containing this new job.';
CREATE FUNCTION @extschema@.update_job(
		job_id integer,
        job_command text default null,
        datname name default null,
        schedule @extschema@.schedule default null,
        rolname name default null,
        job_description text default null,
        enabled boolean default null,
        job_timeout interval default null,
        parallel boolean default null)
RETURNS @extschema@.member_job
LANGUAGE SQL
AS
$BODY$
	UPDATE @extschema@.member_job mj SET
		job_command     = coalesce(update_job.job_command,     job_command),
		schedule        = coalesce(update_job.schedule,        schedule),
		job_description = coalesce(update_job.job_description, job_description),
		enabled         = coalesce(update_job.enabled,         enabled),
		job_timeout     = coalesce(update_job.job_timeout,     job_timeout),
		parallel        = coalesce(update_job.parallel,        parallel),
		roloid          = (SELECT oid FROM pg_catalog.pg_roles    pr WHERE pr.rolname = coalesce(update_job.rolname, mj.rolname)),
		datoid          = (SELECT oid FROM pg_catalog.pg_database pd WHERE pd.datname = coalesce(update_job.datname, mj.datname))
	WHERE job_id     = update_job.job_id
    RETURNING *;
$BODY$;

COMMENT ON FUNCTION @extschema@.update_job(integer, text, name, schedule, name, text, boolean, interval, boolean) IS
'Update a given job_id with the provided values. Returns the new (update) record.';
CREATE FUNCTION @extschema@.delete_job(job_id integer)
RETURNS @extschema@.member_job
RETURNS NULL ON NULL INPUT
LANGUAGE SQL
AS
$BODY$
    DELETE FROM @extschema@.member_job mj
    WHERE mj.job_id=delete_job.job_id
    RETURNING *;
$BODY$;

COMMENT ON FUNCTION @extschema@.delete_job(job_id integer) IS
'Deletes the job with the specified job_id. Returns the deleted record.';
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
       AND (@extschema@.parse_crontab(schedule)).minute @> minute
       AND (@extschema@.parse_crontab(schedule)).hour   @> hour
       AND (@extschema@.parse_crontab(schedule)).month  @> month
       AND (
                (@extschema@.parse_crontab(schedule)).dom @> dom
                OR
                (@extschema@.parse_crontab(schedule)).dow @> dow
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
       AND @extschema@.parse_truncate_timestamps(schedule) @> utc_string
       AND enabled = true;
END;
$BODY$
SECURITY DEFINER
ROWS 3;

COMMENT ON FUNCTION @extschema@.job_scheduled_at(timestamptz) IS
'Returns all the jobs that should be running this minute according to their schedule.
When no value is provided for runtime, the clock_timestamp() will be used.

This is a function accessing the @extschema@.job table directly, and therefore
needs to be defined as a security definer function. The where clauses should however
safely limit the output.';
CREATE FUNCTION @extschema@.create_job_log(job_id integer)
RETURNS @extschema@.member_job_log
RETURNS NULL ON NULL INPUT
LANGUAGE SQL
AS
$BODY$
     INSERT INTO @extschema@.member_job_log (
            job_id,
            rolname,
            datname,
            job_command,
            job_started
     )
     SELECT mj.job_id,
            rolname,
            datname,
            job_command,
            clock_timestamp()
       FROM @extschema@.member_job mj
      WHERE mj.job_id = create_job_log.job_id
      RETURNING *
$BODY$
SECURITY INVOKER;
CREATE OR REPLACE FUNCTION @extschema@.run_job(job_id integer, jl_id integer default null)
RETURNS @extschema@.member_job_log
LANGUAGE plpgsql
AS
$BODY$
DECLARE
    job_log @extschema@.member_job_log;
BEGIN

    -- Generate a log entry if no jl_id provided
    IF jl_id IS NULL THEN
        job_log := @extschema@.create_job_log(job_id);

        IF job_log.jl_id IS NULL THEN
            RAISE SQLSTATE '22023' USING
                MESSAGE = 'Invalid parameter value',
                DETAIL  = format('Cannot find record with job_id=%s in @extschema@.member_job', coalesce(job_id::text,'NULL'));
        END IF;
    ELSE
        SELECT *
          INTO job_log
          FROM @extschema@.member_job_log mjl
         WHERE mjl.jl_id = run_job.jl_id;

        IF NOT FOUND THEN
            RAISE SQLSTATE '22023' USING
                MESSAGE = 'Invalid parameter value',
                DETAIL  = format('Cannot find record with jl_id=%s in @extschema@.member_job_log', coalesce(jl_id::text,'NULL'));
        END IF;

        IF job_log.job_finished IS NOT NULL THEN
            RAISE SQLSTATE '22023' USING
                MESSAGE = 'Invalid parameter value',
                DETAIL  = 'We will not overwrite an already finished job log',
                HINT    = 'Specify NULL for jl_id to generate a new job log entry';
        END IF;
    END IF;

    IF job_id IS NULL THEN
        job_id := job_log.job_id;
    ELSE
        RAISE NOTICE 'pietje, %, %', job_id, job_log.job_id;
        IF job_id <> job_log.job_id THEN
            RAISE SQLSTATE '22023' USING
                MESSAGE = 'Invalid parameter values',
                DETAIL  = format('This job log entry references job_id: %s, you specified job_id %s, they should be equal', job_log.job_id, job_id);
         END IF;
    END IF;

    -- At this stage we are sure that we have a valid job_log, which seems to have a sane job_id as well
    BEGIN
        EXECUTE job_log.job_command;
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS job_log.exception_message := MESSAGE_TEXT,
                                    job_log.exception_hint    := PG_EXCEPTION_HINT,
                                    job_log.exception_detail  := PG_EXCEPTION_DETAIL,
                                    job_log.exception_context := PG_EXCEPTION_CONTEXT,
                                    job_log.job_sqlstate      := RETURNED_SQLSTATE;
    END;

    UPDATE @extschema@.member_job_log mjl
       SET job_finished      = clock_timestamp(),
           job_sqlstate      = coalesce(job_log.job_sqlstate,'00000'),
           exception_message = job_log.exception_message,
           exception_hint    = job_log.exception_hint,
           exception_detail  = job_log.exception_detail,
           exception_context = job_log.exception_context
     WHERE mjl.jl_id = job_log.jl_id
    RETURNING *
    INTO job_log;

    UPDATE @extschema@.member_job mj
       SET failure_count = (case when job_log.job_sqlstate <> '00000' then failure_count+1 else failure_count end),
           success_count = (case when job_log.job_sqlstate =  '00000' then success_count+1 else success_count end),
           last_executed = job_log.job_started;

    RETURN job_log;
END;
$BODY$
SECURITY INVOKER;
DO
$$
DECLARE
    relation_cursor CURSOR FOR
        SELECT relname
          FROM pg_catalog.pg_depend    pd
          JOIN pg_catalog.pg_extension pe ON (pd.refobjid = pe.oid)
          JOIN pg_catalog.pg_class     pc ON (pd.objid    = pc.oid)
         WHERE deptype='e'
           AND extname='elephant_worker'
           AND relkind in ('r', 'v', 'm');

    sequence_cursor CURSOR FOR
        SELECT relname
          FROM pg_catalog.pg_depend    pd
          JOIN pg_catalog.pg_extension pe ON (pd.refobjid = pe.oid)
          JOIN pg_catalog.pg_class     pc ON (pd.objid    = pc.oid)
         WHERE deptype='e'
           AND extname='elephant_worker'
           AND relkind in ('S');

    function_cursor CURSOR FOR
        SELECT proname,
               pg_catalog.pg_get_function_identity_arguments(pp.oid) as identity_arguments
          FROM pg_catalog.pg_depend    pd
          JOIN pg_catalog.pg_extension pe ON (pd.refobjid = pe.oid)
          JOIN pg_catalog.pg_proc      pp ON (pd.objid    = pp.oid)
         WHERE deptype='e'
           AND extname='elephant_worker';

    domain_cursor CURSOR FOR
        SELECT typname
          FROM pg_catalog.pg_depend    pd
          JOIN pg_catalog.pg_extension pe ON (pd.refobjid = pe.oid)
          JOIN pg_catalog.pg_type      pt ON (pd.objid    = pt.oid)
         WHERE typtype = 'd'
           AND extname='elephant_worker';
BEGIN
    FOR object IN relation_cursor
    LOOP
        EXECUTE format('REVOKE ALL ON %I.%I FROM PUBLIC', '@extschema@', object.relname);
    END LOOP;

    FOR object IN sequence_cursor
    LOOP
        EXECUTE format('REVOKE ALL ON SEQUENCE %I.%I FROM PUBLIC', '@extschema@', object.relname);
        EXECUTE format('GRANT USAGE ON SEQUENCE %I.%I TO job_scheduler', '@extschema@', object.relname);
    END LOOP;

    FOR object IN function_cursor
    LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION %I.%I(%s) FROM PUBLIC', '@extschema@', object.proname, object.identity_arguments);
        EXECUTE format('GRANT EXECUTE ON FUNCTION %I.%I(%s) TO job_scheduler', '@extschema@', object.proname, object.identity_arguments);
    END LOOP;

    FOR object IN domain_cursor
    LOOP
        EXECUTE format('REVOKE ALL ON DOMAIN %I.%I FROM PUBLIC', '@extschema@', object.typname);
        EXECUTE format('GRANT USAGE ON DOMAIN %I.%I TO job_scheduler', '@extschema@', object.typname);
    END LOOP;
END;
$$;
