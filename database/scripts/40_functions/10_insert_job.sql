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
