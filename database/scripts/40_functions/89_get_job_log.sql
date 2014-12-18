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
