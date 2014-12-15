CREATE FUNCTION @extschema@.delete_job(job_id integer)
RETURNS @extschema@.my_job
LANGUAGE SQL
AS
$BODY$
    DELETE FROM @extschema@.member_job mj
    WHERE mj.job_id=delete_job.job_id
    RETURNING *;
$BODY$;
