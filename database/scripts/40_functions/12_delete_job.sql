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
