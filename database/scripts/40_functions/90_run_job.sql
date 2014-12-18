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
