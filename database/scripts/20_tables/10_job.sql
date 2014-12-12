CREATE TABLE @extschema@.job (
    job_id              serial primary key,
    user_name           name not null default current_user,
    function_signature  text not null,
    function_arguments  text [] default '{}'::text[],
    schedule            text,
    enabled             boolean not null default true,
    failure_count       integer not null default 0 check ( failure_count>=0 ),
    CONSTRAINT is_current_user_member_of_user_name check ( pg_catalog.pg_has_role(current_user, user_name, 'MEMBER') ),
    CONSTRAINT does_function_exist                 check ( function_signature::regprocedure::text IS NOT NULL ),
    CONSTRAINT correct_number_of_arguments         check ( array_length( string_to_array(function_signature::regprocedure::text, ','), 1 )
                                                           =
                                                           array_length( function_arguments, 1) )
);
-- If pg_dump is called, we *do* want the jobs to be dumped as well
SELECT pg_catalog.pg_extension_config_dump('job', '');
COMMENT ON TABLE @extschema@.job IS
'This table holds all the job definitions.';

-- Sequence is allowed to be used by all
DO
$BODY$
DECLARE
    seqname text;
BEGIN
    SELECT pg_catalog.pg_get_serial_sequence('@extschema@.job', 'job_id')
      INTO seqname;
    EXECUTE format('GRANT USAGE ON %s TO PUBLIC;', seqname);
END;
$BODY$;



CREATE VIEW @extschema@.my_job WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job
 WHERE user_name=current_user;
COMMENT ON VIEW @extschema@.my_job IS
'This view shows all the job definitions of the current_user.
This view is a filter of @extschema@.job, for more details, look at the comments of that table.';

CREATE VIEW @extschema@.member_job WITH (security_barrier) AS
SELECT *
  FROM @extschema@.job
 WHERE pg_has_role(current_user, (SELECT rolname FROM pg_catalog.pg_roles WHERE rolname=user_name), 'MEMBER');
COMMENT ON VIEW @extschema@.member_job IS
'This view shows all the job definitions of the users of which the current_user is a member.
This view is a filter of @extschema@.job, for more details, look at the comments of that table.';

COMMENT ON COLUMN @extschema@.job.user_name IS
'The user this job should be run as. To schedule a job you must be a member of this role.';

COMMENT ON COLUMN @extschema@.job.function_signature IS
'The function signature, examples:
  abc()
  test.abc()
  test.abc(int, timestamptz)
  test.abc(integer)';

COMMENT ON COLUMN @extschema@.job.function_arguments IS
'The arguments for the function. The amount of arguments should match the number of arguments in the signature.';

REVOKE ALL ON TABLE @extschema@.job FROM PUBLIC;

-- Needs more finegraining
GRANT SELECT, INSERT, UPDATE, DELETE ON @extschema@.my_job TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON @extschema@.member_job TO PUBLIC;
