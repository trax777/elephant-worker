CREATE TABLE @extschema@.job (
    job_id              serial primary key,
    user_name           name not null,
    function_signature  text not null,
    function_arguments  text [] default '{}'::text[]
);
SELECT pg_catalog.pg_extension_config_dump('job', '');
