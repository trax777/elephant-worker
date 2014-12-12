-- We explicitly name the sequence, as we use it in function calls also
CREATE SEQUENCE @extschema@.run_log_rl_id_seq;
CREATE TABLE @extschema@.run_log (
    rl_id               integer not null default nextval('run_log_rl_id_seq') primary key,
    function_name       name not null,
    user_name           name not null,
    job_id              integer,
    schema_name         name,
    function_arguments  hstore,
    run_started         timestamptz,
    run_finished        timestamptz,
    rows_returned       bigint,
    run_sqlstate        character varying(5),
    exception_message   text,
    exception_detail    text,
    exception_hint      text
);
ALTER SEQUENCE @extschema@.run_log_rl_id_seq OWNED BY run_log.rl_id;
GRANT USAGE ON @extschema@.run_log_rl_id_seq TO elephant_scheduler;
GRANT INSERT ON @extschema@.run_log TO elephant_scheduler;

-- Make sure the contents of this table is dumped when pg_dump is called
SELECT pg_catalog.pg_extension_config_dump('run_log', '');
