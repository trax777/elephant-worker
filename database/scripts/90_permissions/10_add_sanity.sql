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
