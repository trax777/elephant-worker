set search_path=:extschema,public;
DO
$$
DECLARE
    allowed_combi int [][];
    roloids int [];
    datoid oid;
    job_count int := 1000;
    rolcount integer;
    minute int;
    hour int;
    dom int;
    dow int;
    month int;
BEGIN
    SELECT oid
      INTO datoid
      FROM pg_catalog.pg_database
     WHERE datname=current_catalog;
    roloids := array(SELECT oid
                       FROM pg_catalog.pg_roles pr
                      WHERE has_database_privilege(pr.oid, datoid, 'CONNECT')
                );
    rolcount := array_length(roloids,1);

    FOR i in 1..job_count LOOP
        IF i%10000 = 0 THEN
            RAISE NOTICE 'Inserted % jobs', i;
        END IF;
        minute := (random()*59)::int;
        hour   := (random()*23)::int;
        dom    := (random()*30)::int+1;
        dow    := (random()*6)::int;
        month  := (random()*11)::int+1;

        INSERT INTO job(schedule, datoid, roloid, job_command)
        VALUES (minute||' '||hour||' '||dom||' '||month||' '||dow, datoid, roloids[i%rolcount], i);
    END LOOP;
END;
$$;

VACUUM ANALYZE job;
