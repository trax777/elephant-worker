CREATE FUNCTION @extschema@.schedule_matches(schedule @extschema@.schedule, matcher @extschema@.schedule_matcher)
RETURNS BOOLEAN
RETURNS NULL ON NULL INPUT
LANGUAGE SQL
AS
$BODY$
    
    SELECT true;
$BODY$
SECURITY INVOKER
IMMUTABLE;
