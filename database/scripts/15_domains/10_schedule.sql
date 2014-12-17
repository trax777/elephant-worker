CREATE DOMAIN @extschema@.schedule AS TEXT
CONSTRAINT is_valid_schedule CHECK (
    parse_crontab(VALUE) IS NOT NULL
    OR
    parse_truncate_timestamps(VALUE) IS NOT NULL
);

COMMENT ON DOMAIN @extschema@.schedule IS
'A schedule can contain either:
- a valid crontab schedule, examples: "0 0 1 1 3", "*/3 12-22/5 * * *", "@daily"
- a (n array of) timestamp(s), as a text representation at UTC, examples: 
    ''{"2042-12-05 13:37 +00","2014-01-01 12:31 +00"}''
    ''1982-08-06 09:30 +02''';
