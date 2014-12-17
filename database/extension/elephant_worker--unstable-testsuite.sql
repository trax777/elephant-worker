\set extname elephant_worker
\set extschema scheduler
\set dummy dummy
drop extension if exists :extname cascade;
create extension :extname with schema :extschema;
SELECT oid AS datoid FROM pg_database WHERE datname=current_catalog;
\gset
INSERT INTO :extschema.my_job (job_command, datoid, schedule) VALUES
('SELECT 1', :datoid, '0 0            1 1 0'),
('SELECT 1', :datoid, '* 0            * 1 0'),
('SELECT 1', :datoid, '*/12 0         * 1 0'),
('SELECT 1', :datoid, '*/12,*/11    0 * 1 0'),
('SELECT 1', :datoid, '*/12,30-40/3 0 * 11 0'),
('SELECT 1', :datoid, '@hourly'),
('SELECT 1', :datoid, '1-59/7 1 * 1 1');
