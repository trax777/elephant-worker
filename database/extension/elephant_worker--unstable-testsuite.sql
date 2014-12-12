\set extname elephant_worker
\set dummy dummy
drop extension if exists :extname cascade;
create extension :extname with schema scheduler;
