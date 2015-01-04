MODULE_big = elephant_worker
OBJS = worker.o launcher.o jobs.o

EXTENSION = elephant_worker
DATA = elephant_worker--1.0.sql
PGFILEDESC = "elephant_worker - execute periodic tasks from the database"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
