MODULE_big = elephant_worker
OBJS = worker.o launcher.o

EXTENSION=elephant_worker
DATA=elephant_worker--unstable.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
