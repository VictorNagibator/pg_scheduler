# PostgreSQL Job Scheduler Extension
EXTENSION = scheduler
DATA = scheduler--1.0.sql
PGFILEDESC = "pg_scheduler - flexible SQL/shell job scheduler"

# needs 'cause of libpq library 
MODULE_big = scheduler
OBJS = scheduler.o

SHLIB_LINK = -lpq

PG_CPPFLAGS = -I$(shell pg_config --includedir) -I.

REGRESS = scheduler-test
REGRESS_OPTS = \
    --inputdir=./ \
    --load-extension=scheduler

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_scheduler
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif