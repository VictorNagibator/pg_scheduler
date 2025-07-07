# contrib/pg_scheduler/Makefile

MODULE_big = scheduler
OBJS       = scheduler.o

EXTENSION  = scheduler
DATA       = scheduler--1.0.sql
PGFILEDESC = "pg_scheduler - flexible SQL/shell job scheduler"

SHLIB_LINK = -lpq

# Добавляем флаги компиляции по умолчанию
PG_CPPFLAGS = -I$(shell pg_config --includedir)
CFLAGS = -g -O0

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS     := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_scheduler
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif