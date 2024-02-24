MODULES = detect_ddl

EXTENSION = detect_ddl
DATA = detect_ddl--1.0.sql
PGFILEDESC = "detect_ddl - DDL detector"

REGRESS = detect_ddl

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/detect_ddl
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
