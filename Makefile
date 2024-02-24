MODULES = ddl_detector

EXTENSION = ddl_detector
DATA = ddl_detector--1.0.sql
PGFILEDESC = " ddl_detector - minimal DDL detector"

REGRESS = ddl_detector

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/ddl_detector
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
