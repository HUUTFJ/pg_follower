MODULE_big = ddl_detector

OBJS = \
	$(WIN32RES) \
	ddl_detector.o \
	ddl_detector_output.o

EXTENSION = ddl_detector
DATA = ddl_detector--1.0.sql
PGFILEDESC = " ddl_detector - minimal DDL detector"

# Settings for the regression test

EXTRA_INSTALL=contrib/test_decoding
REGRESS_OPTS = --temp-config ./logical.conf
REGRESS = ddl_detector ddl_detector_output

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
