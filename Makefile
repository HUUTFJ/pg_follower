MODULE_big = pg_follower

OBJS = \
	$(WIN32RES) \
	pg_follower.o \
	pg_follower_apply.o \
	pg_follower_output.o
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = pg_follower
DATA = pg_follower--1.0.sql
PGFILEDESC = " pg_follower - Capture changes and follow"

# Settings for the regression test
EXTRA_INSTALL=contrib/test_decoding
REGRESS_OPTS = --temp-config ./logical.conf
REGRESS = pg_follower pg_follower_output pg_follower_apply

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_follower
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
