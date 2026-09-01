# pg_arrow Makefile

MODULE_big = pg_arrow
OBJS = pg_arrow.o

EXTENSION = pg_arrow
DATA = pg_arrow--1.0.sql
REGRESS = pg_arrow pg_arrow_numeric pg_arrow_bit pg_arrow_errors

# libarrow comes from the system package (libarrow-dev, apt.arrow.apache.org)
# rather than being vendored the way pg_zerialize vendors glaze/jsoncons -
# it's already apt-installable here (confirmed: `pkg-config --modversion
# arrow` -> 19.0.1) and is large enough that hand-vendoring/building it
# wouldn't make sense the way it did for the smaller header-only libraries
# pg_zerialize vendors.
ARROW_CFLAGS = $(shell pkg-config --cflags arrow)
ARROW_LIBS = $(shell pkg-config --libs arrow)

PG_CPPFLAGS = -std=c++20 -fPIC $(ARROW_CFLAGS)
SHLIB_LINK = -lstdc++ $(ARROW_LIBS)

# Use C++ compiler
CC = g++
CXX = g++

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# PGXS links MODULE_big with the C driver; avoid passing C-only warning flags
# from PostgreSQL's build into that link. C++ compilation uses CXXFLAGS below.
override CFLAGS :=

# Compile C++ with PostgreSQL's release, warning, and hardening flags.
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
