# Makefile for Alex's thesis scheduler plugin

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -fexceptions

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = sched_thesis.la

sched_thesis_la_SOURCES = \
			thesis_wrapper.c \
			thesis.c	\
			thesis.h
sched_thesis_la_LDFLAGS = $(PLUGIN_FLAGS)
