# CS165 MakeFile
# This file includes automatic dependency tracking
# Please see example of "utils" to see how to add additional file
# to your project

all: client server tags

# C-compiler settings
CC = gcc -std=c11

# Default optimization level
O ?= 0	#3

# Flags and other libraries
override CFLAGS += -Wall -Wextra -pedantic -g -ggdb3 -D_FORTIFY_SOURCE -DLOG -DLOG_ERROR \
		-DLOG_INFO -pthread -O$(O) -I$(INCLUDES) -D _GNU_SOURCE
LDFLAGS =
LIBS =
INCLUDES = include

####### Automatic dependency magic #######
# Set-up dependency directory
DEPSDIR := .deps
BUILDSTAMP := $(DEPSDIR)/rebuildstamp
DEPFILES := $(wildcard $(DEPSDIR)/*.d)
ifneq ($(DEPFILES),)
include $(DEPFILES)
endif
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP

# Dependency compilation
ifneq ($(DEP_CC),$(CC) $(CFLAGS) $(DEPCFLAGS) $(O))
DEP_CC := $(shell mkdir -p $(DEPSDIR); echo >$(BUILDSTAMP); echo "DEP_CC:=$(CC) $(CFLAGS) $(DEPCFLAGS) $(O)" >$(DEPSDIR)/_cc.d)
endif

# Make sure dependency directories are generated
$(DEPSDIR)/stamp $(BUILDSTAMP):
	mkdir -p $(@D)
	touch $@

####### Automatic dependency magic #######

%.o : %.c $(BUILDSTAMP)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -O$(O) -o $@ -c $<

##
# To include additional non-executable files (e.g. selects.c, utils.c, etc),
# you'll need to add an additional build dependency to the file that requires
# the new file.  For example, see that client and server both require utils.o
#
# If you create a new file such as selects.c, then you will need a "selects.o"
# dependency on the right side of whichever one requires the file.
##

client: client.o utils.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

server: server.o dbo.o utils.o parse.o symtable.o execute.o vector.o sync.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS) $(MACROS)

clean:
	rm -f client server *.o *~ *.bak core *.core cs165_unix_socket tags
	rm -rf .deps *.dSYM

distclean: clean
	rm -rf $(DEPSDIR)

tags:
	ctags -R .

.PHONY: all clean distclean
