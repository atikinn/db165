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

server: server.o dbo.o utils.o parse.o symtable.o execute.o vector.o sync.o sindex.o btree.o bset.o ccsort.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS) $(MACROS)

clean:
	rm -f client server *.o *~ *.bak core *.core cs165_unix_socket tags
	rm -rf .deps *.dSYM

dbclean:
	rm -rf ./db

distclean: clean
	rm -rf $(DEPSDIR)

tags:
	  ctags -R .
test0:
	  ./client < ../project_tests/ddl.txt

test1:
	  ./client < ../project_tests/test1.txt

test2:
	  ./client < ../project_tests/test2.txt

test3:
	  ./client < ../project_tests/test3.txt

test4:
	  ./client < ../project_tests/test4.txt

test5:
	  ./client < ../project_tests/test5.txt

test6:
	  ./client < ../project_tests/test6.txt

test7:
	  ./client < ../project_tests/test7.txt

test8:
	  ./client < ../project_tests/test08.dsl

test9:
	  ./client < ../project_tests/test09.dsl

test10:
	  ./client < ../project_tests/test10.dsl

test11:
	  ./client < ../project_tests/test11.dsl

test12:
	  ./client < ../project_tests/test12.dsl

test13:
	  ./client < ../project_tests/test13.dsl

test14:
	  ./client < ../project_tests/test14.dsl

test15:
	  ./client < ../project_tests/test15.dsl

test16:
	  ./client < ../project_tests/test16.dsl

test17:
	  ./client < ../project_tests/test17.dsl

test18:
	  ./client < ../project_tests/test18.dsl

test19:
	  ./client < ../project_tests/test19.dsl

test20:
	  ./client < ../project_tests/test20.dsl

test21:
	  ./client < ../project_tests/test21.dsl

test22:
	  ./client < ../project_tests/test22.dsl

test23:
	  ./client < ../project_tests/test23.dsl

test24:
	  ./client < ../project_tests/test24.dsl

test25:
	  ./client < ../project_tests/test25.dsl

test26:
	  ./client < ../project_tests/test26.dsl

test27:
	  ./client < ../project_tests/test27.dsl

test28:
	  ./client < ../project_tests/test28.dsl

test29:
	echo "already inserted in test08"
	  #./client < ../project_tests/test29.dsl

test30:
	  ./client < ../project_tests/test30.dsl

test31:
	  ./client < ../project_tests/test31.dsl

test32:
	  ./client < ../project_tests/test32.dsl

test33:
	  ./client < ../project_tests/test33.dsl

.PHONY: all clean distclean
