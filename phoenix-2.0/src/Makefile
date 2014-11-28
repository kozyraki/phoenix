#------------------------------------------------------------------------------
# Copyright (c) 2007-2009, Stanford University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of Stanford University nor the names of its 
#       contributors may be used to endorse or promote products derived from 
#       this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#------------------------------------------------------------------------------ 

# This Makefile requires GNU make.

HOME = ..

include $(HOME)/Defines.mk

ifeq ($(LINKAGE),dynamic)
CFLAGS += -fPIC
endif

.PHONY: default all clean

SRCS := \
        map_reduce.c \
        memory.c \
        processor.c \
        scheduler.c \
        synch.c \
        taskQ.c \
        mcs.c \
        pt_mutex.c \
        locality.c \
        iterator.c \
        tpool.c \
#
OBJS := ${SRCS:.c=.o}

default: all

all: $(HOME)/$(LIB_DIR)/$(TARGET)

$(HOME)/$(LIB_DIR)/$(TARGET): $(TARGET)
	cp $< $(HOME)/$(LIB_DIR)

$(LIB_PHOENIX).a: $(OBJS)
	$(AR) cr $@ $(OBJS)
	$(RANLIB) $@

$(LIB_PHOENIX).so: $(OBJS)
	$(CC) --shared -o $@ $(OBJS)

%.o: %.c *.h
	$(CC) $(CFLAGS) -c $< -o $@ -I$(HOME)/$(INC_DIR)
 
clean:
	rm -f $(HOME)/$(LIB_DIR)/$(TARGET) $(TARGET) $(OBJS)
