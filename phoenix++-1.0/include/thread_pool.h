/* Copyright (c) 2007-2011, Stanford University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of Stanford University nor the names of its 
*       contributors may be used to endorse or promote products derived from 
*       this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/ 

#ifndef TPOOL_H_
#define TPOOL_H_

#include "stddefines.h"
#include "synch.h"

typedef void (*thread_func)(void *, thread_loc const& loc);
class sched_policy;

class thread_pool
{
public:
    thread_pool(int num_threads, sched_policy const* policy = NULL);
    ~thread_pool();

    int set(thread_func thread_func, void** args, int num_workers);
    int begin();
    int wait();

private:
    struct thread_arg_t {
        thread_pool*    pool;
        thread_loc      loc;
        semaphore       sem_run;
    };

    int             num_threads;
    int             num_workers;
    int             die;
    thread_func     thread_function;
    semaphore       sem_all_workers_done;
    unsigned int    num_workers_done;
    void            **args;
    pthread_t       *threads;
    thread_arg_t    *thread_args;


    static void* loop (void*);
};

#endif /* TPOOL_H_ */

// vim: ts=8 sw=4 sts=4 smarttab smartindent
