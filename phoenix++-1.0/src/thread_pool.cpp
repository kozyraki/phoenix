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

#include <assert.h>
#include <pthread.h>

#include "../include/thread_pool.h"
#include "../include/atomic.h"
#include "../include/scheduler.h"

thread_pool::thread_pool(int num_threads, sched_policy const* policy)
{
    int             ret;
    pthread_attr_t  attr;

    this->num_threads = num_threads;
    this->num_workers = num_threads;

    this->args = new void*[num_threads];
    this->threads = new pthread_t[num_threads];
    this->thread_args = new thread_arg_t[num_threads];
    
    CHECK_ERROR (pthread_attr_init (&attr));
    CHECK_ERROR (pthread_attr_setscope (&attr, PTHREAD_SCOPE_SYSTEM));
    CHECK_ERROR (pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED));

    this->die = 0;
    for (int i = 0; i < num_threads; ++i) {
        /* Initialize thread argument. */
        this->thread_args[i].pool = this;
        this->thread_args[i].loc.thread = i;
        this->thread_args[i].loc.cpu = policy != NULL ? policy->thr_to_cpu(i) : -1;        
        // we'll get this when the thread runs...
        this->thread_args[i].loc.lgrp = -1;                    
        this->thread_args[i].loc.seed = i;        
        
        ret = pthread_create (
            &this->threads[i], &attr, loop, &this->thread_args[i]);
    }
}

thread_pool::~thread_pool()
{
    assert (this->die == 0);

    this->num_workers = this->num_threads;
    this->num_workers_done = 0;

    this->die = 1;
    for (int i = 0; i < this->num_threads; ++i) {        
        this->thread_args[i].sem_run.post();
    }
    this->sem_all_workers_done.wait();    

    delete [] this->args;
    delete [] this->threads;
    delete [] this->thread_args;
}

int thread_pool::set(thread_func thread_func, void** args, int num_workers)
{
    this->thread_function = thread_func;
    assert (num_workers <= this->num_threads);
    this->num_workers = num_workers;

    for (int i = 0; i < this->num_workers; ++i)
    {
        int j = i * this->num_threads / num_workers;
        this->args[j] = args[i];
    }

    return 0;
}

int thread_pool::begin()
{
    if (this->num_workers == 0)
        return 0;

    this->num_workers_done = 0;

    for (int i = 0; i < this->num_workers; ++i)
    {
        int j = i * this->num_threads / num_workers;
        this->thread_args[j].sem_run.post();
    }

    return 0;
}

int thread_pool::wait()
{
    if (this->num_workers == 0)
        return 0;

    this->sem_all_workers_done.wait();

    return 0;
}

void* thread_pool::loop(void* arg)
{
    thread_arg_t*    thread_arg = (thread_arg_t*)arg;
    
    assert (thread_arg);

    thread_func     thread_func;
    thread_pool*    pool = thread_arg->pool;
    thread_loc&        loc = thread_arg->loc;
    void            *thread_func_arg;
    
    if(loc.cpu >= 0)
        proc_bind_thread (loc.cpu);
        
    loc.lgrp = loc_get_lgrp();

    while (!pool->die)
    {
        thread_arg->sem_run.wait();
        if (pool->die)
            break;
        
        thread_func = *(pool->thread_function);
        thread_func_arg = pool->args[loc.thread];        
        
        // Run thread function.
        (*thread_func)(thread_func_arg, loc);
        
        int num_workers_done = fetch_and_inc(&pool->num_workers_done) + 1;        
        if (num_workers_done == pool->num_workers) {            
            // Everybody's done.
            pool->sem_all_workers_done.post();
        }        
    }

    int num_workers_done = fetch_and_inc(&pool->num_workers_done) + 1;        
    if (num_workers_done == pool->num_workers) {            
        // Everybody's done.
        pool->sem_all_workers_done.post();
    }

    return NULL;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
