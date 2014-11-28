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

#ifndef TASK_Q_
#define TASK_Q_

#include <deque>

#include "stddefines.h"

class lock;

class task_queue
{
public:

    struct task_t {
        uint64_t        id;
        uint64_t        len;
        uint64_t        data;
        uint64_t        pad;
    };

    task_queue(int sub_queues, int num_threads);
    ~task_queue();

    void enqueue(task_t const& task, thread_loc const& loc, 
        int total_tasks=0, int lgrp=-1);
    void enqueue_seq(task_t const& task, int total_tasks=0, int lgrp=-1);
    int dequeue(task_t& task, thread_loc const& loc);

private:

    int             num_queues;
    int             num_threads;
    std::deque<task_t>* queues;
    lock**          locks;
};

#endif /* TASK_Q_ */

// vim: ts=8 sw=4 sts=4 smarttab smartindent
