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
#include "../include/task_queue.h"
#include "../include/synch.h"

using namespace std;

task_queue::task_queue(int sub_queues, int num_threads)
{
    this->num_queues = sub_queues;
    this->num_threads = num_threads;
    
    this->queues = new std::deque<task_t>[this->num_queues];
    this->locks = new lock*[this->num_queues];
    for (int i = 0; i < this->num_queues; ++i)
        this->locks[i] = new lock(this->num_threads);
}

task_queue::~task_queue()
{
    for (int i = 0; i < this->num_queues; ++i) {
        delete this->locks[i];
    }

    delete [] this->locks;
    delete [] this->queues;
}

/* Queue TASK at LGRP task queue with locking.
   LGRP is a locality hint denoting to which locality group this task 
   should be queued at. If LGRP is less than 0, the locality group is 
   randomly selected. TID is required for MCS locking. */
void task_queue::enqueue (const task_t& task, thread_loc const& loc, int total_tasks, int lgrp)
{
    int index = (lgrp < 0) ? 
        (total_tasks > 0 ? task.id * this->num_queues / total_tasks : rand_r(&loc.seed)) : 
        lgrp;
    index %= this->num_queues;

    locks[index]->acquire(loc.thread);
    queues[index].push_back(task);
    locks[index]->release(loc.thread);
}

/* Queue TASK at LGRP task queue without locking.
   LGRP is a locality hint denoting to which locality group this task
   should be queued at. If LGRP is less than 0, the locality group is
   randomly selected. */
void task_queue::enqueue_seq (const task_t& task, int total_tasks, int lgrp)
{
    int index = (lgrp < 0) ? 
        (total_tasks > 0 ? task.id * this->num_queues / total_tasks : rand()) : 
        lgrp;
    index %= this->num_queues;
    queues[index].push_back(task);    
}

int task_queue::dequeue (task_t& task, thread_loc const& loc)
{
    int index = (loc.lgrp < 0) ? loc.cpu : loc.lgrp;
    
   /* Do task stealing if nothing on our queue.
      Cycle through all indexes until success or exhaustion */
    int ret = 0;
    for (int i = index; i < index + this->num_queues && ret == 0; i++)
    {
        int idx = i % this->num_queues;
        locks[idx]->acquire(loc.thread);
        if(this->queues[idx].size() > 0)
        {
            if(idx == index)
            {
                task = this->queues[idx].front();
                this->queues[idx].pop_front();
            }
            else
            {
                task = this->queues[idx].back();
                this->queues[idx].pop_back();
                dprintf("Stole task from %d to %d\n", idx, index);
            }
            ret = 1;
        }
        locks[idx]->release(loc.thread);
    }

    if(ret) {
        __builtin_prefetch ((void*)task.data, 0, 3);
        dprintf("Task %llu: started on cpu %d\n", task.id, loc.cpu);        
    }
        
    return ret;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
