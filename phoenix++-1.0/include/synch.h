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

#ifndef SYNCH_H_
#define SYNCH_H_

#include "stddefines.h"

#ifdef MR_LOCK_PTMUTEX

#include <pthread.h>

class lock
{
    pthread_mutex_t m;
public:
    lock(int threads) {
        pthread_mutex_init(&m, NULL);
    }

    ~lock() {
        pthread_mutex_destroy(&m);
    }

    void acquire(int thread) {
        pthread_mutex_lock(&m);
    }

    void release(int thread) {
        pthread_mutex_unlock(&m);
    }
};

#elif defined(MR_LOCK_MCS)

#include <atomic.h>

class lock
{
    struct mcs_lock;

    struct mcs_lock_priv {
        mcs_lock  *mcs_head;
        mcs_lock_priv    *next;
        uintptr_t        locked;
        char pad[L2_CACHE_LINE_SIZE-3*sizeof(uintptr_t)];
    };

    struct mcs_lock {
        mcs_lock_priv    *head;
    };

    mcs_lock l;
    mcs_lock_priv* privs;

public:
    lock(int threads) {
        l.head = NULL;
        privs = new mcs_lock_priv[threads];
        for(int i = 0; i < threads; i++) {
            privs[i].mcs_head = &l;
            privs[i].next = NULL;
            privs[i].locked = 0;
        }
    }

    ~lock() {
        delete [] privs;
    }

    void acquire(int thread) {
        mcs_lock_priv* priv = &privs[thread];
        mcs_lock* mcs = priv->mcs_head;
        assert(priv->locked == 0);

        set_and_flush(priv->next, NULL);
        mcs_lock_priv* prev = (mcs_lock_priv*)(atomic_xchg((uintptr_t)priv, (uintptr_t*)(&mcs->head)));
        if(prev != NULL) {
            // someone else has lock
            // NOTE: this ordering is important-- if locked after next assignment,
            // we may have a schedule that will spin forever
            set_and_flush(priv->locked, 1);
            set_and_flush(prev->next, priv);

            while (atomic_read(&priv->locked)) { asm("":::"memory"); }
        }
    }

    void release(int thread) {
        mcs_lock_priv* priv = &privs[thread];
        mcs_lock* mcs = priv->mcs_head;

        if(priv->next == NULL) {
            if (cmp_and_swp( (uintptr_t)NULL, (uintptr_t*)(&mcs->head), 
                (uintptr_t)priv)) {
                // we were the only one on the lock, now it's empty
                return;
            }

            // wait for next to get thrown on
            while (((void*)atomic_read(&(priv->next))) == NULL) {
                asm("" ::: "memory");
            }
        }

        set_and_flush(priv->next->locked, 0);
    }
};
#else
#warning "No lock type selected."
#endif

// Semaphores

#ifdef _DARWIN_
#include <mach/mach.h>
#include <mach/semaphore.h>
#else
#include <semaphore.h>
#endif

class semaphore
{
private:
#ifdef _DARWIN_
    semaphore_t sem;
#else
    sem_t sem;
#endif
public:
    semaphore(int initialValue=0)
    {
        #ifdef _DARWIN_
        semaphore_create(mach_task_self(), &sem, SYNC_POLICY_FIFO, initialValue);
        #else
        sem_init(&sem, 0, initialValue);
        #endif
    }

    void post()
    {
        #ifdef _DARWIN_
        semaphore_signal(sem);
        #else
        sem_post(&sem);
        #endif
    }

    void wait()
    {
        #ifdef _DARWIN_
        semaphore_wait(sem);
        #else
        sem_wait(&sem);
        #endif
    }
};

#endif /* SYNCH_H_ */

// vim: ts=8 sw=4 sts=4 smarttab smartindent
