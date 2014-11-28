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

#ifndef SCHEDULER_H_
#define SCHEDULER_H_

#include "locality.h"
#include "processor.h"


#ifdef _SOLARIS_
#define NUM_CORES_PER_CHIP      8
#define NUM_STRANDS_PER_CORE    8
#endif

class sched_policy
{
protected:
    int     num_cpus;
    int     num_chips_per_sys;
    int        offset;
public:
    sched_policy(int offset = 0) : offset(offset) 
    {
        num_cpus = proc_get_num_cpus();
        num_chips_per_sys = loc_get_num_lgrps ();
    }

    virtual ~sched_policy() {}

    virtual int thr_to_cpu(int thr) const = 0;
};

class sched_policy_strand_fill : public sched_policy
{
public:
    sched_policy_strand_fill(int offset = 0) : sched_policy(offset) {}
    int thr_to_cpu(int thr) const
    {
        return (thr+offset) % num_cpus;
    }
};

class sched_policy_core_fill : public sched_policy
{
public:
    sched_policy_core_fill(int offset = 0) : sched_policy(offset) {}
    int thr_to_cpu(int thr) const
    {
#ifdef NUM_CORES_PER_CHIP
        int core, strand;
        thr += offset;
        thr %= num_cpus;
        core = thr % (NUM_CORES_PER_CHIP * num_chips_per_sys);
        strand = (thr / (NUM_CORES_PER_CHIP * num_chips_per_sys));
        strand %= NUM_STRANDS_PER_CORE;
        return (core * NUM_STRANDS_PER_CORE + strand);
#else
        return (thr+offset) % num_cpus;
#endif
    }
};

class sched_policy_chip_fill : public sched_policy
{
public:
    sched_policy_chip_fill(int offset = 0) : sched_policy(offset) {}
    int thr_to_cpu(int thr) const
    {
#ifdef NUM_CORES_PER_CHIP
        int chip,  core, strand;
        thr += offset;
        thr %= num_cpus;
        chip = thr % num_chips_per_sys;
        core = (thr / num_chips_per_sys) % NUM_CORES_PER_CHIP;
        strand = thr / (NUM_CORES_PER_CHIP * NUM_STRANDS_PER_CORE);
        strand %= NUM_STRANDS_PER_CORE;

        return (chip * (NUM_CORES_PER_CHIP * NUM_STRANDS_PER_CORE) +
                core * (NUM_STRANDS_PER_CORE) +
                strand);
#else
        return (thr+offset) % num_cpus;
#endif
    }
};

#endif /* SCHEDULER_H_ */

// vim: ts=8 sw=4 sts=4 smarttab smartindent
