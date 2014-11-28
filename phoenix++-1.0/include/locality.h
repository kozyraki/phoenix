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

#ifndef LOCALITY_H_
#define LOCALITY_H_

#include <assert.h>
#include <unistd.h>

#if defined(_LINUX_) && defined(NUMA_SUPPORT)
#include <numaif.h>
#include <numa.h>
#elif defined (_SOLARIS_) && defined(NUMA_SUPPORT)
#include <sys/lgrp_user.h>
#include <sys/mman.h>
#else
// No NUMA support by default for now...
//#warning "NUMA support disabled."
#endif

#include "stddefines.h"
#include "processor.h"

/* Retrieve the number of total locality groups on system. */
inline int loc_get_num_lgrps ()
{
#if defined(_LINUX_) && defined(NUMA_SUPPORT)
    static int max = -1;
    if(max < 0)
        max = numa_max_node() + 1;
    return max;
#elif defined (_SOLARIS_) && defined(NUMA_SUPPORT)
    int ret;
    lgrp_cookie_t cookie;
    int nlgrps;

    cookie = lgrp_init (LGRP_VIEW_CALLER);
    nlgrps = lgrp_nlgrps (cookie);
    ret = lgrp_fini (cookie);
    assert (!ret);

    if (nlgrps > 1)
    {
        /* Do not count the locality group that encompasses all the 
           locality groups. */
        nlgrps -= 1;
    }

    return nlgrps;
#else
    return 1;
#endif
}

/* Retrieve the locality group of the calling LWP. */
inline int loc_get_lgrp ()
{
#if defined(_LINUX_) && defined(NUMA_SUPPORT)
    // assumes locality groups are adjacent and the same size. 
    // Need to figure out if there is a better way to get this info.
    return proc_get_cpuid() / (proc_get_num_cpus() / loc_get_num_lgrps());
#elif defined (_SOLARIS_) && defined(NUMA_SUPPORT)
    int lgrp = lgrp_home (P_LWPID, P_MYID);

    if (lgrp > 0) {
        /* On a system with multiple locality groups, there exists a
           mother locality group (lgroup 0) that encompasses all the 
           locality groups. Collapse down the hierarchy. */
        lgrp -= 1;
    }
    
    return lgrp;
#else
    return -1;
#endif
}

/* Retrieve the locality group of the physical memory that backs
   the virtual address ADDR. */
inline int loc_mem_to_lgrp (void const* addr)
{
#if defined(_LINUX_) && defined(NUMA_SUPPORT)
    int mode;
    get_mempolicy(&mode, NULL, 0, (void*)addr, MPOL_F_NODE | MPOL_F_ADDR);
    return mode;
#elif defined(_SOLARIS_) && defined(NUMA_SUPPORT)
    uint_t info = MEMINFO_VLGRP;
    uint64_t inaddr;
    uint64_t lgrp;
    uint_t validity;

    if (sizeof (void *) == 4) {
        /* 32 bit. */
        inaddr = 0xffffffff & (intptr_t)addr;
    } else {
        /* 64 bit. */
        assert (sizeof (void *) == 8);
        inaddr = (uint64_t)addr;
    }

    CHECK_ERROR(meminfo (&inaddr, 1, &info, 1, &lgrp, &validity));
    
    validity = 1;
    if (validity != 3)
    {
        /* VALIDITY better be 3 here. 
           If it is 1, it means the memory has been assigned, but
           not allocated yet. */
        lgrp = 1;
    }

    if (lgrp > 0) {
        /* On a system with multiple locality groups, there exists a
           mother locality group (lgroup 0) that encompasses all the 
           locality groups. Collapse down the hierarchy. */
        lgrp -= 1;
    }
    
    return lgrp;
#else
    return -1;
#endif
}

#endif /* LOCALITY_H_ */

// vim: ts=8 sw=4 sts=4 smarttab smartindent
