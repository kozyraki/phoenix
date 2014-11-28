/* Copyright (c) 2007-2009, Stanford University
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

#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

#include "map_reduce.h"
#include "stddefines.h"

static inline void * MR_MALLOC(size_t size)
{
    void * temp = malloc(size);
    assert(temp);
    return temp;
}

static inline char * MR_GETENV(char *envstr)
{
    char *env = getenv(envstr);
    if (!env) return "0";
    else return env;
}


static int unit_size;
static int (*compare_g)(const void *, const void *);

/** mr_sort_map()
 *  Sorts based on the val output of wordcount
 */
static void mr_sort_map(map_args_t *args) 
{
    assert(args);
    
    void *data = (void *)args->data;
    int i;

    assert(data);
    
    qsort(data, args->length, unit_size, compare_g);
    for (i = 0; i < args->length; i++)
    {
        emit_intermediate(((char *)data) + (i*unit_size), (void *)0, unit_size); 
    }
}

/** mr_mypartition()
 *  
 */
static int mr_mypartition(int reduce_tasks, void* key, int key_size)
{
    //unsigned int max_int = 0x80000000;// for random()
    unsigned int max_int = 0x40000;// for random()

    //unsigned int max_int = (unsigned int)RAND_MAX + 1;// for rand()
    unsigned int val = *(unsigned int*)key;
    unsigned int part_size = max_int / reduce_tasks;

    return (val / part_size);
    //return default_partition(reduce_tasks, (void*)key, 4);
}

extern struct timeval begin, end;
extern unsigned int library_time;

/** mapreduce_sort()
 *  
 */
void mapreduce_sort(void *base, size_t num_elems, size_t width,
         int (*compar)(const void *, const void *))
{
    final_data_t sort_vals;

    // Global variable
    unit_size = width;
    compare_g = compar;

    // Setup map reduce args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = base; // Array to sort
    map_reduce_args.map = mr_sort_map;
    map_reduce_args.reduce = NULL; // Identity Reduce
    map_reduce_args.splitter = NULL; // Array splitter //mr_sort_splitter;
    map_reduce_args.key_cmp = compar;
    map_reduce_args.unit_size = width;
    map_reduce_args.partition = mr_mypartition; 
    map_reduce_args.result = &sort_vals;
    map_reduce_args.data_size = num_elems * width;
    map_reduce_args.L1_cache_size = atoi(MR_GETENV("MR_L1CACHESIZE"));//1024 * 1024 * 2;
    map_reduce_args.num_map_threads = atoi(MR_GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_reduce_threads = atoi(MR_GETENV("MR_NUMTHREADS"));//16;
    map_reduce_args.num_merge_threads = atoi(MR_GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_procs = atoi(MR_GETENV("MR_NUMPROCS"));//16;
    map_reduce_args.key_match_factor = (float)atof(MR_GETENV("MR_KEYMATCHFACTOR"));//2;
    map_reduce_args.use_one_queue_per_task = atoi(MR_GETENV("MR_1QPERTASK")) ? true : false;

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "inter library: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);
    CHECK_ERROR(map_reduce (&map_reduce_args) < 0);
    get_time (&end);

#ifdef TIMING
    library_time += time_diff (&end, &begin);
    fprintf (stderr, "library: %u\n", library_time);
#endif

    get_time (&begin);
    
    char * tmp = (char *)MR_MALLOC(width * num_elems); 
    int i;
    
    // Need to copy to temp array first since
    // we could be sorting array of pointers
    for(i = 0; i < sort_vals.length; i++)
    {
        memcpy(tmp + (i*width), sort_vals.data[i].key, width);
    }
    
    memcpy(base, tmp, width * num_elems);
    
    free(tmp);
    free(sort_vals.data);
}
