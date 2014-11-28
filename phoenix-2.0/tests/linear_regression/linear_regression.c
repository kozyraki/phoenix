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
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <sys/time.h>
#include <inttypes.h>

#include "map_reduce.h"
#include "stddefines.h"


typedef struct {
    char x;
    char y;
} POINT_T;

enum {
    KEY_SX = 0,
    KEY_SY,
    KEY_SXX,
    KEY_SYY,
    KEY_SXY,
};

static int intkeycmp(const void *v1, const void *v2)
{
    intptr_t i1 = (intptr_t)v1;
    intptr_t i2 = (intptr_t)v2;

    if (i1 < i2) 
         return 1;
    else if (i1 > i2) 
         return -1;
    else 
         return 0;
}

/** sort_map()
 *  Sorts based on the val output of wordcount
 */
static void linear_regression_map(map_args_t *args) 
{
    assert(args);
    
    POINT_T *data = (POINT_T *)args->data;
    int i;

    assert(data);

    long long * SX  = CALLOC(sizeof(long long), 1);
    long long * SXX = CALLOC(sizeof(long long), 1);
    long long * SY  = CALLOC(sizeof(long long), 1);
    long long * SYY = CALLOC(sizeof(long long), 1);
    long long * SXY = CALLOC(sizeof(long long), 1);

    register long long x, y;
    register long long sx = 0, sxx = 0, sy = 0, syy = 0, sxy = 0;

    for (i = 0; i < args->length; i++)
    {
        //Compute SX, SY, SYY, SXX, SXY
        x = data[i].x;
        y = data[i].y;

        sx  += x;
        sxx += x * x;
        sy  += y;
        syy += y * y;
        sxy += x * y;
    }

    *SX = sx;
    *SXX = sxx;
    *SY = sy;
    *SYY = syy;
    *SXY = sxy;

    emit_intermediate((void*)KEY_SX,  (void*)SX,  sizeof(void*)); 
    emit_intermediate((void*)KEY_SXX, (void*)SXX, sizeof(void*)); 
    emit_intermediate((void*)KEY_SY,  (void*)SY,  sizeof(void*)); 
    emit_intermediate((void*)KEY_SYY, (void*)SYY, sizeof(void*)); 
    emit_intermediate((void*)KEY_SXY, (void*)SXY, sizeof(void*)); 
}

static int linear_regression_partition(int reduce_tasks, void* key, int key_size)
{
    return default_partition(reduce_tasks, (void *)&key, key_size);
}

/** linear_regression_reduce()
 *
 */
static void linear_regression_reduce(void *key_in, iterator_t *itr)
{
    long long *sumptr = CALLOC(sizeof(long long), 1);
    register long long sum = 0;
    long long *val;

    assert (itr);

    while (iter_next (itr, (void **)&val))
    {
        sum += *val;
        free (val);
    }

    *sumptr = sum;
    emit(key_in, (void *)sumptr);
}

static void *linear_regression_combiner (iterator_t *itr)
{
    long long *sumptr = CALLOC(sizeof(long long), 1);
    register long long sum = 0;
    long long *val;

    assert(itr);

    while (iter_next (itr, (void **)&val))
    {
        sum += *val;
        free (val);
    }

    *sumptr = sum;
    return (void *)sumptr;
}

int main(int argc, char *argv[]) {

    final_data_t final_vals;
    int fd;
    char * fdata;
    char * fname;
    struct stat finfo;
    int i;

    struct timeval starttime,endtime;
    struct timeval begin, end;

    get_time (&begin);

    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        printf("USAGE: %s <filename>\n", argv[0]);
        exit(1);
    }
    
    fname = argv[1];

    printf("Linear Regression: Running...\n");
    
    // Read in the file
    CHECK_ERROR((fd = open(fname, O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd, &finfo) < 0);
#ifndef NO_MMAP
    // Memory map the file
    CHECK_ERROR((fdata = mmap(0, finfo.st_size + 1, 
        PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)) == NULL);
#else
    uint64_t ret;

    fdata = (char *)malloc (finfo.st_size);
    CHECK_ERROR (fdata == NULL);

    ret = read (fd, fdata, finfo.st_size);
    CHECK_ERROR (ret != finfo.st_size);
#endif

    CHECK_ERROR (map_reduce_init ());

    // Setup scheduler args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = fdata; // Array to regress
    map_reduce_args.map = linear_regression_map;
    map_reduce_args.reduce = linear_regression_reduce; // Identity Reduce
    map_reduce_args.combiner = linear_regression_combiner;
    map_reduce_args.splitter = NULL; // Array splitter;
    map_reduce_args.key_cmp = intkeycmp;
    map_reduce_args.unit_size = sizeof(POINT_T);
    map_reduce_args.partition = linear_regression_partition; 
    map_reduce_args.result = &final_vals;
    map_reduce_args.data_size = finfo.st_size - (finfo.st_size % map_reduce_args.unit_size);
    map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));//1024 * 512;
    map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
    map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
    map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;

    printf("Linear Regression: Calling MapReduce Scheduler\n");

    gettimeofday(&starttime,0);

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "initialize: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);
    CHECK_ERROR (map_reduce (&map_reduce_args) < 0);
    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "library: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);

    CHECK_ERROR (map_reduce_finalize ());

    long long n;
    double a, b, xbar, ybar, r2;
    long long SX_ll = 0, SY_ll = 0, SXX_ll = 0, SYY_ll = 0, SXY_ll = 0;
    // ADD UP RESULTS
    for (i = 0; i < final_vals.length; i++)
    {
        keyval_t * curr = &final_vals.data[i];
        switch ((intptr_t)curr->key)
        {
        case KEY_SX:
             SX_ll = (*(long long*)curr->val);
             break;
        case KEY_SY:
             SY_ll = (*(long long*)curr->val);
             break;
        case KEY_SXX:
             SXX_ll = (*(long long*)curr->val);
             break;
        case KEY_SYY:
             SYY_ll = (*(long long*)curr->val);
             break;
        case KEY_SXY:
             SXY_ll = (*(long long*)curr->val);
             break;
        default:
             // INVALID KEY
             CHECK_ERROR(1);
        }
        free(curr->val);
    }

    double SX = (double)SX_ll;
    double SY = (double)SY_ll;
    double SXX= (double)SXX_ll;
    double SYY= (double)SYY_ll;
    double SXY= (double)SXY_ll;

    n = (long long) finfo.st_size / sizeof(POINT_T); 
    b = (double)(n*SXY - SX*SY) / (n*SXX - SX*SX);
    a = (SY_ll - b*SX_ll) / n;
    xbar = (double)SX_ll / n;
    ybar = (double)SY_ll / n;
    r2 = (double)(n*SXY - SX*SY) * (n*SXY - SX*SY) / ((n*SXX - SX*SX)*(n*SYY - SY*SY));

    gettimeofday(&endtime,0);

    printf("Linear Regression: Completed %ld\n",(endtime.tv_sec - starttime.tv_sec));

    printf("Linear Regression Results:\n");
    printf("\ta     = %lf\n", a);
    printf("\tb     = %lf\n", b);
    printf("\txbar = %lf\n", xbar);
    printf("\tybar = %lf\n", ybar);
    printf("\tr2    = %lf\n", r2);
    printf("\tSX    = %lld\n", SX_ll);
    printf("\tSY    = %lld\n", SY_ll);
    printf("\tSXX  = %lld\n", SXX_ll);
    printf("\tSYY  = %lld\n", SYY_ll);
    printf("\tSXY  = %lld\n", SXY_ll);

    free(final_vals.data);

#ifndef NO_MMAP
    CHECK_ERROR(munmap(fdata, finfo.st_size + 1) < 0);
#else
    free (fdata);
#endif
    CHECK_ERROR(close(fd) < 0);

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "finalize: %u\n", time_diff (&end, &begin));
#endif

    return 0;
}
