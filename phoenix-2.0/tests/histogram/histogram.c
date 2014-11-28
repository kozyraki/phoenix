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
#include <inttypes.h>

#include "map_reduce.h"
#include "stddefines.h"

#define IMG_DATA_OFFSET_POS 10
#define BITS_PER_PIXEL_POS 28


int swap;        // to indicate if we need to swap byte order of header information
short red_keys[256];
short green_keys[256];
short blue_keys[256];

/* test_endianess
 *
 */
void test_endianess() {
    unsigned int num = 0x12345678;
    char *low = (char *)(&(num));
    if (*low ==  0x78) {
        dprintf("No need to swap\n");
        swap = 0;
    }
    else if (*low == 0x12) {
        dprintf("Need to swap\n");
        swap = 1;
    }
    else {
        printf("Error: Invalid value found in memory\n");
        exit(1);
    } 
}

/* swap_bytes
 *
 */
void swap_bytes(char *bytes, int num_bytes) {
    int i;
    char tmp;
    
    for (i = 0; i < num_bytes/2; i++) {
        dprintf("Swapping %d and %d\n", bytes[i], bytes[num_bytes - i - 1]);
        tmp = bytes[i];
        bytes[i] = bytes[num_bytes - i - 1];
        bytes[num_bytes - i - 1] = tmp;    
    }
}

/* myshortcmp
 * Comparison function
 */
int myshortcmp(const void *s1, const void *s2)
{    
    short val1 = *((short *)s1);
    short val2 = *((short *)s2);
    
    if (val1 < val2) {
        return -1;
    }
    else if (val1 > val2) {
        return 1;
    }
    else {
        return 0;
    }
}

/** hist_map()
 * Map function that computes the histogram values for the portion
 * of the image assigned to the map task
 */
void hist_map(map_args_t *args) 
{
    int i;
    short *key;
    unsigned char *val;
    intptr_t red[256];
    intptr_t green[256];
    intptr_t blue[256];

    assert(args);
    unsigned char *data = (unsigned char *)args->data;
    assert(data);

    memset(&(red[0]), 0, sizeof(intptr_t) * 256);
    memset(&(green[0]), 0, sizeof(intptr_t) * 256);
    memset(&(blue[0]), 0, sizeof(intptr_t) * 256);
    
    for (i = 0; i < (args->length) * 3; i+=3) 
    {
        val = &(data[i]);
        blue[*val]++;
        
        val = &(data[i+1]);
        green[*val]++;
        
        val = &(data[i+2]);
        red[*val]++;    
    }
    
    for (i = 0; i < 256; i++) 
    {
        if (blue[i] > 0) {
            key = &(blue_keys[i]);
            emit_intermediate((void *)key, (void *)blue[i], (int)sizeof(short));
        }
        
        if (green[i] > 0) {
            key = &(green_keys[i]);
            emit_intermediate((void *)key, (void *)green[i], (int)sizeof(short));
        }
        
        if (red[i] > 0) {
            key = &(red_keys[i]);
            emit_intermediate((void *)key, (void *)red[i], (int)sizeof(short));
        }
    }
}

/** hist_reduce()
 * Reduce function that adds up the values for each location in the array
 */
void hist_reduce(void *key_in, iterator_t *itr)
{
    short *key = (short *)key_in;
    void *val;
    intptr_t sum = 0;

    assert(key);
    assert(itr);
    
    /* dprintf("For key %hd, there are %d vals\n", *key, vals_len); */
    
    while (iter_next (itr, &val))
    {
        sum += (intptr_t)val;
    }

    emit(key, (void *)sum);
}

void *hist_combiner (iterator_t *itr)
{
    void *val;
    intptr_t sum = 0;

    assert(itr);
    
    /* dprintf("For key %hd, there are %d vals\n", *key, vals_len); */
    
    while (iter_next (itr, &val))
    {
        sum += (intptr_t)val;
    }

    return (void *)sum;
}

int main(int argc, char *argv[]) {
    
    final_data_t hist_vals;
    int i;
    int fd;
    char *fdata;
    struct stat finfo;
    char * fname;
    struct timeval begin, end;

    get_time (&begin);

    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        printf("USAGE: %s <bitmap filename>\n", argv[0]);
        exit(1);
    }
    
    fname = argv[1];

    printf("Histogram: Running...\n");
    
    // Read in the file
    CHECK_ERROR((fd = open(fname, O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd, &finfo) < 0);
#ifndef NO_MMAP
    // Memory map the file
    CHECK_ERROR((fdata = mmap(0, finfo.st_size + 1, 
        PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)) == NULL);
#else
    int ret;
        
    fdata = (char *)malloc (finfo.st_size);
    CHECK_ERROR (fdata == NULL);

    ret = read (fd, fdata, finfo.st_size);
    CHECK_ERROR (ret != finfo.st_size);
#endif

    if ((fdata[0] != 'B') || (fdata[1] != 'M')) {
        printf("File is not a valid bitmap file. Exiting\n");
        exit(1);
    }
    
    test_endianess();     // will set the variable "swap"
    
    unsigned short *bitsperpixel = (unsigned short *)(&(fdata[BITS_PER_PIXEL_POS]));
    if (swap) {
        swap_bytes((char *)(bitsperpixel), sizeof(*bitsperpixel));
    }
    if (*bitsperpixel != 24) {     // ensure its 3 bytes per pixel
        printf("Error: Invalid bitmap format - ");
        printf("This application only accepts 24-bit pictures. Exiting\n");
        exit(1);
    }
    
    unsigned short *data_pos = (unsigned short *)(&(fdata[IMG_DATA_OFFSET_POS]));
    if (swap) {
        swap_bytes((char *)(data_pos), sizeof(*data_pos));
    }
    
    int imgdata_bytes = (int)finfo.st_size - (int)(*(data_pos));
    printf("This file has %d bytes of image data, %d pixels\n", imgdata_bytes,
                                                                                imgdata_bytes / 3);
    
    // We use this global variable arrays to store the "key" for each histogram
    // bucket. This is to prevent memory leaks in the mapreduce scheduler                                                                                
    for (i = 0; i < 256; i++) {
        blue_keys[i] = i;
        green_keys[i] = 1000 + i;
        red_keys[i] = 2000 + i;
    }

    CHECK_ERROR (map_reduce_init ());

    // Setup map reduce args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = &(fdata[*data_pos]);    //&hist_data;
    map_reduce_args.map = hist_map;
    map_reduce_args.reduce = hist_reduce;
    map_reduce_args.combiner = hist_combiner;
    map_reduce_args.splitter = NULL; //hist_splitter;
    map_reduce_args.key_cmp = myshortcmp;
    
    map_reduce_args.unit_size = 3;  // 3 bytes per pixel
    map_reduce_args.partition = NULL; // use default
    map_reduce_args.result = &hist_vals;
    
    map_reduce_args.data_size = imgdata_bytes;

    map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));//1024 * 512;
    map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
    map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
    map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;

    fprintf(stderr, "Histogram: Calling MapReduce Scheduler\n");

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "initialize: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);
    CHECK_ERROR( map_reduce (&map_reduce_args) < 0);
    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "library: %u\n", time_diff (&end, &begin));
#endif

    get_time (&begin);

    CHECK_ERROR (map_reduce_finalize ());

    short pix_val;
    intptr_t freq;
    short prev = 0;
    
    dprintf("\n\nBlue\n");
    dprintf("----------\n\n");
    for (i = 0; i < hist_vals.length; i++)
    {
        keyval_t * curr = &((keyval_t *)hist_vals.data)[i];
        pix_val = *((short *)curr->key);
        freq = (intptr_t)curr->val;
        
        if (pix_val - prev > 700) {
            if (pix_val >= 2000) {
                dprintf("\n\nRed\n");
                dprintf("----------\n\n");
            }
            else if (pix_val >= 1000) {
                dprintf("\n\nGreen\n");
                dprintf("----------\n\n");
            }
        }
        
        dprintf("%hd - %" PRIdPTR "\n", pix_val % 1000, freq);
        
        prev = pix_val;
    }

    free(hist_vals.data);

#ifndef NO_MMAP
    CHECK_ERROR (munmap (fdata, finfo.st_size + 1) < 0);
#else
    free (fdata);
#endif
    CHECK_ERROR (close (fd) < 0);

    get_time (&end);

#ifdef TIMING
    fprintf (stderr, "finalize: %u\n", time_diff (&end, &begin));
#endif

    return 0;
}
