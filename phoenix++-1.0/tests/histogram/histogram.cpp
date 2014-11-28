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

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "map_reduce.h"

#ifdef TBB
#include "tbb/scalable_allocator.h"
#endif

#define IMG_DATA_OFFSET_POS 10
#define BITS_PER_PIXEL_POS 28

struct pixel {
    unsigned char b;
    unsigned char g;
    unsigned char r;
};

#ifdef MUST_USE_HASH
class HistogramMR : public MapReduceSort<HistogramMR, pixel, intptr_t, uint64_t, hash_container<intptr_t, uint64_t, sum_combiner, std::tr1::hash<intptr_t>
#elif defined(MUST_USE_FIXED_HASH)
class HistogramMR : public MapReduceSort<HistogramMR, pixel, intptr_t, uint64_t, fixed_hash_container<intptr_t, uint64_t, sum_combiner, 32768, std::tr1::hash<intptr_t>
#else
class HistogramMR : public MapReduceSort<HistogramMR, pixel, intptr_t, uint64_t, array_container<intptr_t, uint64_t, sum_combiner, 768
#endif
#ifdef TBB
    , tbb::scalable_allocator
#endif
> >
{
public:
    void map(data_type const& p, map_container& out) const {
        emit_intermediate(out, p.b, 1);
        emit_intermediate(out, p.g+256, 1);
        emit_intermediate(out, p.r+512, 1);
    }
#ifdef MUST_REDUCE
    void reduce(key_type const& key, reduce_iterator const& values, std::vector<keyval>& out) const {
        value_type total=0, val;
        while (values.next(val))
        {
            total += val;
        }
        keyval kv = {key, val};
        out.push_back(kv);
    }
#endif
};

/* test_endianess
 *
 */
bool test_endianess() {
    unsigned int num = 0x12345678;
    char *low = (char *)(&(num));
    if (*low ==  0x78) {
        dprintf("No need to swap\n");
        return false;
    }
    else if (*low == 0x12) {
        dprintf("Need to swap\n");
        return true;
    }
    else {
        printf("Error: Invalid value found in memory\n");
        exit(1);
    } 
}


int main(int argc, char *argv[]) {
    int fd;
    char *fdata;
    struct stat finfo;
    char * fname;
    timespec begin, end;
 
    get_time (begin);
    
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
#ifdef MMAP_POPULATE
    // Memory map the file
    CHECK_ERROR((fdata = (char*)mmap(0, finfo.st_size + 1, 
        PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0)) == NULL);
#else
    // Memory map the file
    CHECK_ERROR((fdata = (char*)mmap(0, finfo.st_size + 1, 
        PROT_READ, MAP_PRIVATE, fd, 0)) == NULL);
#endif
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

    bool needsSwap = test_endianess();
    
    unsigned short bitsperpixel = 
        *(unsigned short *)(&(fdata[BITS_PER_PIXEL_POS]));
    if (needsSwap) {
        bitsperpixel = (bitsperpixel >> 8) + ((bitsperpixel & 255) << 8);
    }
    if (bitsperpixel != 24) {     // ensure it's 3 bytes per pixel
        printf("Error: Invalid bitmap format - ");
        printf("This application only accepts 24-bit pictures. Exiting\n");
        exit(1);
    } 
    
    unsigned short data_pos = *(unsigned short *)(&(fdata[IMG_DATA_OFFSET_POS]));
    if (needsSwap) {
        data_pos = (data_pos >> 8) + ((data_pos & 255) << 8);
    }
    
    int imgdata_bytes = (int)finfo.st_size - (int)data_pos;
    printf("This file has %d bytes of image data, %d pixels\n", 
        imgdata_bytes, imgdata_bytes / 3);
    get_time (end);
    print_time("initialize", begin, end);

    fprintf(stderr, "Histogram: Calling MapReduce Scheduler\n");
    get_time (begin);
    std::vector<HistogramMR::keyval> result;    
    HistogramMR* mapReduce = new HistogramMR();
	CHECK_ERROR( mapReduce->run(
        (pixel*)&(fdata[data_pos]), imgdata_bytes / 3, result) < 0);
    delete mapReduce;
    get_time (end);

    print_time("library", begin, end);

    get_time (begin);


    short pix_val;
    intptr_t freq;
    short prev = 0;
    
    printf("\n\nBlue\n");
    printf("----------\n\n");
    bool red = false, green = false;
    for (size_t i = 0; i < 10 && i < result.size(); i++)
    {
        pix_val = result[i].key;
        freq = result[i].val;
        if (freq == 0) continue;
        
        if (pix_val >= 256 && !green) {
            printf("\n\nGreen\n");
            printf("----------\n\n");
            green = true;
        }
        else if (pix_val >= 512 && !red) {
            printf("\n\nRed\n");
            printf("----------\n\n");
            red = true;
        }
        
        printf("%hd - %ld\n", pix_val%256, freq);
        
        prev = pix_val;
    }

#ifndef NO_MMAP
    CHECK_ERROR (munmap (fdata, finfo.st_size + 1) < 0);
#else
    free (fdata);
#endif
    CHECK_ERROR (close (fd) < 0);

    get_time (end);

    print_time("finalize", begin, end);

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
