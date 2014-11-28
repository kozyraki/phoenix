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

struct POINT_T {
    char x;
    char y;
};

enum {
    KEY_SX = 0,
    KEY_SY,
    KEY_SXX,
    KEY_SYY,
    KEY_SXY,
    KEY_COUNT
};

#ifdef MUST_USE_HASH
class lrMR : public MapReduce<lrMR, POINT_T, unsigned char, uint64_t, hash_container< unsigned char, uint64_t, sum_combiner, std::tr1::hash<unsigned char>
#elif defined(MUST_USE_FIXED_HASH)
class lrMR : public MapReduce<lrMR, POINT_T, unsigned char, uint64_t, fixed_hash_container< unsigned char, uint64_t, sum_combiner, 32768, std::tr1::hash<unsigned char>
#else
class lrMR : public MapReduce<lrMR, POINT_T, unsigned char, uint64_t, array_container< unsigned char, uint64_t, sum_combiner, KEY_COUNT
#endif
#ifdef TBB
    , tbb::scalable_allocator
#endif
> >
{
public:
    void map(data_type const& p, map_container& out) const
    {
        uint64_t px = p.x, py = p.y;        // cast first so multiply happens in 64-bits
        emit_intermediate(out, KEY_SXX, px*px);
        emit_intermediate(out, KEY_SYY, py*py);
        emit_intermediate(out, KEY_SXY, px*py);
        emit_intermediate(out, KEY_SX,  px);
        emit_intermediate(out, KEY_SY,  py);
    }
};

int main(int argc, char *argv[]) {

    int fd;
    char * fdata;
    char * fname;
    struct stat finfo;
    
    struct timespec begin, end;

    get_time (begin);

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
    uint64_t ret;

    fdata = (char *)malloc (finfo.st_size);
    CHECK_ERROR (fdata == NULL);

    ret = read (fd, fdata, finfo.st_size);
    CHECK_ERROR (ret != finfo.st_size);
#endif

    int data_size = finfo.st_size / sizeof(POINT_T);
    printf("data size: %d\n", data_size);
    printf("Linear Regression: Calling MapReduce Scheduler\n");

    get_time (end);
    print_time("initialize", begin, end);

    std::vector<lrMR::keyval> result;
    get_time (begin);
    lrMR mapReduce;
    CHECK_ERROR( mapReduce.run((POINT_T*)fdata, data_size, result) < 0);    
    get_time (end);
    print_time("library", begin, end);

    get_time (begin);

    long long n;
    double a, b, xbar, ybar, r2;
    long long SX_ll = 0, SY_ll = 0, SXX_ll = 0, SYY_ll = 0, SXY_ll = 0;
    
    // PULL OUT RESULTS
    for (size_t i = 0; i < result.size(); i++)
    {
        switch (result[i].key)
        {
        case KEY_SX:
             SX_ll = result[i].val;
             break;
        case KEY_SY:
             SY_ll = result[i].val;
             break;
        case KEY_SXX:
             SXX_ll = result[i].val;
             break;
        case KEY_SYY:
             SYY_ll = result[i].val;
             break;
        case KEY_SXY:
             SXY_ll = result[i].val;
             break;
        default:
             // INVALID KEY
             CHECK_ERROR(1);
            break;
        }
    }

    double SX = (double)SX_ll;
    double SY = (double)SY_ll;
    double SXX= (double)SXX_ll;
    double SYY= (double)SYY_ll;
    double SXY= (double)SXY_ll;

    n = (long long) data_size; 
    b = (double)(n*SXY - SX*SY) / (n*SXX - SX*SX);
    a = (SY_ll - b*SX_ll) / n;
    xbar = (double)SX_ll / n;
    ybar = (double)SY_ll / n;
    r2 = (double)(n*SXY - SX*SY) * (n*SXY - SX*SY) / ((n*SXX - SX*SX)*(n*SYY - SY*SY));

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

#ifndef NO_MMAP
    CHECK_ERROR(munmap(fdata, finfo.st_size + 1) < 0);
#else
    free (fdata);
#endif
    CHECK_ERROR(close(fd) < 0);

    get_time (end);
    print_time("finalize", begin, end);

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
