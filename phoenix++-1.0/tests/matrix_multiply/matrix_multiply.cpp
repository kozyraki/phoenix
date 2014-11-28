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
#include <assert.h>

#include "map_reduce.h"

struct mm_data_t {
    int row_num;
    int rows;
    int *matrix_A;
    int *matrix_B;
    int matrix_len;
    int* output;
};

class MatrixMulMR : public MapReduce<MatrixMulMR, mm_data_t, int, int>
{
    int *matrix_A, *matrix_B;
    int matrix_size;
    int row;
    int *output;

public:
    explicit MatrixMulMR(int* _mA, int* _mB, int size, int* out) : 
        matrix_A(_mA), matrix_B(_mB), matrix_size(size), row(0), output(out) {}
        
    void* locate(mm_data_t* d, uint64_t len) const
    {
        return d->matrix_A + d->row_num * d->matrix_len;
    }

    /** matrixmul_map()
     * Multiplies the allocated regions of matrix to compute partial sums 
     */
    void map(mm_data_t const& data, map_container& out) const
    {
        int* a_ptr = data.matrix_A + data.row_num*data.matrix_len + 0;
        int* b_ptr = data.matrix_B + 0;
            
        int* output = data.output + data.row_num*data.matrix_len;
        for(int i = 0; i < data.matrix_len ; i++) {
            for(int j=0;j<data.matrix_len ; j++) {
                output[j] += a_ptr[i] * b_ptr[j];
            }
            b_ptr += data.matrix_len;
        }
    }

    /** matrixmul_split()
     *  Assign a set of rows of the output matrix to each map task 
     * (for now, default to one row per task)
     */
    int split(mm_data_t& out)
    {
        /* End of data reached, return FALSE. */
        if (row >= matrix_size)
        {
            return 0;
        }

        out.matrix_A = matrix_A;
        out.matrix_B = matrix_B;
        out.matrix_len = matrix_size;
        out.output = output;
        out.rows = 1;
        out.row_num = row++;
        
        /* Return true since the out data is valid. */
        return 1;
    }
};

int main(int argc, char *argv[]) {

    int i,j, create_files;
    int fd_A, fd_B, file_size;
    int * fdata_A, *fdata_B;
    int matrix_len, row_block_len;
    struct stat finfo_A, finfo_B;
    char const* fname_A, *fname_B;
    int ret;
    
    struct timespec begin, end;

    get_time (begin);
    
    srand( (unsigned)time( NULL ) );

    // Make sure a filename is specified
    if (argv[1] == NULL)
    {
        dprintf("USAGE: %s [side of matrix] [size of Row block]\n", argv[0]);
        exit(1);
    }

    fname_A = "matrix_file_A.txt";
    fname_B = "matrix_file_B.txt";
    CHECK_ERROR ( (matrix_len = atoi(argv[1])) < 0);
    file_size = ((matrix_len*matrix_len))*sizeof(int);

    fprintf(stderr, "***** file size is %d\n", file_size);

    if(argv[2] == NULL)
        row_block_len = 1;
    else
        CHECK_ERROR ( (row_block_len = atoi(argv[2])) < 0);

    if(argv[3] != NULL)
        create_files = 1;
    else
        create_files = 0;

    printf("MatrixMult: Side of the matrix is %d\n", matrix_len);
    printf("MatrixMult: Row Block Len is %d\n", row_block_len);
    printf("MatrixMult: Running...\n");

    /* If the matrix files do not exist, create them */
    if(create_files)
    {
        dprintf("Creating files\n");

        int value = 0;
        CHECK_ERROR((fd_A = open(fname_A,O_CREAT | O_RDWR,S_IRWXU)) < 0);
        CHECK_ERROR((fd_B = open(fname_B,O_CREAT | O_RDWR,S_IRWXU)) < 0);
        
        for(i=0;i<matrix_len;i++)
        {
            for(j=0;j<matrix_len;j++)
            {
                value = (rand())%11;
                ret = write(fd_A,&value,sizeof(int));
                assert(ret == sizeof(int));
                //dprintf("%d  ",value);
            }
            //dprintf("\n");
        }
        //dprintf("\n");

        for(i=0;i<matrix_len;i++)
        {
            for(j=0;j<matrix_len;j++)
            {
                value = (rand())%11;
                ret = write(fd_B,&value,sizeof(int));
                assert(ret == sizeof(int));
                //dprintf("%d  ",value);
            }
            //dprintf("\n");
        }

        CHECK_ERROR(close(fd_A) < 0);
        CHECK_ERROR(close(fd_B) < 0);
    }

    // Read in the file
    CHECK_ERROR((fd_A = open(fname_A,O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd_A, &finfo_A) < 0);
#ifndef NO_MMAP
#ifdef MMAP_POPULATE
    // Memory map the file
    CHECK_ERROR((fdata_A = (int*)mmap(0, finfo_A.st_size + 1, 
        PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_A, 0)) == NULL);
#else
    // Memory map the file
    CHECK_ERROR((fdata_A = (int*)mmap(0, finfo_A.st_size + 1, 
        PROT_READ, MAP_PRIVATE, fd_A, 0)) == NULL);
#endif
#else
    int ret;

    fdata_A = (char *)malloc (file_size);
    CHECK_ERROR (fdata_A == NULL);

    ret = read (fd_A, fdata_A, file_size);
    CHECK_ERROR (ret != file_size);
#endif

    // Read in the file
    CHECK_ERROR((fd_B = open(fname_B,O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd_B, &finfo_B) < 0);
#ifndef NO_MMAP
#ifdef MMAP_POPULATE
    // Memory map the file
    CHECK_ERROR((fdata_B = (int*)mmap(0, finfo_B.st_size + 1, 
        PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_B, 0)) == NULL);
#else
    // Memory map the file
    CHECK_ERROR((fdata_B = (int*)mmap(0, finfo_B.st_size + 1, 
        PROT_READ, MAP_PRIVATE, fd_B, 0)) == NULL);
#endif
#else
    fdata_B = (char *)malloc (file_size);
    CHECK_ERROR (fdata_B == NULL);

    ret = read (fd_B, fdata_B, file_size);
    CHECK_ERROR (ret != file_size);
#endif

    int* output = (int*)calloc(matrix_len*matrix_len, sizeof(int));

    fprintf(stderr, "***** data size is %ld\n", (intptr_t)file_size);
    printf("MatrixMult: Calling MapReduce Scheduler Matrix Multiplication\n");

    get_time (end);
    print_time("initialize", begin, end);

    get_time (begin);
    std::vector<MatrixMulMR::keyval> result;
    MatrixMulMR mapReduce(fdata_A, fdata_B, matrix_len, output);
    mapReduce.run(result);
    get_time (end);
    print_time("library", begin, end);

    get_time (begin);
    int sum = 0;
    for(i=0;i<matrix_len*matrix_len;i++)
    {
          sum += output[i];
    }
    printf ("MatrixMult: total sum is %d\n", sum);

    printf("MatrixMult: MapReduce Completed\n");

    free(output);

#ifndef NO_MMAP
    CHECK_ERROR(munmap(fdata_A, file_size + 1) < 0);
#else
    free (fdata_A);
#endif
    CHECK_ERROR(close(fd_A) < 0);

#ifndef NO_MMAP
    CHECK_ERROR(munmap(fdata_B, file_size + 1) < 0);
#else
    free (fdata_B);
#endif
    CHECK_ERROR(close(fd_B) < 0);

    get_time (end);
    print_time("finalize", begin, end);

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
