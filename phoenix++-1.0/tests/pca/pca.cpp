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

#ifdef TBB
#include "tbb/scalable_allocator.h"
#endif

#include "map_reduce.h"

typedef struct {
    int row_num;
    int const* matrix;
} pca_map_data_t;

typedef struct {
    int const* matrix;
    long long const* means;
    int row_num;
    int col_num;
} pca_cov_data_t;

#define DEF_GRID_SIZE 100  // all values in the matrix are from 0 to this value 
#define DEF_NUM_ROWS 10
#define DEF_NUM_COLS 10

int num_rows;
int num_cols;
int grid_size;

/** parse_args()
 *  Parse the user arguments to determine the number of rows and colums
 */  
void parse_args(int argc, char **argv) 
{
    int c;
    extern char *optarg;
    
    num_rows = DEF_NUM_ROWS;
    num_cols = DEF_NUM_COLS;
    grid_size = DEF_GRID_SIZE;
    
    while ((c = getopt(argc, argv, "r:c:s:")) != EOF) 
    {
        switch (c) {
            case 'r':
                num_rows = atoi(optarg);
                break;
            case 'c':
                num_cols = atoi(optarg);
                break;
            case 's':
                grid_size = atoi(optarg);
                break;
            case '?':
                printf("Usage: %s -r <num_rows> -c <num_cols> -s <max value>\n", argv[0]);
                exit(1);
        }
    }
    
    if (num_rows <= 0 || num_cols <= 0 || grid_size <= 0) {
        printf("Illegal argument value. All values must be numeric and greater than 0\n");
        exit(1);
    }

    printf("Number of rows = %d\n", (int)num_rows);
    printf("Number of cols = %d\n", (int)num_cols);
    printf("Max value for each element = %d\n", (int)grid_size);    
}

/** dump_points()
 *  Print the values in the matrix to the screen
 */
void dump_points(int *vals, int rows, int cols)
{
    int i, j;
    
    for (i = 0; i < rows; i++) 
    {
        for (j = 0; j < cols; j++)
        {
            printf("%5d ",vals[i*cols+j]);
        }
        printf("\n");
    }
}

/** generate_points()
 *  Create the values in the matrix
 */
void generate_points(int *pts, int rows, int cols) 
{    
    int i, j;
       
    for (i=0; i<rows; i++) 
    {
        for (j=0; j<cols; j++) 
        {
            pts[i * cols + j] = rand() % grid_size;
        }
    }
}

#ifdef MUST_USE_HASH
class MeanMR : public MapReduce<MeanMR, pca_map_data_t, int, long long, hash_container<int, long long, one_combiner, std::tr1::hash<int>
#elif defined(MUST_USE_FIXED_HASH)
class MeanMR : public MapReduce<MeanMR, pca_map_data_t, int, long long, fixed_hash_container<int, long long, one_combiner, 32768, std::tr1::hash<int>
#else
class MeanMR : public MapReduce<MeanMR, pca_map_data_t, int, long long, common_array_container<int, long long, one_combiner, 1000
#endif
#ifdef TBB
    , tbb::scalable_allocator
#endif
> >
{
    int *matrix;
    int row;

public:
    explicit MeanMR(int* _matrix) : matrix(_matrix), row(0) {}

    void* locate(data_type* d, uint64_t len) const
    {
        return (void*)const_cast<int*>(d->matrix + d->row_num * num_cols);
    }

    /** pca_mean_map()
     *  Map task to compute the mean
     */
    void map(data_type const& data, map_container& out) const
    {
        long long sum = 0;
        int const* m = data.matrix + data.row_num * num_cols;
        for(int j = 0; j < num_cols; j++)
        {
            sum += m[j];
        }
        emit_intermediate(out, data.row_num, sum/num_cols);
    }    

    int split(pca_map_data_t& out)
    {
        /* End of data reached, return FALSE. */
        if (row >= num_rows)
        {
            return 0;
        }

        out.matrix = matrix;
        out.row_num = row++;
        
        /* Return true since the out data is valid. */
        return 1;
    }
};

#ifdef MUST_USE_HASH
class CovMR : public MapReduceSort<CovMR, pca_cov_data_t, intptr_t, long long, hash_container<intptr_t, long long, one_combiner, std::tr1::hash<intptr_t>
#elif defined(MUST_USE_FIXED_HASH)
class CovMR : public MapReduceSort<CovMR, pca_cov_data_t, intptr_t, long long, fixed_hash_container<intptr_t, long long, one_combiner, 256, std::tr1::hash<intptr_t>
#else
class CovMR : public MapReduceSort<CovMR, pca_cov_data_t, intptr_t, long long, common_array_container<intptr_t, long long, one_combiner, 1000*1000
#endif
#ifdef TBB
    , tbb::scalable_allocator
#endif
> >
{
    int *matrix;
    long long const* means;
    int row;
    int col;

public:
    explicit CovMR(int* _matrix, long long const* _means) : 
        matrix(_matrix), means(_means), row(0), col(0) {}

    void* locate(data_type* d, uint64_t len) const
    {
        return (void*)const_cast<int*>(d->matrix + d->row_num * num_cols);
    }

    /** pca_cov_map()
     *  Map task for computing the covariance matrix
     * 
     */
    void map(data_type const& data, map_container& out) const
    {
        int const* v1 = data.matrix + data.row_num*num_cols;
        int const* v2 = data.matrix + data.col_num*num_cols;
        long long m1 = data.means[data.row_num];
        long long m2 = data.means[data.col_num];
        long long sum = 0;
        for(int i = 0; i < num_cols; i++)
        {
            sum += (v1[i] - m1) * (v2[i] - m2);
        }
        sum /= (num_cols-1);
        emit_intermediate(out, data.row_num*num_cols + data.col_num, sum);
    }

    /** pca_cov_split()
     *  Splitter function for computing the covariance
     *  Need to produce a task for each variable (row) pair
     */
    int split(pca_cov_data_t& out)
    {
        /* End of data reached, return FALSE. */
        if (row >= num_rows)
        {
            return 0;
        }

        out.matrix = matrix;
        out.means = means;
        out.row_num = row;
        out.col_num = col;

        col++;
        if(col >= num_rows) // yes, num_rows
        {
            row++;
            // will only compute upper right triangle since 
            // covariance matrix is symmetric
            col = row;        
        }
        
        /* Return true since the out data is valid. */
        return 1;
    }
};


int main(int argc, char **argv)
{
    struct timespec begin, end;
    double library_time = 0;

    get_time (begin);
    
    parse_args(argc, argv);    
    
    // Allocate space for the matrix
    int* matrix = (int *)malloc(sizeof(int) * num_rows * num_cols);
    
    //Generate random values for all the points in the matrix 
    generate_points(matrix, num_rows, num_cols);
    
    // Print the points
    //dump_points(matrix, num_rows, num_cols);
    
    // Setup scheduler args for computing the mean
    
    printf("PCA Mean: Calling MapReduce Scheduler\n");

    get_time (end);
    print_time("initialize", begin, end);

    get_time (begin); 
    std::vector<MeanMR::keyval> result;
    MeanMR meanMR(matrix);
    meanMR.run(result);
    get_time (end);
    library_time += time_diff (end, begin);
    printf("PCA Mean: MapReduce Completed\n"); 
    
    get_time (begin);
    assert (result.size() == (size_t)num_rows);
    
    long long* means = new long long[num_rows];
    for(size_t i = 0; i < result.size(); i++)
    {
        means[result[i].key] = result[i].val;
    }

    get_time (end);

    print_time("inter library", begin, end);

    printf("PCA Cov: Calling MapReduce Scheduler\n");
 
    get_time (begin);
    std::vector<CovMR::keyval> result2;
    CovMR covMR(matrix, means);
    covMR.run(result2);
    get_time (end);

    library_time += time_diff (end, begin);
    print_time("library", library_time);

    get_time (begin);

    printf("PCA Cov: MapReduce Completed\n"); 

    assert(result2.size() == (size_t)((((num_rows * num_rows) - num_rows)/2) + num_rows));
   
    // Free the allocated structures
    int cnt = 0;
    long long sum = 0;
    for (size_t i = 0; i < result2.size(); i++) 
    {
        sum += result2[i].val;
        
        //printf("%5d ", result2[i].val);
        cnt++;
        if (cnt == num_rows)
        {
            //printf("\n"); 
            num_rows--;
            cnt = 0;
        }
    }
    printf("\n\nCovariance sum: %lld\n", sum);
    
    delete [] means;
    free (matrix);

    get_time (end);
    print_time("finalize", begin, end);

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
