/* Copyright (c) 2007-2011, Stanford University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*      * Redistributions of source code must retain the above copyright
*         notice, this list of conditions and the following disclaimer.
*      * Redistributions in binary form must reproduce the above copyright
*         notice, this list of conditions and the following disclaimer in the
*         documentation and/or other materials provided with the distribution.
*      * Neither the name of Stanford University nor the names of its 
*         contributors may be used to endorse or promote products derived from 
*         this software without specific prior written permission.
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
#include <limits>

#include "map_reduce.h"

#ifdef TBB
#include "tbb/scalable_allocator.h"
#endif

#define DEF_NUM_POINTS 100000
#define DEF_NUM_MEANS 100
#define DEF_DIM 3
#define DEF_GRID_SIZE 1000


int num_points; // number of vectors
int dim;         // Dimension of each vector
int num_means; // number of clusters
int grid_size; // size of each dimension of vector space
int modified;
int num_pts = 0;


struct point
{
    int* d;
    int cluster;  // Cluster id or cluster point count (for means)
    
    point() { d = NULL; cluster = -1; }
    point(int* d, int cluster) { this->d = d; this->cluster = cluster; }
    
    point& normalize() {
	for(int i = 0; i < dim; ++i)
             d[i] /= cluster;
        cluster = 1;
        return *this;
    }
    
    unsigned int sq_dist(point const& p)
    {
        unsigned int sum = 0;
        for (int i = 0; i < dim; i++) 
        {
            int diff = d[i] - p.d[i];
            sum += diff * diff;
        }
        return sum;
    }
    
    void dump() {
        for(int j = 0; j < dim; j++)
            printf("%5d ", d[j]);
        printf("\n");
    }
    
    void generate() {
        for(int j = 0; j < dim; j++)
            d[j] = rand() % grid_size;
    }
};

/** parse_args()
 *  Parse the user arguments
 */
void parse_args(int argc, char **argv) 
{
    int c;
    extern char *optarg;
    
    num_points = DEF_NUM_POINTS;
    num_means = DEF_NUM_MEANS;
    dim = DEF_DIM;
    grid_size = DEF_GRID_SIZE;
    
    while ((c = getopt(argc, argv, "d:c:p:s:")) != EOF) 
    {
        switch (c) {
            case 'd':
                dim = atoi(optarg);
                break;
            case 'c':
                num_means = atoi(optarg);
                break;
            case 'p':
                num_points = atoi(optarg);
                break;
            case 's':
                grid_size = atoi(optarg);
                break;
            case '?':
                printf("Usage: %s -d <vector dimension> -c <num clusters> -p <num points> -s <max value>\n", argv[0]);
                exit(1);
        }
    }
    
    if (dim <= 0 || num_means <= 0 || num_points <= 0 || grid_size <= 0) {
        printf("Illegal argument value. All values must be numeric and greater than 0\n");
        exit(1);
    }
    
    printf("Dimension = %d\n", dim);
    printf("Number of clusters = %d\n", num_means);
    printf("Number of points = %d\n", num_points);
    printf("Size of each dimension = %d\n", grid_size);    
}

template<class V, template<class> class Allocator>
class point_combiner : public associative_combiner<point_combiner<V, Allocator>, V, Allocator> 
{
public:
     static void F(point& a, point const& b) { 
         a.cluster += b.cluster;
         for(int i = 0; i < dim; i++) a.d[i] += b.d[i]; 
     }
     static void Init(point& a) { 
         a.cluster = 0;
         a.d = (int*)calloc(dim, sizeof(int)); 
     }
     static bool Empty(point const& a) { 
         return a.cluster == 0; 
     }
};

#ifdef MUST_USE_HASH
class KmeansMR : public MapReduce<KmeansMR, point, intptr_t, point, hash_container<intptr_t, point, point_combiner, std::tr1::hash<intptr_t>
#elif defined(MUST_USE_FIXED_HASH)
class KmeansMR : public MapReduce<KmeansMR, point, intptr_t, point, fixed_hash_container<intptr_t, point, point_combiner, 256, std::tr1::hash<intptr_t>
#else
class KmeansMR : public MapReduce<KmeansMR, point, intptr_t, point, array_container<intptr_t, point, point_combiner, DEF_NUM_MEANS
#endif
#ifdef TBB
    , tbb::scalable_allocator
#endif
> >
{
    std::vector<point> const& means;
public:    

    void* locate(data_type* d, uint64_t len) const
    {
        return d->d;
    }

    void map(data_type& p, map_container& out) const
    {
        unsigned int min_dist = std::numeric_limits<unsigned int>::max();
        uint64_t min_idx = 0;

        for (size_t j = 0; j < means.size(); j++)
        {
            unsigned int cur_dist = p.sq_dist(means[j]);
            if (cur_dist < min_dist) 
            {
                min_dist = cur_dist;
                min_idx = j; 
            }
        }

        if (p.cluster != (int)min_idx) 
        {
            p.cluster = (int)min_idx;
            modified = true;
        }

        emit_intermediate(out, min_idx, point(p.d, 1));
    }
    
    KmeansMR(std::vector<point> const& means)
#ifdef MUST_USE_HASH 
        : MapReduce<KmeansMR, point, intptr_t, point, hash_container<intptr_t, point, point_combiner, std::tr1::hash<intptr_t>
#elif defined(MUST_USE_FIXED_HASH)
        : MapReduce<KmeansMR, point, intptr_t, point, fixed_hash_container<intptr_t, point, point_combiner, 256, std::tr1::hash<intptr_t>
#else
        : MapReduce<KmeansMR, point, intptr_t, point, array_container<intptr_t, point, point_combiner, DEF_NUM_MEANS
#endif
#ifdef TBB
    , tbb::scalable_allocator
#endif
    > >(), means(means)
    {}
};

int main(int argc, char **argv)
{
    std::vector<point> means;
    
    struct timespec begin, end, ibegin, iend;
    double library_time = 0;
    double inter_library_time = 0;

    get_time (begin);
    
    parse_args(argc, argv);    
    
    // get points
    int* pointdata = (int *)malloc(sizeof(int) * num_points * dim);
    point* points = new point[num_points];
    for(int i = 0; i < num_points; i++) {
        points[i] = point(&pointdata[i*dim], -1);
        points[i].generate();
    }

    // get means
    for (int i=0; i<num_means; i++) {
        means.push_back(point((int*)malloc(sizeof(int) * dim), 0));
        means[i].generate();
    } 
    
    modified = true;

    get_time (end);
    print_time("initialize", begin, end);

    printf("KMeans: Calling MapReduce Scheduler\n");

    KmeansMR* mapReduce = new KmeansMR(means);
    while (modified == true)
    {
        get_time (ibegin);
        modified = false;
        //dprintf(".");
        std::vector<KmeansMR::keyval> result;
        get_time (begin);        
        CHECK_ERROR( mapReduce->run(points, num_points, result) < 0);
        get_time (end);
        library_time += time_diff (end, begin);

        for (size_t i = 0; i < result.size(); i++)
        {
            free(means[result[i].key].d);
            means[result[i].key] = result[i].val.normalize();
        }
        get_time (iend);
        inter_library_time += time_diff (iend, ibegin) - time_diff(end, begin);
    } 
    delete mapReduce;

    print_time("library", library_time);
    print_time("inter library", inter_library_time);

    get_time (begin);

    dprintf("\n");
    printf("KMeans: MapReduce Completed\n");  

    printf("\n\nFinal means:\n");
    for(int i = 0; i < num_means; i++)
        means[i].dump();

    free(pointdata);
    delete [] points;
    
    get_time (end);

    print_time("finalize", begin, end);

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
