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
#include <string.h>

#include "map_reduce.h"

#define DEFAULT_UNIT_SIZE 5
#define SALT_SIZE 2
#define MAX_REC_LEN 1024
#define OFFSET 5

typedef struct {
  char *keys;
  int keys_len;
  char *encrypt_file;
} str_map_data_t;

 char const* key1 = "Helloworld";
 char const* key2 = "howareyou";
 char const* key3 = "ferrari";
 char const* key4 = "whotheman";

 char *key1_final;
 char *key2_final;
 char *key3_final;
 char *key4_final;

/** compute_hashes()
 *  Simple Cipher to generate a hash of the word 
 */
void compute_hashes(char const* word, unsigned int length, char* final_word)
{
    unsigned int i;
    for(i=0;i<length;i++)
        final_word[i] = word[i]+OFFSET;
    final_word[i] = 0;
}

class MatchMR : public MapReduce<MatchMR, str_map_data_t, int, int>
{
    char *keys_file, *encrypt_file;
    int keys_file_len, encrypt_file_len;
    int splitter_pos, chunk_size;    

public:
    explicit MatchMR(char* keys, int keys_len, char* encrypt, int encrypt_len, int chunk_size) : keys_file(keys), encrypt_file(encrypt), keys_file_len(keys_len), encrypt_file_len(encrypt_len), splitter_pos(0), chunk_size(chunk_size) {}

    void *locate (data_type *data, uint64_t len) const
    {
        return data->keys;
    }

    void map(data_type& data, map_container& out) const
    {
        char cur_word_final[MAX_REC_LEN];

        int index = 0;
        while(index < data.keys_len)
        {
            char* key = data.keys + index;
            int len = 0;
            while(index+len < data.keys_len && data.keys[index+len] != '\r' && data.keys[index+len] != '\n')
                len++;

            compute_hashes(key, len, cur_word_final);

            if(!strcmp(key1_final, cur_word_final));
                dprintf("FOUND: WORD IS %s\n", key1);

            if(!strcmp(key2_final, cur_word_final));
                dprintf("FOUND: WORD IS %s\n", key2);

            if(!strcmp(key3_final, cur_word_final));
                dprintf("FOUND: WORD IS %s\n", key3);

            if(!strcmp(key4_final, cur_word_final));
                dprintf("FOUND: WORD IS %s\n", key4);

            index += len;
            while(index < data.keys_len && (data.keys[index] == '\r' || data.keys[index] == '\n'))
                index++;
        }
    }

    /** string_match_split()
     *  Splitter Function to assign portions of the file to each map task
     */
    int split(str_map_data_t& out)
    {
        /* End of data reached, return FALSE. */
        if ((int)splitter_pos >= keys_file_len)
        {
            return 0;
        }

        /* Determine the nominal end point. */
        int end = std::min(splitter_pos + chunk_size, keys_file_len);

        /* Move end point to next word break */
        while(end < keys_file_len && keys_file[end] != '\r' && keys_file[end] != '\n')
            end++;

        /* Set the start of the next data. */
        out.keys = keys_file + splitter_pos;
        out.keys_len = end - splitter_pos;
        
        // Skip line breaks...
        while(end < keys_file_len && keys_file[end] == '\r' && keys_file[end] == '\n')
            end++;
        splitter_pos = end;

        /* Return true since the out data is valid. */
        return 1;
    }
};

int main(int argc, char *argv[]) {
    
    int fd_keys;
    char *fdata_keys;
    struct stat finfo_keys;
    char *fname_keys;

    struct timespec begin, end;

    get_time (begin);

    if (argv[1] == NULL)
    {
        printf("USAGE: %s <keys filename>\n", argv[0]);
        exit(1);
    }
    fname_keys = argv[1];

    printf("String Match: Running...\n");

    // Read in the file
    CHECK_ERROR((fd_keys = open(fname_keys,O_RDONLY)) < 0);
    // Get the file info (for file length)
    CHECK_ERROR(fstat(fd_keys, &finfo_keys) < 0);
#ifndef NO_MMAP
#ifdef MMAP_POPULATE
    // Memory map the file
    CHECK_ERROR((fdata_keys = (char*)mmap(0, finfo_keys.st_size + 1, 
        PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_keys, 0)) == NULL);
#else
    // Memory map the file
    CHECK_ERROR((fdata_keys = (char*)mmap(0, finfo_keys.st_size + 1, 
        PROT_READ, MAP_PRIVATE, fd_keys, 0)) == NULL);
#endif
#else
    int ret;

    fdata_keys = (char *)malloc (finfo_keys.st_size);
    CHECK_ERROR (fdata_keys == NULL);

    ret = read (fd_keys, fdata_keys, finfo_keys.st_size);
    CHECK_ERROR (ret != finfo_keys.st_size);
#endif

    key1_final = (char*)malloc(strlen(key1)+1);
    key2_final = (char*)malloc(strlen(key2)+1);
    key3_final = (char*)malloc(strlen(key3)+1);
    key4_final = (char*)malloc(strlen(key4)+1);

    compute_hashes(key1, strlen(key1), key1_final);
    compute_hashes(key2, strlen(key2), key2_final);
    compute_hashes(key3, strlen(key3), key3_final);
    compute_hashes(key4, strlen(key4), key4_final);
    
    get_time (end);

    print_time("initialize", begin, end);

    printf("String Match: Calling String Match\n");

    get_time (begin);
    MatchMR mr(fdata_keys, finfo_keys.st_size, NULL, 0, 64*1024);
    std::vector<MatchMR::keyval> out;
    CHECK_ERROR (mr.run(out) < 0);
    get_time (end);

    print_time("library", begin, end);

    get_time (begin);

    free(key1_final);
    free(key2_final);
    free(key3_final);
    free(key4_final);

#ifndef NO_MMAP
    CHECK_ERROR(munmap(fdata_keys, finfo_keys.st_size + 1) < 0);
#else
    free (fdata_keys);
#endif
    CHECK_ERROR(close(fd_keys) < 0);

    get_time (end);

    print_time("finalize", begin, end);

    return 0;
}

// vim: ts=8 sw=4 sts=4 smarttab smartindent
