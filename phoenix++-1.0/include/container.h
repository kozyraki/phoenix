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

#ifndef CONTAINER_H_
#define CONTAINER_H_

#include <tr1/unordered_map>
#include <list>
#include <map>

// storage for flexible cardinality keys
template<typename K, typename V, class Hash=std::tr1::hash<K>, 
    template<class> class Allocator = std::allocator>
class hash_table
{
private:
    typedef std::pair<K, V> entry;
    std::vector< entry, Allocator<entry> > table;
    std::vector< bool, Allocator<bool> > occupied;
    Hash kh;
    uint64_t size;
    uint64_t load;
public:
    hash_table()
    {
        size = 0;
        load = 0;
        rehash(256);
    }
    
    ~hash_table()
    {
    }
    
    void rehash(uint64_t newsize) {
        std::vector<entry, Allocator<entry> > newtable(newsize);
        std::vector<bool, Allocator<bool> > newoccupied(newsize, false);
        for(uint64_t i = 0; i < size; i++) {
            if(occupied[i]) {
                uint64_t index = kh(table[i].first) & (newsize-1);
                while(newoccupied[index])
                    index = (index+1) & (newsize-1);
                newtable[index] = table[i];
                newoccupied[index] = true;
            }
        }
        newtable.swap(table);
        newoccupied.swap(occupied);
        size = newsize;
    }

    V& operator[] (K const& key) 
    {
        uint64_t index = kh(key) & (size-1);
        while(occupied[index] && !(table[index].first == key)) {
            index = (index+1) & (size-1);
        }

        if(occupied[index])
            return table[index].second;
        else {
            load++;
            if(load >= size>>1) {
                rehash(size<<1);
                index = kh(key) & (size-1);
                while(occupied[index] && !(table[index].first == key)) {
                    index = (index+1) & (size-1);
                }
            }
            table[index].first = key;
            table[index].second = V();
            occupied[index] = true;
            return table[index].second;
        }
    }

    class const_iterator {
        hash_table const* a;
        uint64_t index;
    public:
        const_iterator(hash_table const& a, int index)
        {
            this->a = &a;
            this->index = index;
            
            while(this->index < this->a->size && 
                !this->a->occupied[this->index]) {
                this->index++;
            }
        }
        bool operator !=(const_iterator const& other) const {
            return index != other.index;
        }
        const_iterator& operator++() {
            if(index < a->size) {
                index++;
                while(index < a->size && !a->occupied[index]) {
                    index++;
                }
            }
            return *this;
        }
        entry const& operator*() {
            return a->table[index];
        }
    };

    const_iterator begin() const {
        return const_iterator(*this, 0);
    }

    const_iterator end() const {
        return const_iterator(*this, size); 
    }
};

template<typename K, typename V, 
    template<typename, template<class> class> class Combiner, 
    class Hash = std::tr1::hash<K>, 
    template<class> class Allocator = std::allocator>
class hash_container
{
public:
    typedef K key_type;
    typedef V value_type;
    typedef std::pair<const K, Combiner<V, Allocator> > constKCV;
    typedef std::pair<K, Combiner<V, Allocator> > KCV;
private:
    std::vector< KCV, Allocator<KCV> >* vals; 
    uint64_t in_size, out_size;
public:

    typedef hash_table<K, Combiner<V, Allocator>, Hash, Allocator > input_type;
    typedef typename Combiner<V, Allocator>::combined output_type;

    hash_container() : vals(NULL), in_size(0), out_size(0) {}

    void init(uint64_t in_size, uint64_t out_size)
    {
        this->in_size = in_size;
        this->out_size = out_size;
        vals = new std::vector< KCV, Allocator<KCV> >[in_size * out_size];
    }
 
    virtual ~hash_container() 
    {
        delete [] vals;
    }
    
    input_type get(uint64_t in_index)
    {
        input_type i;
        return i;
    }

    void add(uint64_t in_index, input_type const& j)
    {
        Hash kh;
        for(typename input_type::const_iterator i = j.begin(); i != j.end(); ++i)
        {
            if(!(*i).second.empty())
                vals[(kh((*i).first)%out_size)*in_size + in_index].push_back(*i);
        }
    }

    class iterator
    {
    private:
        hash_container<K, V, Combiner, Hash, Allocator> const* ac;
        uint64_t index;
        std::tr1::unordered_map<K, output_type, Hash, std::equal_to<K>, 
            Allocator<std::pair<const K, output_type> > > combined;
        typename std::tr1::unordered_map<K, output_type, Hash >::const_iterator i;
    public:
        iterator(hash_container const* ac, uint64_t index) : ac(ac), index(index) 
        {
            // hash merge 
            for(uint64_t i = 0; i < ac->in_size; i++)
            {
                std::vector< KCV, Allocator<KCV> > const& iv = 
                    ac->vals[index*ac->in_size + i];
                for(size_t j = 0; j < iv.size(); j++)
                {
                    std::pair<K, Combiner<V, Allocator> > const& val = iv[j];
                    combined[val.first].add(&val.second);
                }
            }
            this->i = combined.begin();
        }
       
        bool next(K& key, output_type& values)
        {
            if(i == combined.end())
                return false;
            key = (K)i->first;
            values = i->second;
            ++i;
            return true;
        }
    };

    iterator begin(uint64_t out_index)
    {
        return iterator(this, out_index);
    }
};

// Storage for fixed cardinality keys
template<typename K, typename V, 
	template<typename, template<class> class> class Combiner, int N, 
	template<class> class Allocator = std::allocator>
class array_container
{
private:
    Combiner<V, Allocator>* vals;
    uint64_t in_size, out_size;
public:

    typedef K key_type;
    typedef V value_type;
    
    typedef Combiner<V, Allocator>* input_type;
    typedef typename Combiner<V, Allocator>::combined output_type;

    array_container() : vals(NULL), in_size(0), out_size(0) {}

    void init(uint64_t in_size, uint64_t out_size)
    {
        this->in_size = in_size;
        this->out_size = out_size;
        vals = new Combiner<V, Allocator>[this->in_size * N];
    }
 
    virtual ~array_container() 
    {
        delete [] vals;
    }


    void add(uint64_t in_index, input_type const& j)
    {
        for(uint64_t i = 0; i < N; ++i)
        {
	    vals[i*in_size + in_index] = j[i];
        }
        delete [] j;
    }

    input_type get(uint64_t in_index)
    {
	//switch to use allocator here...
        input_type r = new Combiner<V, Allocator>[N];
        for(uint64_t i = 0; i < N; ++i)
        {
	    r[i] = Combiner<V, Allocator>();
        }
        return r;
    }

    class iterator
    {
    private:
        array_container<K, V, Combiner, N, Allocator> const* ac;
        uint64_t index, i;
    public:
        iterator(array_container const* ac, uint64_t index) : 
            ac(ac), index(index), i(index) {}
       
        bool next(K& key, output_type& values)
        {
            if(i >= N)
                return false;
            key = (K)i;
            values.clear();
            for(size_t j = 0; j < ac->in_size; j++)
            {
                if(!ac->vals[i*ac->in_size+j].empty())
                    values.add(&ac->vals[i*ac->in_size+j]);
            }
            i += ac->in_size;	// XXX: in_size == num reduce threads
            return true;
        }
    };

    iterator begin(uint64_t out_index)
    {
        return iterator(this, out_index);
    }
};

// Unlocked storage, everyone writes to the same array. 
// Assumes that only a single task needs to write to an entry.
template<typename K, typename V, 
    template<typename, template<class> class> class Combiner, int N, 
    template<class> class Allocator = std::allocator>
class common_array_container
{
private:
    Combiner<V, Allocator>* vals;
    uint64_t in_size, out_size;
public:

    typedef K key_type;
    typedef V value_type;
    
    typedef Combiner<V, Allocator>* input_type;
    typedef typename Combiner<V, Allocator>::combined output_type;

    common_array_container() : vals(NULL), in_size(0), out_size(0) {}

    void init(uint64_t in_size, uint64_t out_size)
    {
        this->in_size = in_size;
        this->out_size = out_size;
        vals = new Combiner<V, Allocator>[N];
        for(uint64_t i = 0; i < N; ++i)
        {
	    vals[i] = Combiner<V, Allocator>();
        }
    }
 
    virtual ~common_array_container() 
    {
        delete [] vals;
    }

    void add(uint64_t in_index, input_type const& j)
    {
        // no need to copy anything...
    }

    input_type get(uint64_t in_index)
    {
        return vals;
    }

    class iterator
    {
    private:
        common_array_container<K, V, Combiner, N, Allocator> const* ac;
        uint64_t index, i;
    public:
        iterator(common_array_container const* ac, uint64_t index) : 
            ac(ac), index(index), i(index) {}
       
        bool next(K& key, output_type& values)
        {
            if(i >= N)
                return false;
            key = (K)i;
            values.clear();
            values.add(&ac->vals[i]);
            i += ac->in_size;
            return true;
        }
    };

    iterator begin(uint64_t out_index)
    {
        return iterator(this, out_index);
    }
};

// Fixed width hash table from Phoenix 2
template<typename K, typename V, 
    template<typename, template<class> class> class Combiner, int N, 
    class Hash = std::tr1::hash<K>,
    template<class> class Allocator = std::allocator>
class fixed_hash_container
{
private:
    // Each hash bucket is a linked list of <K, Combiner<V> >
    typedef std::list< std::pair<K, Combiner<V, Allocator> >, 
        Allocator< std::pair<K, Combiner<V, Allocator> > > > hash_bucket;

    class hash_table
    {
    private:
        hash_bucket** buckets;
    public:    

        hash_table() 
        {
            buckets = new hash_bucket*[N];
            for(size_t i = 0; i < N; ++i)
            {
                // NOTE: These buckets will be freed during the reduce phase
                buckets[i] = new hash_bucket();
            }
        }

        ~hash_table()
        {
            delete [] buckets;
        }

        Combiner<V, Allocator>& operator[] (K const& key) 
        {
            Hash kh;
            hash_bucket* bucket = buckets[kh(key) % N];
            typename hash_bucket::iterator i;

            for(i = bucket->begin(); i != bucket->end(); ++i)
            {
                if(i->first == key) break;
            }

            if(i != bucket->end()) {
                return i->second;
            } else {
                bucket->push_back(std::pair<K, Combiner<V, Allocator> >(
                    key, Combiner<V, Allocator>()));
                return bucket->back().second;
            }
        }

        friend class fixed_hash_container;
    };
    
    hash_table* hash_tables;
    uint64_t in_size, out_size;
public:    

    typedef K key_type;
    typedef V value_type;

    typedef hash_table input_type;
    typedef typename Combiner<V, Allocator>::combined output_type;

    fixed_hash_container() : hash_tables(NULL), in_size(0), out_size(0) {}

    void init(uint64_t in_size, uint64_t out_size)
    {
        this->in_size = in_size;
        this->out_size = out_size;
        hash_tables = new hash_table[in_size];
    }
 
    virtual ~fixed_hash_container() 
    {
        delete [] hash_tables;
    }

    void add(uint64_t in_index, input_type const& j)
    {
        hash_table& table = hash_tables[in_index];

        for(int i = 0; i < N; ++i) {
            table.buckets[i] = j.buckets[i];
        }
    }
    
    input_type get(uint64_t in_index)
    {
        input_type i;
        return i;
    }
    
    class iterator
    {
    private:
        fixed_hash_container<K, V, Combiner, N, Hash, Allocator> const* fc;
        uint64_t begin_idx;
        uint64_t end_idx;
        std::tr1::unordered_map<K, output_type, Hash> combined;
        typename std::tr1::unordered_map<K, output_type, Hash>::const_iterator i;
    public:
        iterator(fixed_hash_container const* fc, uint64_t index) : fc(fc)
        {
            if(index < N)
            {
                // Calculate bucket range
                size_t buckets_per_task = N / fc->out_size;
                if(buckets_per_task == 0) 
                    buckets_per_task = 1;
                size_t remainder = N % fc->out_size;
                begin_idx = buckets_per_task * index;
                if(index < remainder) {
                    begin_idx += index;
                } else {
                    begin_idx += remainder;
                }
                end_idx = begin_idx + buckets_per_task;
                if(index < remainder) {
                    end_idx += 1;
                }
                
                // hash merge 
                for(size_t bucket_idx = begin_idx; bucket_idx < end_idx; 
                    bucket_idx++)
                {
                    for(uint64_t i = 0; i < fc->in_size; i++)
                    {
                        input_type& table = fc->hash_tables[i];
                        hash_bucket* bucket = table.buckets[bucket_idx];
                        typename hash_bucket::iterator j;

                        for(j = bucket->begin(); j != bucket->end(); j++)
                        {
                            combined[j->first].add(&j->second);
                        }
                        
                    }
                }
            }
            this->i = combined.begin();
        }
       
        bool next(K& key, output_type& values)
        {
            if(i == combined.end())
                return false;
            key = (K)i->first;
            values = i->second;
            ++i;
            return true;
        }

        ~iterator()
        {
            // Free up those buckets
            for(size_t bucket_idx = begin_idx; bucket_idx < end_idx; bucket_idx++)
            {
                for(uint64_t i = 0; i < fc->in_size; i++)
                {
                    input_type& table = fc->hash_tables[i];
                    hash_bucket* bucket = table.buckets[bucket_idx];
                    delete bucket;
                }
            }
        }
    };

    iterator begin(uint64_t out_index)
    {
        return iterator(this, out_index);
    }
};

#endif /* CONTAINER_H_ */

// vim: ts=8 sw=4 sts=4 smarttab smartindent
