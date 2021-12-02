/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _ARR_H_
#define _ARR_H_

#include "global.h"
#include "helper.h"
#include "mem_alloc.h"

template <class T>
class Array {
public:
    Array() : items(NULL), capacity(0), count(0) {}
    void init(uint64_t size) {
        DEBUG_M("Array::init %ld*%ld\n",sizeof(T),size);
        items = (T*) mem_allocator.alloc(sizeof(T)*size);
        capacity = size;
        assert(items);
        assert(capacity == size);
        count = 0;
    }

    void clear() { count = 0; }

    void copy(Array a) {
        init(a.size());
        for(uint64_t i = 0; i < a.size(); i++) {
        add(a[i]);
        }
        assert(size() == a.size());
    }

    void append(Array a) {
        assert(count + a.size() <= capacity);
        for(uint64_t i = 0; i < a.size(); i++) {
        add(a[i]);
        }
    }


    void release() {
        DEBUG_M("Array::release %ld*%ld\n",sizeof(T),capacity);
        mem_allocator.free(items,sizeof(T)*capacity);
        items = NULL;
        count = 0;
        capacity = 0;
    }

    void add_unique(T item){
        for(uint64_t i = 0; i < count; i++) {
        if (items[i] == item) return;
        }
        add(item);
    }

    void add(T item){
        assert(count < capacity);
        items[count] = item;
        ++count;
    }

    void add() {
        assert(count < capacity);
        ++count;
    }

    T get(uint64_t idx) {
        assert(idx < count);
        return items[idx];
    }

    void set(uint64_t idx, T item) {
        assert(idx < count);
        items[idx] = item;
    }

    bool contains(T item) {
        for (uint64_t i = 0; i < count; i++) {
            if (items[i] == item) {
                return true;
            }
        }
        return false;
    }

    uint64_t getPosition(T item) {
        for (uint64_t i = 0; i < count; i++) {
            if (items[i] == item) {
                return i;
            }
        }
        return count;
    }

    void swap(uint64_t i, uint64_t j) {
        T tmp = items[i];
        items[i] = items[j];
        items[j] = tmp;
    }
    T operator[](uint64_t idx) {
        assert(idx < count);
        return items[idx];
    }
    uint64_t get_count() {return count;}
    uint64_t size() {return count;}
    bool is_full() { return count == capacity;}
    bool is_empty() { return count == 0;}
    uint64_t get_capacity() {return capacity;}
private:
    T * items;
    uint64_t capacity;
    uint64_t count;

};


#endif
