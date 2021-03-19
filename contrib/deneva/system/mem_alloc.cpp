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

#include "mem_alloc.h"

#include "global.h"
#include "helper.h"
//#include "jemalloc/jemalloc.h"

#define N_MALLOC

void mem_alloc::free(void * ptr, uint64_t size) {
    if (NO_FREE) {
    }
    DEBUG_M("free %ld 0x%lx\n",size,(uint64_t)ptr);
#ifdef N_MALLOC
    std::free(ptr);
#else
    je_free(ptr);
#endif
}

void mem_alloc::free(void * ptr) {
    if (NO_FREE) {
    }
    DEBUG_M("free 0x%lx\n", (uint64_t)ptr);
#ifdef N_MALLOC
    std::free(ptr);
#else
    je_free(ptr);
#endif
}

void * mem_alloc::alloc(uint64_t size) {
    void * ptr;

#ifdef N_MALLOC
    ptr = malloc(size);
#else
    ptr = je_malloc(size);
#endif
    DEBUG_M("alloc %ld 0x%lx\n",size,(uint64_t)ptr);
    assert(ptr != NULL);
    return ptr;
}

void * mem_alloc::align_alloc(uint64_t size) {
    uint64_t aligned_size = size + CL_SIZE - (size % CL_SIZE);
    return alloc(aligned_size);
}


void * mem_alloc::realloc(void * ptr, uint64_t size) {
#ifdef N_MALLOC
    void * _ptr = std::realloc(ptr,size);
#else
    void * _ptr = je_realloc(ptr,size);
#endif
    DEBUG_M("realloc %ld 0x%lx\n",size,(uint64_t)_ptr);
    return _ptr;
}


