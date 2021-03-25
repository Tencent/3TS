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

#ifndef _MEM_ALLOC_H_
#define _MEM_ALLOC_H_

#include "global.h"
#include <memory>

class mem_alloc {
public:
    void * alloc(uint64_t size);
    void * align_alloc(uint64_t size);
    void * realloc(void * ptr, uint64_t size);
    void free(void * block, uint64_t size);
    void free(void * block);
    template <typename T, typename ...Args> T* construct(Args&&... args) {
        void* ptr = alloc(sizeof(T));
        return new(ptr) T(std::forward<Args>(args)...);
    }
    template <typename T, typename ...Args> std::shared_ptr<T> make_shared(Args&&... args) {
        return std::shared_ptr<T>(construct<T>(std::forward<Args>(args)...),
                [this](T* const ptr) { free(ptr, sizeof(T)); });
    }
};

#endif
