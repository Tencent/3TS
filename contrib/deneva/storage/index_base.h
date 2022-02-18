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

#ifndef _INDEX_BASE_H_
#define _INDEX_BASE_H_

#include "global.h"

class table_t;

class index_base {
public:
    virtual RC init() {
        return RCOK;
    };

    virtual RC init(uint64_t size) {
        return RCOK;
    };

    virtual bool  index_exist(idx_key_t key)=0; // check if the key exist.

    virtual RC index_insert(idx_key_t key, itemid_t *item, int part_id = -1) = 0;

    virtual RC index_insert_nonunique(idx_key_t key, itemid_t *item, int part_id = -1) = 0;

    virtual int index_read(idx_key_t key, itemid_t *&item, int part_id = -1) = 0;

    virtual int index_read(idx_key_t key, itemid_t *&item, int part_id = -1, int thd_id = 0) = 0;

        // TODO implement index_remove
    virtual RC index_remove(idx_key_t key) {
        return RCOK;
    };

    // the index in on "table". The key is the merged key of "fields"
    table_t *             table;
};

#endif
