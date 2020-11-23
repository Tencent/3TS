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

#include "global.h"
#include "helper.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "mem_alloc.h"

void table_t::init(Catalog * schema) {
    this->table_name = schema->table_name;
    this->table_id = schema->table_id;
    this->schema = schema;
    cur_tab_size = new uint64_t;
    // isolate cur_tab_size with other parameters.
    // Because cur_tab_size is frequently updated, causing false
    // sharing problems
    char * ptr = new char[CL_SIZE*2 + sizeof(uint64_t)];
    cur_tab_size = (uint64_t *) &ptr[CL_SIZE];
}

RC table_t::get_new_row(row_t *& row) {
    // this function is obsolete.
    assert(false);
    return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id) {
    RC rc = RCOK;
    DEBUG_M("table_t::get_new_row alloc\n");
    void * ptr = mem_allocator.alloc(sizeof(row_t));
    assert (ptr != NULL);

    row = (row_t *) ptr;
    rc = row->init(this, part_id, row_id);
    row->init_manager(row);

    return rc;
}
