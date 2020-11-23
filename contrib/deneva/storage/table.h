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

#ifndef _TABLE_H_
#define _TABLE_H_

#include "global.h"

// TODO sequential scan is not supported yet.
// only index access is supported for table.

class Catalog;
class row_t;

class table_t
{
public:
    void init(Catalog * schema);
    // row lookup should be done with index. But index does not have
    // records for new rows. get_new_row returns the pointer to a
    // new row.
    RC get_new_row(row_t *& row); // this is equivalent to insert()
    RC get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id);

    void delete_row(); // TODO delete_row is not supportet yet

    uint64_t get_table_size() { return *cur_tab_size; };
    Catalog * get_schema() { return schema; };
    const char * get_table_name() { return table_name; };
    uint32_t get_table_id() { return table_id; };

    Catalog *         schema;
private:
    const char *     table_name;
    uint32_t table_id;
    uint64_t *         cur_tab_size;
    char             pad[CL_SIZE - sizeof(void *)*3 - sizeof(uint32_t)];
};

#endif
