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

#ifndef _CATALOG_H_
#define _CATALOG_H_

#include <map>
#include <vector>
#include "global.h"
#include "helper.h"

class Column {
public:
    Column() {
        this->type = new char[80];
        this->name = new char[80];
    }
      Column(uint64_t size, char *type, char *name, uint64_t id, uint64_t index) {
        this->size = size;
        this->id = id;
        this->index = index;
        this->type = new char[80];
        this->name = new char[80];
        strcpy(this->type, type);
        strcpy(this->name, name);
    };

    UInt64 id;
    UInt32 size;
    UInt32 index;
    char * type;
    char * name;
    char pad[CL_SIZE - sizeof(uint64_t)*3 - sizeof(char *)*2];
};

class Catalog {
public:
    // abandoned init function
    // field_size is the size of each each field.
    void init(const char * table_name, uint32_t table_id, int field_cnt);
    void add_col(char * col_name, uint64_t size, char * type);

    UInt32             field_cnt;
     const char *     table_name;
     uint32_t       table_id;

    UInt32 get_tuple_size() {
        return tuple_size;
    };

    uint64_t get_field_cnt() {
        return field_cnt;
    };
    uint64_t get_field_size(int id) {
        return _columns[id].size;
    };
    uint64_t get_field_index(int id) {
        return _columns[id].index;
    };
    char *             get_field_type(uint64_t id);
    char *             get_field_name(uint64_t id);
    uint64_t         get_field_id(const char * name);
    char *             get_field_type(char * name);
    uint64_t         get_field_index(char * name);

    void             print_schema();
    Column *         _columns;
    UInt32             tuple_size;
private:
    char pad[CL_SIZE - sizeof(uint64_t)*2 - sizeof(int) - sizeof(char *)*2 - sizeof(uint32_t)];
};

#endif
