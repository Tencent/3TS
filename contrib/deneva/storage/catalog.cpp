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

#include "catalog.h"
#include "global.h"
#include "helper.h"

void Catalog::init(const char* table_name, uint32_t table_id, int field_cnt) {
    this->table_name = table_name;
    this->table_id = table_id;
    this->field_cnt = 0;
    this->_columns = new Column [field_cnt];
    this->tuple_size = 0;
}

void Catalog::add_col(char * col_name, uint64_t size, char * type) {
    _columns[field_cnt].size = size;
    strcpy(_columns[field_cnt].type, type);
    strcpy(_columns[field_cnt].name, col_name);
    _columns[field_cnt].id = field_cnt;
    _columns[field_cnt].index = tuple_size;
    tuple_size += size;
    field_cnt ++;
}

uint64_t Catalog::get_field_id(const char * name) {
    UInt32 i;
    for (i = 0; i < field_cnt; i++) {
    if (strcmp(name, _columns[i].name) == 0) break;
    }
    assert (i < field_cnt);
    return i;
}

char* Catalog::get_field_type(uint64_t id) { return _columns[id].type; }

char* Catalog::get_field_name(uint64_t id) { return _columns[id].name; }

char* Catalog::get_field_type(char* name) { return get_field_type(get_field_id(name)); }

uint64_t Catalog::get_field_index(char* name) { return get_field_index(get_field_id(name)); }

void Catalog::print_schema() {
    printf("\n[Catalog] %s\n", table_name);
    for (UInt32 i = 0; i < field_cnt; i++) {
        printf("\t%s\t%s\t%ld\n", get_field_name(i), get_field_type(i), get_field_size(i));
    }
}
