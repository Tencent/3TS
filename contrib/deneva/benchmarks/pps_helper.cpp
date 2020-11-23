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

#include "pps_helper.h"

uint64_t parts_to_partition(uint64_t part_key) { return (part_key) % g_part_cnt; }

uint64_t products_to_partition(uint64_t product_key) { return (product_key) % g_part_cnt; }

uint64_t suppliers_to_partition(uint64_t supplier_key) { return (supplier_key) % g_part_cnt; }

/*
uint64_t URand(uint64_t x, uint64_t y) {
    return x + RAND(y - x + 1);
}
uint64_t RAND(uint64_t max) {
    return rand() % max;
}
*/
