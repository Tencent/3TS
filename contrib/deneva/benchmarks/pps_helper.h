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

#ifndef _PPS_HELPER_H_
#define _PPS_HELPER_H_

#include "global.h"
#include "helper.h"
#include "tpcc_helper.h"

enum {
  FIELD1=1,
  FIELD2,
  FIELD3,
  FIELD4,
  FIELD5,
  FIELD6,
  FIELD7,
  FIELD8,
  FIELD9,
  FIELD10,
  PART_AMOUNT,
};


uint64_t parts_to_partition(uint64_t part_key);
uint64_t products_to_partition(uint64_t product_key);
uint64_t suppliers_to_partition(uint64_t supplier_key);

/*
// return random data from [0, max-1]
uint64_t RAND(uint64_t max);
// random number from [x, y]
uint64_t URand(uint64_t x, uint64_t y);
*/

#endif
