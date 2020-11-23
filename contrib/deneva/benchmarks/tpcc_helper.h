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

#ifndef _TPCC_HELPER_H_
#define _TPCC_HELPER_H_

#include "global.h"
#include "helper.h"

uint64_t distKey(uint64_t d_id, uint64_t d_w_id);
uint64_t custKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id);
uint64_t orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
uint64_t orderPrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
// non-primary key
uint64_t custNPKey(char * c_last, uint64_t c_d_id, uint64_t c_w_id);
uint64_t stockKey(uint64_t s_i_id, uint64_t s_w_id);

uint64_t w_from_distKey(uint64_t d_key);
uint64_t w_from_custKey(uint64_t c_key);
uint64_t w_from_orderlineKey(uint64_t s_key);
uint64_t w_from_orderPrimaryKey(uint64_t s_key);
uint64_t w_from_custNPKey(uint64_t cnp_key);
uint64_t w_from_stockKey(uint64_t s_key);
//uint64_t orderKey(uint64_t o_id, uint64_t o_d_id, uint64_t o_w_id);
// the max of ol_number is 15. That's why there is a 15 here
//uint64_t olKey(uint64_t ol_o_id, uint64_t ol_d_id,
//    uint64_t ol_w_id, uint64_t ol_number);

uint64_t Lastname(uint64_t num, char* name);

// return random data from [0, max-1]
uint64_t RAND(uint64_t max);
// random number from [x, y]
uint64_t URand(uint64_t x, uint64_t y);
// non-uniform random number
uint64_t NURand(uint64_t A, uint64_t x, uint64_t y);
// random string with random length beteen min and max.
uint64_t MakeAlphaString(int min, int max, char * str);
uint64_t MakeNumberString(int min, int max, char* str);

uint64_t wh_to_part(uint64_t wid);

#endif
