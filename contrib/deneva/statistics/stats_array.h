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

#ifndef _STATS_ARR_H_
#define _STATS_ARR_H_

enum StatsArrType {
    ArrInsert,
    ArrIncr
};
class StatsArr {
public:
    void init(uint64_t size,StatsArrType type);
    void clear();
    void quicksort(int low_idx, int high_idx);
    void resize();
    void insert(uint64_t item);
    void append(StatsArr arr);
    void print(FILE * f);
    void print(FILE * f,uint64_t min, uint64_t max);
    uint64_t get_idx(uint64_t idx);
    uint64_t get_percentile(uint64_t ile);
    uint64_t get_avg();

    uint64_t * arr;
    uint64_t size;
    uint64_t cnt;
    StatsArrType type;
private:
    pthread_mutex_t mtx;
};

#endif
