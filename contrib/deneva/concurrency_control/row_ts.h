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

#ifndef ROW_TS_H
#define ROW_TS_H

class table_t;
class Catalog;
class TxnManager;
struct TsReqEntry {
    TxnManager * txn;
    // for write requests, need to have a copy of the data to write.
    row_t * row;
    itemid_t * item;
    ts_t ts;
    uint64_t starttime;
    TsReqEntry * next;
};

class Row_ts {
public:
    void init(row_t * row);
    RC access(TxnManager * txn, TsType type, row_t * row);

private:
     pthread_mutex_t * latch;
    bool blatch;

    void buffer_req(TsType type, TxnManager * txn, row_t * row);
    TsReqEntry * debuffer_req(TsType type, TxnManager * txn);
    TsReqEntry * debuffer_req(TsType type, ts_t ts);
    TsReqEntry * debuffer_req(TsType type, TxnManager * txn, ts_t ts);
    void update_buffer(uint64_t thd_id);
    ts_t cal_min(TsType type);
    TsReqEntry * get_req_entry();
    void return_req_entry(TsReqEntry * entry);
    void return_req_list(TsReqEntry * list);

    row_t * _row;
    ts_t wts;
    ts_t rts;
    ts_t min_wts;
    ts_t min_rts;
    ts_t min_pts;

    TsReqEntry * readreq;
    TsReqEntry * writereq;
    TsReqEntry * prereq;
    uint64_t preq_len;
};

#endif
