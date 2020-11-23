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

#ifndef ROW_MVCC_H
#define ROW_MVCC_H

class table_t;
class Catalog;
class TxnManager;

struct MVReqEntry {
    TxnManager * txn;
    ts_t ts;
    ts_t starttime;
    MVReqEntry * next;
};

struct MVHisEntry {
    ts_t ts;
    // only for write history. The value needs to be stored.
//    char * data;
    row_t * row;
    MVHisEntry * next;
    MVHisEntry * prev;
};



class Row_mvcc {
public:
    void init(row_t * row);
    RC access(TxnManager * txn, TsType type, row_t * row);
private:
     pthread_mutex_t * latch;
    bool blatch;

    row_t * _row;
    MVReqEntry * get_req_entry();
    void return_req_entry(MVReqEntry * entry);
    MVHisEntry * get_his_entry();
    void return_his_entry(MVHisEntry * entry);

    bool conflict(TsType type, ts_t ts);
    void buffer_req(TsType type, TxnManager * txn);
    MVReqEntry * debuffer_req( TsType type, TxnManager * txn = NULL);
    void update_buffer(TxnManager * txn);
    void insert_history( ts_t ts, row_t * row);

    row_t * clear_history(TsType type, ts_t ts);

    MVReqEntry * readreq_mvcc;
    MVReqEntry * prereq_mvcc;
    MVHisEntry * readhis;
    MVHisEntry * writehis;
    MVHisEntry * readhistail;
    MVHisEntry * writehistail;
    uint64_t whis_len;
    uint64_t rhis_len;
    uint64_t rreq_len;
    uint64_t preq_len;
};

#endif
