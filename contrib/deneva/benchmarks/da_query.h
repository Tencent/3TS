#ifndef _DAQuery_H_
#define _DAQuery_H_

#include "global.h"
#include "helper.h"
#include "query.h"
#include "da.h"
//#include "creator.h"

class Workload;
class Message;

class DAQuery : public BaseQuery {
public:
    DAQuery()
    {
        trans_id=0;
        item_id=0;
        seq_id=0;
        state=0;
        next_state=0;
        last_state=0;
    }
      void init();
    void init(uint64_t thd_id, Workload * h_wl);
      void release();
      void print();
      bool readonly();

    static std::set<uint64_t> participants(Message * msg, Workload * wl);
    DATxnType txn_type;
    uint64_t trans_id;
    uint64_t item_id;
    uint64_t seq_id;
    uint64_t write_version;
    uint64_t state;
    uint64_t next_state;
    uint64_t last_state;


};

class DAQueryGenerator : public QueryGenerator {
public:
  BaseQuery * create_query(Workload * h_wl, uint64_t home_partition_id);
  static uint64_t seq_num;
private:
  uint64_t action_2_state(ActionSequence& act_seq,size_t i, uint64_t seq_id);
};
#endif
