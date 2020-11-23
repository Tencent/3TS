
#include "query.h"
#include "da_query.h"
#include "da.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "message.h"


void DAQuery::init(uint64_t thd_id, Workload * h_wl){
  BaseQuery::init();
}

void DAQuery::init() {
  BaseQuery::init();
}


void DAQuery::release() {
  BaseQuery::release();
}

void DAQuery::print()
{
    std::cout<<
    "trans_id:"<<trans_id<<" "
        "item_id"<<item_id<<" "
        "seq_id"<<seq_id<<" "
        "state"<<state<<" "
        "next_state"<<next_state<<" "
        "last_state"<<last_state<<std::endl;
}

bool DAQuery::readonly()
{
    return false;
}

std::set<uint64_t> DAQuery::participants(Message * msg, Workload * wl) {
  std::set<uint64_t> participant_set;
  return participant_set;
}

uint64_t DAQueryGenerator::action_2_state(ActionSequence& act_seq,size_t i, uint64_t seq_id)
{

  //turn to 64bit in order to not overflow
  uint64_t ret=0, trans_id2=0, item_id2=0, type2=0, seq_id2=0, number2=0;
  if(i<0||i>=act_seq.actions().size())
    return ret;
  trans_id2=act_seq.actions()[i].trans_id();
  if(act_seq.actions()[i].IsPointDML())
    item_id2=act_seq.actions()[i].item_id();
  type2=static_cast<int>(act_seq.actions()[i].type());
  seq_id2=seq_id;
  number2=i;


  ret = ret |  trans_id2;
  ret = ret | (item_id2 <<  8 );
  ret = ret | (type2    <<  16);
  ret = ret | (seq_id2  <<  24);
  ret = ret | (number2  <<  32);
  return ret;
}
DATxnType type_trans(char type)
{
    if(type=='R')
      return DA_READ;
    else if(type=='W')
      return DA_WRITE;
    else if(type=='C')
      return DA_COMMIT;
    else if(type=='A')
      return DA_ABORT;
    else
      return DA_SCAN;
}
uint64_t DAQueryGenerator::seq_num=0;
BaseQuery * DAQueryGenerator::create_query(Workload * h_wl, uint64_t home_partition_id)
{
  #if CREATOR_USE_T
    struct Options opt;
    opt.trans_num=TRANS_CNT;
    opt.item_num=ITEM_CNT;
    opt.subtask_num=SUBTASK_NUM;
    opt.subtask_id=SUBTASK_ID;
    if(opt.subtask_num>1)
      opt.subtask_id=home_partition_id;
    opt.max_dml=MAX_DML;
    opt.with_abort=WITH_ABORT;
    opt.tail_dtl=TAIL_DTL;
    opt.save_history_with_empty_opt=SAVE_HISTROY_WITH_EMPTY_OPT;
    opt.dynamic_seq_len=DYNAMIC_SEQ_LEN;
    TraversalActionSequenceCreator creator(opt);
  #else
    InputActionSequenceCreator creator(string(INPUT_FILE_PATH));
  #endif
  BaseQuery * ret=NULL;
  const auto handle = [this](ActionSequence &&act_seq,uint64_t seq_id) {
    size_t seq_size=act_seq.actions().size();
    int* t_version=(int*)malloc(act_seq.item_num()*sizeof(int));
    memset(t_version,0,act_seq.item_num()*sizeof(int));
    //bool pu=false;
    for(size_t i=0;i<seq_size;i++)
    {
        DAQuery * DAQ_t=new DAQuery();
        DAQ_t->trans_id=act_seq.actions()[i].trans_id();
        DAQ_t->item_id=act_seq.actions()[i].item_id();
        DAQ_t->seq_id=seq_num;//*opt.subtask_num+opt.subtask_id;
        DAQ_t->state=action_2_state(act_seq,i,DAQ_t->seq_id);
        DAQ_t->next_state=action_2_state(act_seq,i+1,DAQ_t->seq_id);
        DAQ_t->last_state=action_2_state(act_seq,i-1,DAQ_t->seq_id);
        DAQ_t->txn_type=type_trans(static_cast<char>(act_seq.actions()[i].type()));
        if(act_seq.actions()[i].type()==Action::Type::WRITE)
          t_version[DAQ_t->item_id]++;
        DAQ_t->write_version=t_version[DAQ_t->item_id];
        da_gen_qry_queue.push_data(DAQ_t);
        /*
        while(!(pu=da_query_queue.push(DAQ_t)));
        if(pu)
          printf("true ");
        else
          printf("false ");
        fflush(stdout);
        */
    }
    free(t_version);
    seq_num++;
    printf("product: %lu\n",seq_num);
    fflush(stdout);
  };

  creator.DeliverActionSequences(handle);
  printf("history thread exit\n");
  fflush(stdout);
  return ret;
}
