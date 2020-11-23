#pragma once
#include <random>

#include "generic.h"


class ActionSequenceCreator {
 public:
  ActionSequenceCreator() {}
  ~ActionSequenceCreator() {}
  virtual void DeliverActionSequences(const std::function<void(ActionSequence &&,uint64_t)> &handle) const = 0;
};

class InputActionSequenceCreator : public ActionSequenceCreator {
 public:
  InputActionSequenceCreator(const std::string &path);
  ~InputActionSequenceCreator();
  virtual void DeliverActionSequences(
      const std::function<void(ActionSequence &&,uint64_t)> &handle) const;

 private:
  const std::string path_;
};




class TraversalActionSequenceCreator : public ActionSequenceCreator {
 public:
  TraversalActionSequenceCreator(const Options &opt);

  void DeliverActionSequences(const std::function<void(ActionSequence &&,uint64_t)> &handle) const;
  static uint64_t cut_down_;


 private:
  void HandleActionSeq(const std::function<void(ActionSequence &&,uint64_t)> &handle,uint64_t seq_id,
                       const ActionSequence &act_seq) const ;
  void HandleDTLActionSeq(const std::function<void(ActionSequence &&,uint64_t)> &handle,uint64_t seq_id,
                          const ActionSequence &dml_act_seq,
                          const ActionSequence &dtl_act_seq) const ;
  void HandleDMLActionSeq(const std::function<void(ActionSequence &&,uint64_t)> &handle,uint64_t seq_id,
                          const ActionSequence &dml_act_seq, const uint64_t max_trans_id) const ;

  bool SkipDMLAction(const std::vector<Action> &actions) const ;
  bool OnlyOneTrans(const std::vector<Action> &actions) const ;
  void RecursiveFillDMLActionSeq(
      const std::function<void(const ActionSequence &,uint64_t , const uint64_t)> &handle,
      std::vector<Action> &actions, uint64_t max_trans_id, uint64_t max_item_id) const ;
  /*****************************************************************************
   * Just like backtrace structure, condition-if includes the end-condition. And condition-else
   *does the loop works. This function firstly build the Full Permutation of DTL. And then merge
   *it with DML. At last handle() it. args: dtl_act_types: set of DTL
   *
   *
   ****************************************************************************/
  void RecursiveFillDTLActionSeq(const std::function<void(const ActionSequence &)> &handle,
                                 std::vector<Action::Type> &dtl_act_types,
                                 const uint64_t max_trans_id, uint64_t abort_trans_num) const ;

  void RecursiveMoveForwardDTLAction(const std::function<void(const ActionSequence &)> &handle,
                                     ActionSequence &act_seq, const size_t pos) const ;

  const uint64_t trans_num_;
  const uint64_t item_num_;
  const uint64_t dml_act_num_;
  const uint64_t subtask_num_;
  mutable uint64_t dfs_cnt_;
  const bool with_abort_;
  const bool tail_dtl_;
  const bool save_history_with_empty_opt_;
  const bool dynamic_seq_len_;
  mutable uint64_t seq_id_;

};
