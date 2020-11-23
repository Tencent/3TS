
#include <random>

#include "creator.h"


  InputActionSequenceCreator::InputActionSequenceCreator(const std::string &path) : path_(path) {}
  InputActionSequenceCreator::~InputActionSequenceCreator() {}
  void InputActionSequenceCreator::DeliverActionSequences(
      const std::function<void(ActionSequence &&,uint64_t)> &handle) const {
    std::ifstream fs(path_);
    if (!fs) {
      std::cerr << "Open Action Sequences File Failed" << std::endl;
      return;
    }
    uint64_t seq_id=0;
    ActionSequence act_seq;
    while (fs >> act_seq) {
      handle(std::move(act_seq),seq_id);
    }
  }




uint64_t TraversalActionSequenceCreator::cut_down_ = 0;
//uint64_t TraversalActionSequenceCreator::seq_id_ = 0;

  TraversalActionSequenceCreator::TraversalActionSequenceCreator(const Options &opt)
      : trans_num_(opt.trans_num),
        item_num_(opt.item_num),
        dml_act_num_(opt.max_dml),
        subtask_num_(opt.subtask_num),
        dfs_cnt_(opt.subtask_num - opt.subtask_id),
        with_abort_(opt.with_abort),
        tail_dtl_(opt.tail_dtl),
        save_history_with_empty_opt_(opt.save_history_with_empty_opt),
        dynamic_seq_len_(opt.dynamic_seq_len),
        seq_id_(0) {}

  void TraversalActionSequenceCreator::DeliverActionSequences(const std::function<void(ActionSequence &&,uint64_t)> &handle) const {
    std::vector<Action> tmp_actions;
    RecursiveFillDMLActionSeq(
        [this, &handle](const ActionSequence &dml_act_seq,uint64_t seq_id, const uint64_t max_trans_id) {
          HandleDMLActionSeq(handle, seq_id, dml_act_seq, max_trans_id);
        },
        tmp_actions, 0, 0);
  }


  void TraversalActionSequenceCreator::HandleActionSeq(const std::function<void(ActionSequence &&,uint64_t)> &handle,uint64_t seq_id,
                       const ActionSequence &act_seq) const {
    ActionSequence act_seq_copy = act_seq;  // copy ActionSequence
    handle(std::move(act_seq_copy),seq_id);
  }
  void TraversalActionSequenceCreator::HandleDTLActionSeq(const std::function<void(ActionSequence &&,uint64_t)> &handle,uint64_t seq_id,
                          const ActionSequence &dml_act_seq,
                          const ActionSequence &dtl_act_seq) const {
    // Firstly, append every dml_act_seq with dtl_act_seq.
    ActionSequence act_seq_tot = dml_act_seq + dtl_act_seq;
    // Then move dtl_act_seq at a suitable location, forward.
    RecursiveMoveForwardDTLAction(
        [this, &handle,seq_id](const ActionSequence &act_seq) { HandleActionSeq(handle, seq_id ,act_seq); },
        act_seq_tot, dml_act_seq.size());
  }
  void TraversalActionSequenceCreator::HandleDMLActionSeq(const std::function<void(ActionSequence &&,uint64_t)> &handle,uint64_t seq_id,
                          const ActionSequence &dml_act_seq, const uint64_t max_trans_id) const {
    std::vector<Action::Type> dtl_act_types;
    RecursiveFillDTLActionSeq(
        [this, &handle, &dml_act_seq,seq_id](const ActionSequence &dtl_act_seq) {
          HandleDTLActionSeq(handle, seq_id,dml_act_seq, dtl_act_seq);
        },
        dtl_act_types, max_trans_id, 0);
  }

  bool TraversalActionSequenceCreator::SkipDMLAction(const std::vector<Action> &actions) const {
    assert(!actions.empty());
    if (actions.size() == 1) {
      return false;
    }
    const Action &last_action = actions[actions.size() - 2];
    const Action &cur_action = actions[actions.size() - 1];
    const bool is_same_trans = (last_action.trans_id() == cur_action.trans_id());
    const bool is_same_item = (last_action.item_id() == cur_action.item_id());
    return
        /* WW WR same item in same transaction is meaningless */
        (is_same_trans && is_same_item &&
         (Action::Type::WRITE == last_action.type() || Action::Type::READ == cur_action.type())) ||
        /* actions for different items in same transaction can be swapped, so we support item_id
           must be ascending */
        (is_same_trans && last_action.item_id() > cur_action.item_id()) ||
        /* read actions in different transactions can be swapped */
        //(Action::Type::READ == last_action.type() && Action::Type::READ == cur_action.type() &&
        // last_action.trans_id() > cur_action.trans_id()) ||
        false;
  }

  bool TraversalActionSequenceCreator::OnlyOneTrans(const std::vector<Action> &actions) const {
    for (uint64_t i = 1; i < actions.size(); ++i) {
      if (actions[i].trans_id() != actions[i - 1].trans_id()) {
        return false;
      }
    }
    return true;
  }

  void TraversalActionSequenceCreator::RecursiveFillDMLActionSeq(
      const std::function<void(const ActionSequence &,uint64_t , const uint64_t)> &handle,
      std::vector<Action> &actions, uint64_t max_trans_id, uint64_t max_item_id) const {
    size_t cur = actions.size();
    if (dynamic_seq_len_ || cur == dml_act_num_) {
      if (dfs_cnt_ == subtask_num_) {
        if (max_trans_id == trans_num_ || save_history_with_empty_opt_)
          handle(ActionSequence(max_trans_id, item_num_, actions),seq_id_, max_trans_id);
        else
          cut_down_++;
        dfs_cnt_ -= subtask_num_;
      }
      ++seq_id_;
      ++dfs_cnt_;
    }
    if (cur != dml_act_num_) {
      // Make sure trans id is increment
      for (uint64_t trans_id = 0; trans_id < std::min(max_trans_id + 1, trans_num_); ++trans_id)
        for (uint64_t item_id = 0; item_id < std::min(max_item_id + 1, item_num_); ++item_id)
          for (Action::Type dml_act_type : {Action::Type::READ, Action::Type::WRITE}) {
            // Continuous read in same transaction is meaningless
            if (cur > 0 && dml_act_type == Action::Type::READ &&
                dml_act_type == actions[cur - 1].type() &&
                trans_id == actions[cur - 1].trans_id() && item_id == actions[cur - 1].item_id()) {
              continue;
            }
            actions.emplace_back(dml_act_type, trans_id, item_id);
            // if (!SkipDMLAction(actions)) { RecursiveFillDMLActionSeq(handle, actions, versions);
            // }
            RecursiveFillDMLActionSeq(handle, actions, std::max(trans_id + 1, max_trans_id),
                                      std::max(item_id + 1, max_item_id));
            actions.pop_back();
          }
    }
  }
  /*****************************************************************************
   * Just like backtrace structure, condition-if includes the end-condition. And condition-else
   *does the loop works. This function firstly build the Full Permutation of DTL. And then merge
   *it with DML. At last handle() it. args: dtl_act_types: set of DTL
   *
   *
   ****************************************************************************/
  void TraversalActionSequenceCreator::RecursiveFillDTLActionSeq(const std::function<void(const ActionSequence &)> &handle,
                                 std::vector<Action::Type> &dtl_act_types,
                                 const uint64_t max_trans_id, uint64_t abort_trans_num) const {
    if (!with_abort_) {
      dtl_act_types.assign(max_trans_id, Action::Type::COMMIT);
    }
    if (dtl_act_types.size() == max_trans_id) {
      std::vector<uint64_t> trans_order;
      for (uint64_t trans_id = 0; trans_id < max_trans_id; ++trans_id) {
        trans_order.push_back(trans_id);
      }
      do {
        std::vector<Action> dtl_actions;
        for (uint64_t trans_id : trans_order) {
          dtl_actions.emplace_back(dtl_act_types[trans_id], trans_id);
        }
        handle(ActionSequence(max_trans_id, item_num_, dtl_actions, abort_trans_num));
      } while (std::next_permutation(trans_order.begin(), trans_order.end()));
    } else {
      for (Action::Type dtl_act_type : {Action::Type::COMMIT, Action::Type::ABORT}) {
        dtl_act_types.emplace_back(dtl_act_type);
        if (dtl_act_type == Action::Type::ABORT) abort_trans_num++;
        RecursiveFillDTLActionSeq(handle, dtl_act_types, max_trans_id, abort_trans_num);
        dtl_act_types.pop_back();
      }
    }
  }

  void TraversalActionSequenceCreator::RecursiveMoveForwardDTLAction(const std::function<void(const ActionSequence &)> &handle,
                                     ActionSequence &act_seq, const size_t pos) const {
    if (pos == act_seq.size() || tail_dtl_) {
      handle(act_seq);
    } else {
      RecursiveMoveForwardDTLAction(handle, act_seq, pos + 1);
      size_t i = pos;
      while (i > 0 && act_seq[i - 1].trans_id() != act_seq[i].trans_id() &&
             (act_seq[i - 1].IsPointDML() || act_seq[i - 1].type() == Action::Type::SCAN_ODD)) {
        std::swap(act_seq[i - 1], act_seq[i]);
        RecursiveMoveForwardDTLAction(handle, act_seq, pos + 1);
        --i;
      }
      while (i < pos) {
        std::swap(act_seq[i], act_seq[i + 1]);
        ++i;
      }
    }
  }
