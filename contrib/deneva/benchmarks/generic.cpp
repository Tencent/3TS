#include"generic.h"

std::unordered_map<int, std::string> Anomally2Name=
{
  //I, II
  {DIRTY_WRITE,"DIRTY_WRITE"},
  {EDGE_CROESS,"EDGE_CROESS"},
  {LOST_UPDATE,"LOST_UPDATE"},
  {READ_SKEW,"READ_SKEW"},
  {READ_WRITE_SKEW,"READ_WRITE_SKEW"},
  {THREE_TRANS_WRITE_SKEW,"THREE_TRANS_WRITE_SKEW"},
  {MULTI_TRANS_ANOMALY,"MULTI_TRANS_ANOMALY"},
  //WSI
  {WRITE_SKEW, "WRITE_SKEW"},
  {WW_CONFLICT,"WW_CONFLICT"},
  //SSI
  {RW_CONFLICT,"RW_CONFLICT"},
  //BOCC FOCC UNKNOWN
  {UNKNOWN,"UNKNOWN"}
};

std::ostream& operator<<(std::ostream& os, const Anomally e) {
  switch (e) {
  case DIRTY_WRITE: \
    return os << "DIRTY_WRITE";
  case EDGE_CROESS: \
    return os << "EDGE_CROESS";
  case LOST_UPDATE: \
    return os << "LOST_UPDATE";
  case READ_SKEW: \
    return os << "READ_SKEW";
  case READ_WRITE_SKEW: \
    return os << "READ_WRITE_SKEW";
  case THREE_TRANS_WRITE_SKEW: \
    return os << "THREE_TRANS_WRITE_SKEW";
  case MULTI_TRANS_ANOMALY: \
    return os << "MULTI_TRANS_ANOMALY";
  case WRITE_SKEW: \
    return os << "WRITE_SKEW";
  case WW_CONFLICT: \
    return os << "WW_CONFLICT";
  case RW_CONFLICT: \
    return os << "RW_CONFLICT";
  default:
      return os << "UNKNOWN";
  }
}


enum class SerializeReadPolicy { UNCOMMITTED_READ, COMMITTED_READ, REPEATABLE_READ, SI_READ };

template <typename T, typename... Ts>
std::unique_ptr<T> MakeUnique(Ts&&... args) {
  return std::unique_ptr<T>(new T(std::forward<Ts>(args)...));
}

  Action::Action() : type_(Type::UNKNOWN), trans_id_(0) {}
  Action::Action(const Type dtl_type, const uint64_t trans_id) : type_(dtl_type), trans_id_(trans_id) {
    if (!IsDTL()) {
      throw std::to_string(static_cast<char>(dtl_type)) + " is not a DTL Action";
    }
  }
  Action::Action(const Type dml_type, const uint64_t trans_id, const uint64_t item_id,
         const Optional<uint64_t> version)
      : type_(dml_type), trans_id_(trans_id), item_id_(item_id), version_(version) {
    if (!IsPointDML()) {
      throw std::to_string(static_cast<char>(dml_type)) + " is not a Point DML Action";
    }
  }
  Action& Action::operator=(const Action& action) = default;
  Action::~Action() {}

  Action::Type Action::type() const { return type_; }
  uint64_t Action::trans_id() const { return trans_id_; }
  uint64_t Action::item_id() const {
    if(item_id_.HasValue())
      return item_id_.Get();
    else
      return 0;
  }
  uint64_t Action::version() const { return version_.Get(); }
  void Action::SetTransId(uint64_t trans_id) { trans_id_ = trans_id; }
  void Action::SetItemId(const uint64_t item_id) {
    if (IsDTL()) {
      throw "DTL actions update item id is meaningless";
    }
    item_id_.Set(item_id);
  }
  void Action::UpdateVersion(const uint64_t version) {
    if (IsDTL()) {
      throw "DML actions update version is meaningless";
    }
    version_.Set(version);
  }

  std::ostream& operator<<(std::ostream& os, const Action& action) {
    os << static_cast<char>(action.type_) << action.trans_id_;
    if (action.IsPointDML()) {
      if (action.item_id_.Get() >= 26) {
        throw "Not support item_id equal or larger than 26 yet";
      }
      os << static_cast<char>('a' + action.item_id_.Get());
    }

    return os;
  }

 std::istream& operator>>(std::istream& is, Action::Type& type) {
    char c;
    is >> c;
    switch (c) {
      case 'W':
        type = Action::Type::WRITE;
        break;
      case 'R':
        type = Action::Type::READ;
        break;
      case 'C':
        type = Action::Type::COMMIT;
        break;
      case 'A':
        type = Action::Type::ABORT;
        break;
      case 'S':
        type = Action::Type::SCAN_ODD;
        break;
      default:
        type = Action::Type::UNKNOWN;
        is.setstate(std::ios::failbit);
    }
    return is;
  }

  std::istream& operator>>(std::istream& is, Action& action) {
    if (!(is >> action.type_) || !(is >> action.trans_id_)) {
      return is;
    }
    if (action.type_ == Action::Type::WRITE || action.type_ == Action::Type::READ) {
      char item_c;
      if (!(is >> item_c) || !std::islower(item_c)) {
        is.setstate(std::ios::failbit);
        return is;
      }
      action.item_id_ = item_c - 'a';
    }
    return is;
  }

  bool Action::IsPointDML() const { return IsPointDML(type_); }
  bool Action::IsDTL() const { return IsDTL(type_); }
  bool Action::IsPointDML(const Type& type) { return type == Type::READ || type == Type::WRITE; }
  bool Action::IsDTL(const Type& type) { return type == Type::COMMIT || type == Type::ABORT; }
  // surport std::map
  bool Action::operator<(const Action& r) const {
    if (trans_id() == r.trans_id()) {
      uint64_t l_item_id = item_id_.HasValue() ? item_id() : -1;
      uint64_t r_item_id = r.item_id_.HasValue() ? r.item_id() : -1;
      if (l_item_id == r_item_id) {
        uint64_t l_version = version_.HasValue() ? version() : -1;
        uint64_t r_version = r.version_.HasValue() ? r.version() : -1;
        if (l_version == r_version) return type() < r.type();
        return l_version < r.version();
      }
      return l_item_id < r_item_id;
    }
    return trans_id() < r.trans_id();
  }



  ActionSequence::ActionSequence() : ActionSequence(0, 0, {}) {}
  ActionSequence::ActionSequence(const uint64_t trans_num, const uint64_t item_num,
                 const std::vector<Action>& actions)
      : trans_num_(trans_num),abort_trans_num_(0), item_num_(item_num), actions_(actions) {}
  ActionSequence::ActionSequence(const uint64_t trans_num, const uint64_t item_num, std::vector<Action>&& actions)
      : trans_num_(trans_num), abort_trans_num_(0),item_num_(item_num), actions_(actions) {}
  ActionSequence::ActionSequence(const uint64_t trans_num, const uint64_t item_num,
                 const std::vector<Action>& actions, const uint64_t abort_trans_num)
      : trans_num_(trans_num),
        abort_trans_num_(abort_trans_num),
        item_num_(item_num),
        actions_(actions)
         {}
  ActionSequence::ActionSequence(ActionSequence&& act_seq) = default;
  ActionSequence::ActionSequence(const ActionSequence& act_seq) = default;
  ActionSequence::~ActionSequence() {}

  ActionSequence& ActionSequence::operator=(ActionSequence&& act_seq) = default;
  ActionSequence ActionSequence::operator+(const ActionSequence& act_seq) const {
    if (trans_num_ != act_seq.trans_num_ || item_num_ != act_seq.item_num_) {
      throw "Action sequence mismatch";
    }
    std::vector<Action> new_actions = actions_;
    for (const auto& action : act_seq.actions_) {
      new_actions.push_back(action);
    }
    return ActionSequence(trans_num_, item_num_, std::move(new_actions),
                          abort_trans_num_ + act_seq.abort_trans_num_);
  }
  std::string ActionSequence::to_string() const {
    std::string ret;
    for (size_t i = 0; i < actions_.size(); i++) {
      ret.push_back(static_cast<char>(actions_[i].type()));
      ret.push_back('0' + actions_[i].trans_id());
      if (actions_[i].IsPointDML()) ret.push_back('a' + actions_[i].item_id());
    }
    return ret;
  }
  std::vector<Action>& ActionSequence::actions() { return actions_; }
  const std::vector<Action>& ActionSequence::actions() const { return actions_; }
  uint64_t ActionSequence::trans_num() const { return trans_num_; }
  uint64_t ActionSequence::abort_trans_num() const { return abort_trans_num_; }
  uint64_t ActionSequence::item_num() const { return item_num_; }
  size_t ActionSequence::size() const { return actions_.size(); }

  std::ostream& operator<<(std::ostream& os, const ActionSequence& act_seq) {
    for (const Action& action : act_seq.actions_) {
      os << action << ' ';
    }
    return os;
  }

  std::istream& operator>>(std::istream& is, ActionSequence& act_seq) {
    std::string s;
    if (std::getline(is, s)) {
      std::stringstream ss(s);
      std::vector<Action> actions;
      uint64_t trans_num = 0;
      uint64_t item_num = 0;
      for (std::stringstream ss(s); !ss.eof() && !ss.fail();) {
        Action action;
        if (ss >> action) {
          actions.emplace_back(action);
          trans_num = std::max(trans_num, action.trans_id() + 1);
          if (action.IsPointDML()) {
            item_num = std::max(item_num, action.item_id() + 1);
          }
        }
      }
      if (ss.fail()) {
        std::cerr << "Invalid action sequence: \'" << s << "\'" << std::endl;
      } else {
        act_seq = ActionSequence(trans_num, item_num, actions);
      }
    }
    return is;
  }

  Action& ActionSequence::operator[](const size_t index) { return actions_[index]; }

  void ActionSequence::UpdateWriteVersions() {
    std::vector<uint64_t> item_version(item_num_, 0);
    for (Action& action : actions_) {
      if (action.type() == Action::Type::WRITE) {
        action.UpdateVersion(++item_version[action.item_id()]);
      }
    }
  }
  // add
  // update write version, clean up read version
  void ActionSequence::FillWriteVersions() {
    std::vector<uint64_t> item_version(item_num_, 0);
    for (Action& action : actions_) {
      if (action.type() == Action::Type::WRITE) {
        action.UpdateVersion(++item_version[action.item_id()]);
      } else if (action.type() == Action::Type::READ) {
        action.UpdateVersion(-1);  // clear read version as -1
      }
    }
  }

/*
template <>
void ActionSequence::FillReadVersions<SerializeReadPolicy::UNCOMMITTED_READ>(
    ActionSequence& act_seq) {
  std::vector<std::vector<Optional<uint64_t>>> trans_write_item_versions(
      act_seq.trans_num(), std::vector<Optional<uint64_t>>(act_seq.item_num()));
  std::vector<std::vector<Optional<uint64_t>>> item_version_link(
      act_seq.item_num(), {0});

  const auto latest_version = [&item_version_link](const uint64_t item_id) {
    const std::vector<Optional<uint64_t>>& version_link = item_version_link[item_id];
    uint64_t i = version_link.size() - 1;
    for (; i >= 0 && !version_link[i].HasValue(); --i)
      ;
    assert(i < version_link.size());
    return version_link[i].Get();
  };
  const auto release_version = [&item_version_link](const uint64_t item_id,
                                                    const uint64_t version) {
    for (Optional<uint64_t>& cur_version : item_version_link[item_id]) {
      if (cur_version.HasValue() && cur_version.Get() == version) {
        cur_version = {};
        return;
      }
    }
    assert(false);  // cannot found the version
  };
  for (Action& action : act_seq.actions()) {
    if (action.type() == Action::Type::READ) {
      // we did not change version when resort actions, so we use latest version instead of
      // action.version()
      // act_seq.PushTransReadResult(action.trans_id(), action.item_id(),
      //                            latest_version(action.item_id()));
      Optional<uint64_t> read_version =
          trans_write_item_versions[action.trans_id()][action.item_id()];
      if (read_version.HasValue()) {
        action.UpdateVersion(read_version.Get());
      } else
        action.UpdateVersion(0);
    } else if (action.type() == Action::Type::WRITE) {
      item_version_link[action.item_id()].push_back(action.version());
      Optional<uint64_t>& my_last_write_version =
          trans_write_item_versions[action.trans_id()][action.item_id()];
      if (my_last_write_version.HasValue()) {
        release_version(action.item_id(), my_last_write_version.Get());
      }
      my_last_write_version = action.version();
    } else if (action.type() == Action::Type::ABORT) {
      for (uint64_t item_id = 0; item_id < act_seq.item_num(); ++item_id) {
        const Optional<uint64_t>& my_last_write_version =
            trans_write_item_versions[action.trans_id()][item_id];
        if (my_last_write_version.HasValue()) {
          release_version(item_id, my_last_write_version.Get());
        }
      }
    } else if (action.type() == Action::Type::COMMIT) {
    } else {
      throw "Unexpected action type:" + std::to_string(static_cast<char>(action.type()));
    }
  }

  std::vector<uint64_t> final_versions(act_seq.item_num());
  for (uint64_t item_id = 0; item_id < act_seq.item_num(); ++item_id) {
    final_versions[item_id] = latest_version(item_id);
  }
  // act_seq.SetItemFinalVersions(std::move(final_versions));
}
template <>
void ActionSequence::FillReadVersions<SerializeReadPolicy::COMMITTED_READ>(
    ActionSequence& act_seq) {
  std::vector<std::vector<Optional<uint64_t>>> trans_write_item_versions(
      act_seq.trans_num(), std::vector<Optional<uint64_t>>(act_seq.item_num()));
  std::vector<uint64_t> latest_versions(act_seq.item_num(), 0);
  for (Action& action : act_seq.actions()) {
    if (action.type() == Action::Type::READ) {
      Optional<uint64_t> read_version =
          trans_write_item_versions[action.trans_id()][action.item_id()];
      if (read_version.HasValue())
        action.UpdateVersion(read_version.Get());
      else
        action.UpdateVersion(latest_versions[action.item_id()]);
    } else if (action.type() == Action::Type::WRITE) {
      trans_write_item_versions[action.trans_id()][action.item_id()] = action.version();
    } else if (action.type() == Action::Type::ABORT) {
    } else if (action.type() == Action::Type::COMMIT) {
      for (uint64_t item_id = 0; item_id < act_seq.item_num(); item_id++) {
        Optional<uint64_t> write_version = trans_write_item_versions[action.trans_id()][item_id];
        if (write_version.HasValue()) {
          latest_versions[item_id] = write_version.Get();
        }
      }
    } else {
      throw "Unexpected action type:" + std::to_string(static_cast<char>(action.type()));
    }
  }
}
template <>
void ActionSequence::FillReadVersions<SerializeReadPolicy::REPEATABLE_READ>(
    ActionSequence& act_seq) {
  std::vector<std::vector<Optional<uint64_t>>> trans_read_item_versions(
      act_seq.trans_num(), std::vector<Optional<uint64_t>>(act_seq.item_num()));
  std::vector<std::vector<Optional<uint64_t>>> trans_write_item_versions(
      act_seq.trans_num(), std::vector<Optional<uint64_t>>(act_seq.item_num()));
  std::vector<uint64_t> latest_versions(act_seq.item_num(), 0);
  for (Action& action : act_seq.actions()) {
    if (action.type() == Action::Type::READ) {
      const Optional<uint64_t>& write_version =
          trans_write_item_versions[action.trans_id()][action.item_id()];
      if (write_version.HasValue()) {  // item has written
        action.UpdateVersion(write_version.Get());
      } else {
        Optional<uint64_t>& read_version =
            trans_read_item_versions[action.trans_id()][action.item_id()];
        if (!read_version.HasValue()) {  // item has read
          read_version = latest_versions[action.item_id()];
        }
        action.UpdateVersion(read_version.Get());
      }
    } else if (action.type() == Action::Type::WRITE) {
      trans_write_item_versions[action.trans_id()][action.item_id()] = action.version();
    } else if (action.type() == Action::Type::ABORT) {
    } else if (action.type() == Action::Type::COMMIT) {
      for (uint64_t item_id = 0; item_id < act_seq.item_num(); item_id++) {
        Optional<uint64_t> write_version = trans_write_item_versions[action.trans_id()][item_id];
        if (write_version.HasValue()) {
          latest_versions[item_id] = write_version.Get();
        }
      }
    } else {
      throw "Unexpected action type:" + std::to_string(static_cast<char>(action.type()));
    }
  }
}
template <>
void ActionSequence::FillReadVersions<SerializeReadPolicy::SI_READ>(ActionSequence& act_seq) {
  std::vector<uint64_t> latest_versions(act_seq.item_num(), 0);
  std::vector<std::vector<uint64_t>> trans_item_versions_backup(act_seq.trans_num());
  std::vector<std::vector<uint64_t>> trans_item_versions_snapshot(act_seq.trans_num());
  for (Action& action : act_seq.actions()) {
    if (trans_item_versions_snapshot[action.trans_id()].empty()) {
      trans_item_versions_snapshot[action.trans_id()] = latest_versions;
      trans_item_versions_backup[action.trans_id()] = latest_versions;
    }
    if (action.type() == Action::Type::READ) {
      uint64_t read_version = trans_item_versions_snapshot[action.trans_id()][action.item_id()];
      action.UpdateVersion(read_version);
    } else if (action.type() == Action::Type::WRITE) {
      trans_item_versions_snapshot[action.trans_id()][action.item_id()] = action.version();
    } else if (action.type() == Action::Type::ABORT) {
    } else if (action.type() == Action::Type::COMMIT) {
      for (uint64_t item_id = 0; item_id < act_seq.item_num(); ++item_id) {
        if (trans_item_versions_snapshot[action.trans_id()][item_id] !=
            trans_item_versions_backup[action.trans_id()][item_id]) {
          latest_versions[item_id] = trans_item_versions_snapshot[action.trans_id()][item_id];
        }
      }
    } else {
      throw "Unexpected action type:" + std::to_string(static_cast<char>(action.type()));
    }
  }
}
*/
