#pragma once
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>


enum Anomally
{
  //I, II
  DIRTY_WRITE,
  EDGE_CROESS,
  LOST_UPDATE,
  READ_SKEW,
  READ_WRITE_SKEW,
  THREE_TRANS_WRITE_SKEW,
  MULTI_TRANS_ANOMALY,
  //WSI
  WRITE_SKEW,
  WW_CONFLICT,
  //SSI
  RW_CONFLICT,
  //BOCC FOCC UNKNOWN
  UNKNOWN
};

extern std::unordered_map<int, std::string> Anomally2Name;


std::ostream& operator<<(std::ostream& os, const Anomally e) ;


enum class SerializeReadPolicy;

template <typename T, typename... Ts>
std::unique_ptr<T> MakeUnique(Ts&&... args) ;

template <typename T>
class Optional final {
 public:
  Optional();
  Optional(const T& value);
  Optional(T&& value);
  Optional(const Optional<T>& o);
  Optional(Optional<T>&& o);
  ~Optional();
  Optional<T>& operator=(const Optional& o);
  Optional<T>& operator=(Optional&&);

  bool HasValue() const;
  void Set(T&& value) ;
  void Set(const T& value) ;
  T& Get();
  const T& Get() const;

 private:
  bool has_value_;
  std::unique_ptr<T> value_;
};

class Action {
 public:
  enum class Type : char {
    UNKNOWN = '?',
    READ = 'R',
    WRITE = 'W',
    COMMIT = 'C',
    ABORT = 'A',
    SCAN_ODD = 'S'
  };
  Action() ;
  Action(const Type dtl_type, const uint64_t trans_id);
  Action(const Type dml_type, const uint64_t trans_id, const uint64_t item_id,
         const Optional<uint64_t> version = {});
  Action& operator=(const Action& action);
  virtual ~Action();

  Type type() const;
  uint64_t trans_id() const;
  uint64_t item_id() const;
  uint64_t version() const;
  void SetTransId(uint64_t trans_id);
  void SetItemId(const uint64_t item_id);
  void UpdateVersion(const uint64_t version);

  friend std::ostream& operator<<(std::ostream& os, const Action& action);

  friend std::istream& operator>>(std::istream& is, Type& type);

  friend std::istream& operator>>(std::istream& is, Action& action);

  bool IsPointDML() const;
  bool IsDTL() const;
  static bool IsPointDML(const Type& type);
  static bool IsDTL(const Type& type);
  // surport std::map
  bool operator<(const Action& r) const;
 private:
  Type type_;
  uint64_t trans_id_;
  Optional<uint64_t> item_id_;
  Optional<uint64_t> version_;  // version_ identify a unique version, but it CANNOT be compared to
                                // judge new or old
};

class ActionSequence {
 public:
  ActionSequence() ;
  ActionSequence(const uint64_t trans_num, const uint64_t item_num,
                 const std::vector<Action>& actions);
  ActionSequence(const uint64_t trans_num, const uint64_t item_num, std::vector<Action>&& actions);
  ActionSequence(const uint64_t trans_num, const uint64_t item_num,
                 const std::vector<Action>& actions, const uint64_t abort_trans_num);
  ActionSequence(ActionSequence&& act_seq);
  ActionSequence(const ActionSequence& act_seq);
  ~ActionSequence();

  ActionSequence& operator=(ActionSequence&& act_seq);
  ActionSequence operator+(const ActionSequence& act_seq) const;
  std::string to_string() const;
  std::vector<Action>& actions();
  const std::vector<Action>& actions() const;
  uint64_t trans_num() const;
  uint64_t abort_trans_num() const;
  uint64_t item_num() const;
  size_t size() const;

  friend std::ostream& operator<<(std::ostream& os, const ActionSequence& act_seq);

  friend std::istream& operator>>(std::istream& is, ActionSequence& act_seq);

  Action& operator[](const size_t index);
  void UpdateWriteVersions();
  // add
  // update write version, clean up read version
  void FillWriteVersions();
  /*

  template <SerializeReadPolicy>
  inline void FillReadVersions(ActionSequence& act_seq);
*/
 private:
  uint64_t trans_num_;
  uint64_t abort_trans_num_;
  uint64_t item_num_;  // 1
  std::vector<Action> actions_;
};

struct Options {
  uint64_t trans_num;
  uint64_t item_num;

  uint64_t subtask_num;
  uint64_t subtask_id;
  uint64_t max_dml;

  bool with_abort;
  bool tail_dtl;
  bool save_history_with_empty_opt;
  bool dynamic_seq_len;
};

