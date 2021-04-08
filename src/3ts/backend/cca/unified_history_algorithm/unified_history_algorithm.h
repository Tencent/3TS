#include "../algorithm.h"
#include "util/util.h"
#include "util/anomaly_type.h"
#include "util/prece_type.h"
#include "../algorithm.h"
#include "util/dli_identify_util.h"
#include "txn/txn_dli_identify.h"
#include "txn/txn_ssi.h"
#include "row/row_prece.h"
#include "row/row_ssi.h"
#include "alg/alg_dli_identify_chain.h"
#include "alg/alg_dli_identify_cycle.h"
#include "alg/alg_ssi.h"
#include <optional>

namespace ttts {

template<UniAlgs ALG, typename Data>
class UnifiedHistoryAlgorithm : public HistoryAlgorithm {
public:
  UnifiedHistoryAlgorithm() : HistoryAlgorithm(ToString(ALG)), anomaly_counts_{0} {}

  virtual bool Check(const History& history, std::ostream* const os) const override {
    if constexpr (IDENTIFY_ANOMALY) {
        return !CheckInternal_(history, os).has_value();
    } else {
        return CheckInternal_(history, os);
    }

    return !(GetAnomaly(history, os).has_value());
  }

  std::optional<AnomalyType> GetAnomaly(const History& history, std::ostream* const os) const {
    if constexpr (IDENTIFY_ANOMALY) {
        return CheckInternal_(history, os);
    } else {
        if (CheckInternal_(history, os)) {
            return {};
        } else {
            // TODO: forbid GetAnomaly in compile time
            return AnomalyType::UNKNOWN_1;
        }
    }
  }

private:
  static constexpr bool IDENTIFY_ANOMALY = ALG == UniAlgs::UNI_DLI_IDENTIFY_CHAIN || ALG == UniAlgs::UNI_DLI_IDENTIFY_CYCLE;
  mutable std::array<std::atomic<uint64_t>, Count<AnomalyType>()> anomaly_counts_;

  std::conditional_t<IDENTIFY_ANOMALY, std::optional<AnomalyType>, bool> ReturnWithUnknowDA_() const {
    if constexpr (IDENTIFY_ANOMALY) {
      return AnomalyType::UNKNOWN_1;
    } else {
      return false;
    }
  }

  std::optional<AnomalyType> ReturnWithDA_(const TxnManager<ALG, Data>& txn_manager) const {
    if constexpr (IDENTIFY_ANOMALY) {
      if (txn_manager.cycle_ != nullptr) {
        const auto anomaly = AlgManager<ALG, Data>::IdentifyAnomaly(txn_manager.cycle_->Preces());
        ++(anomaly_counts_.at(static_cast<uint32_t>(anomaly)));
        return anomaly;
      }
    }

    return {};
  }

  std::conditional_t<IDENTIFY_ANOMALY, std::optional<AnomalyType>, bool> ReturnWithNoDA_() const {
    if constexpr (IDENTIFY_ANOMALY) {
      return {};
    } else {
      return true;
    }
  }

  std::conditional_t<IDENTIFY_ANOMALY, std::optional<AnomalyType>, bool> ExecAbortInternal_(
      AlgManager<ALG, Data>& alg_manager, TxnManager<ALG, Data>& txn_manager,
      const std::vector<std::pair<uint64_t, uint64_t>>& trans_write_set,
      const std::unordered_map<uint64_t, std::unique_ptr<RowManager<ALG, Data>>>& row_map) const{
    alg_manager.Abort(txn_manager);
    // rollback written row
    RollbackWrittenRow_(trans_write_set, row_map, txn_manager);

    return ReturnWithDA_(txn_manager);
  }

  std::conditional_t<IDENTIFY_ANOMALY, std::optional<AnomalyType>, bool> ExecCommitInternal_(
      AlgManager<ALG, Data>& alg_manager, TxnManager<ALG, Data>& txn_manager,
      const std::unordered_map<uint64_t, std::unique_ptr<RowManager<ALG, Data>>>& row_map,
      const std::unordered_map<uint64_t, uint64_t>& row_value_map, uint64_t commit_ts,
      const std::vector<std::pair<uint64_t, uint64_t>>& trans_write_set) const {

    if (alg_manager.Validate(txn_manager)) {
      alg_manager.Commit(txn_manager, commit_ts);
      // data persistence
      for (const auto& row : row_map) {
        row.second->Write(row_value_map.at(row.first), txn_manager);
      }

      return ReturnWithNoDA_();
    } else {
      alg_manager.Abort(txn_manager);
      // rollback written row
      RollbackWrittenRow_(trans_write_set, row_map, txn_manager);

      return ReturnWithDA_(txn_manager);
    }
  }

  void RollbackWrittenRow_(
      const std::vector<std::pair<uint64_t, uint64_t>>& trans_write_set,
      const std::unordered_map<uint64_t, std::unique_ptr<RowManager<ALG, Data>>>& row_map,
      TxnManager<ALG, Data>& txn_manager) const {
    // rollback written row
    for (const auto& item_write : trans_write_set) {
      row_map.at(item_write.first)->Revoke(item_write.second, txn_manager);
    }
  }

  std::conditional_t<IDENTIFY_ANOMALY, std::optional<AnomalyType>, bool> CheckInternal_(
      const History& history, std::ostream* const os) const {
    std::unique_ptr<AlgManager<ALG, Data>> alg_manager = std::make_unique<AlgManager<ALG, Data>>();
    std::unordered_map<uint64_t, std::shared_ptr<TxnManager<ALG, Data>>> txn_map;
    std::unordered_map<uint64_t, std::unique_ptr<RowManager<ALG, Data>>> row_map;
    std::unordered_map<uint64_t, uint64_t> row_value_map;
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> trans_write_set_list(history.trans_num());
    for (size_t i = 0, size = history.size(); i < size; ++i) {
      const Operation& operation = history.operations()[i];
      const uint64_t trans_id = operation.trans_id();
      // init txn_map
      if (txn_map.count(trans_id) == 0) {
        std::shared_ptr<TxnManager<ALG, Data>> txn = TxnManager<ALG, Data>::Construct(trans_id, i);
        txn_map.emplace(trans_id, std::move(txn));
      }
      // check operation whether R or W
      if (operation.IsPointDML()) {
        const uint64_t item_id = operation.item_id();
        // If it is an unaccessed variable, the value is initialized to 0.
        if (row_value_map.count(0) == 0) {
          row_value_map.emplace(item_id, 0);
        }
        // If it is an unaccessed variable, row obj will be put into row_map.
        if (row_map.count(item_id) == 0) {
          std::unique_ptr<RowManager<ALG, Data>> row = std::make_unique<RowManager<ALG, Data>>(item_id, 0);
          row_map.emplace(item_id, std::move(row));
        }
        // Exec Read
        if (Operation::Type::READ == operation.type() && !row_map[item_id]->Read(*txn_map[trans_id])) {
          return ReturnWithUnknowDA_();
        // Exec Prewrite
        } else if (Operation::Type::WRITE == operation.type()) {
          row_value_map[item_id] += 1;
          if (!row_map[item_id]->Prewrite(row_value_map[item_id], *txn_map[trans_id])) {
            return ReturnWithUnknowDA_();
          }

          trans_write_set_list[trans_id].emplace_back(std::pair<uint64_t, uint64_t>(item_id, row_value_map[item_id]));
        }
      // Exec Abort
      } else if (Operation::Type::ABORT == operation.type()) {
        const auto& ret = ExecAbortInternal_(*alg_manager, *txn_map[trans_id], trans_write_set_list[trans_id], row_map);
        txn_map.erase(trans_id);

        if (ret.has_value()) {
          return ret;
        }
      // Exec Commit
      } else if (Operation::Type::COMMIT == operation.type()) {
        const auto& ret = ExecCommitInternal_(*alg_manager, *txn_map[trans_id], row_map,
                                              row_value_map, i, trans_write_set_list[trans_id]);
        txn_map.erase(trans_id);

        if (ret.has_value()) {
          return ret;
        }
      }
    }

    return ReturnWithNoDA_();
  }
}; // class: UnifiedHistoryAlgorithm
} // namespace: ttts
