#include "algorithm.h"
#include "../../../../contrib/deneva/unified_concurrency_control/util.h"
#include "anomaly_type.h"
#include "prece_type.h"
#include "algorithm.h"
#include "../../../../contrib/deneva/unified_concurrency_control/dli_identify_util.h"
#include "../../../../contrib/deneva/unified_concurrency_control/txn_dli_identify.h"
#include "../../../../contrib/deneva/unified_concurrency_control/row_prece.h"
#include "../../../../contrib/deneva/unified_concurrency_control/alg_dli_identify_chain.h"
#include "../../../../contrib/deneva/unified_concurrency_control/alg_dli_identify_cycle.h"
#include <optional>

namespace ttts {

template<UniAlgs ALG, typename Data>
class UnifiedHistoryAlgorithm : public HistoryAlgorithm {
public:
  UnifiedHistoryAlgorithm() : HistoryAlgorithm(ToString(ALG)), anomaly_counts_{0} {}

  virtual bool Check(const History& history, std::ostream* const os) const override {
    return !(GetAnomaly(history, os).has_value());
  }

  std::optional<AnomalyType> GetAnomaly(const History& history, std::ostream* const os) const {
    std::unique_ptr<AlgManager<ALG, Data>> alg_manager = std::make_unique<AlgManager<ALG, Data>>();
    std::unordered_map<uint64_t, std::unique_ptr<TxnManager<ALG, Data>>> txn_map;
    std::unordered_map<uint64_t, std::unique_ptr<RowManager<ALG, Data>>> row_map;
    std::unordered_map<uint64_t, uint64_t> row_value_map;
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> trans_write_set_list(history.trans_num());
    for (size_t i = 0, size = history.size(); i < size; ++i) {
      const Operation& operation = history.operations()[i];
      const uint64_t trans_id = operation.trans_id();
      // init txn_map
      if (txn_map.count(trans_id) == 0) {
        std::unique_ptr<TxnManager<ALG, Data>> txn = std::make_unique<TxnManager<ALG, Data>>(trans_id);
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
        if (Operation::Type::READ == operation.type()) {
          row_map[item_id]->Read(*txn_map[trans_id]);
        // Exec Prewrite
        } else if (Operation::Type::WRITE == operation.type()) {
          row_value_map[item_id] += 1;
          row_map[item_id]->Prewrite(row_value_map[item_id], *txn_map[trans_id]);
          trans_write_set_list[trans_id].emplace_back(std::pair<uint64_t, uint64_t>(item_id, row_value_map[item_id]));
          //std::cout << "W->tx_id:" << trans_id << " item_id:" << item_id << " value:" << row_value_map[item_id] << std::endl;
        // Exec Abort
        }
      } else if (Operation::Type::ABORT == operation.type()) {
        alg_manager->Abort(*txn_map[trans_id]);
        // rollback written row
        for (const auto& item_write : trans_write_set_list[trans_id]) {
          row_map[item_write.first]->Revoke(item_write.second, *txn_map[trans_id]);
        }
        // check data anomaly in abort
        if (txn_map[trans_id]->cycle_ != nullptr) {
          //std::cout << "abort::preces_size:" << txn_map[trans_id]->cycle_->Preces().size() << std::endl;
          const auto anomaly = AlgManager<ALG, Data>::IdentifyAnomaly(txn_map[trans_id]->cycle_->Preces());
          ++(anomaly_counts_.at(static_cast<uint32_t>(anomaly)));
          return anomaly;
        }
      } else if (Operation::Type::COMMIT == operation.type()) {
        bool ret = alg_manager->Validate(*txn_map[trans_id]);
        if (ret) {
          alg_manager->Commit(*txn_map[trans_id]);
          // data persistence
          for (const auto& row : row_map) {
            row.second->Write(row_value_map[row.first], *txn_map[trans_id]);
          }
        } else {
          alg_manager->Abort(*txn_map[trans_id]);
          // rollback written row
          for (const auto& item_write : trans_write_set_list[trans_id]) {
            row_map[item_write.first]->Revoke(item_write.second, *txn_map[trans_id]);
          }
        }
        // check data anomaly in commit
        if (txn_map[trans_id]->cycle_ != nullptr) {
          //std::cout << "commit::preces_size" << txn_map[trans_id]->cycle_->Preces().size() << std::endl;
          const auto anomaly = AlgManager<ALG, Data>::IdentifyAnomaly(txn_map[trans_id]->cycle_->Preces());
          ++(anomaly_counts_.at(static_cast<uint32_t>(anomaly)));
          return anomaly;
        }
      }
    }
    return {};
  }

private:
  mutable std::array<std::atomic<uint64_t>, Count<AnomalyType>()> anomaly_counts_;
};

}
