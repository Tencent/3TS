#pragma once

#include <unordered_map>
#include <memory>
#include <mutex>
#include <type_traits>
#include "util.h"
#include "dli_identify_util.h"

namespace ttts {

template <UniAlgs ALG, typename Data>
class TxnManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_CYCLE ||
                                                      ALG == UniAlgs::UNI_DLI_IDENTIFY_MERGE>>
{
  public:
    TxnManager(const uint64_t txn_id) : node_(std::make_shared<TxnNode>(txn_id)) {}

    const uint64_t txn_id() const { return node_->txn_id(); }

    std::unique_lock<std::mutex> l_;
    std::shared_ptr<TxnNode> node_; // release condition (1) for TxnNode
    std::unique_ptr<Path> cycle_;
    std::unordered_map<uint64_t, std::shared_ptr<VersionInfo<Data>>> pre_versions_; // record for revoke
};

}
