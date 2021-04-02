/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: williamcliu@tencent.com
 *
 */

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
                                                      ALG == UniAlgs::UNI_DLI_IDENTIFY_CHAIN>>
        : public std::enable_shared_from_this<TxnManager<ALG, Data>>
{
  public:
    template <typename ...Args>
    static std::shared_ptr<TxnManager> Construct(Args&&... args)
    {
        return std::make_shared<TxnManager>(std::forward<Args>(args)...);
    }

    TxnManager(const uint64_t txn_id, const uint64_t start_ts) : node_(std::make_shared<TxnNode>(txn_id)) {}
    TxnManager() {}

    uint64_t txn_id() const { return node_->txn_id(); }

    std::unique_lock<std::mutex> l_;
    std::shared_ptr<TxnNode> node_; // release condition (1) for TxnNode
    std::unique_ptr<Path> cycle_;
    std::unordered_map<uint64_t, std::shared_ptr<VersionInfo<Data>>> pre_versions_; // record for revoke
};

}
