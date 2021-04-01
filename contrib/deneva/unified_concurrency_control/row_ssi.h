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

#include "util.h"
#include <type_traits>
#include <deque>
#include <memory>

template <UniAlgs ALG, typename Data>
class TxnNode<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_SSI>>
{
  public:
    TxnNode() : txn_id_(txn_id), start_time_(start_time), state_(State::ACTIVE), w_side_conf_(false),
                r_side_conf_(false) {}

  private:
    const uint64_t txn_id_;
    const uint64_t start_ts_;
    std::atomic<State> state_;
    uint64_t commit_ts_;
    std::atomic<bool> w_side_conf_;
    std::atomic<bool> r_side_conf_;
};

template <typename Data>
class Version
{

    const std::weak_ptr<TxnNode> w_txn_;
    const uint64_t w_ts_;
    Data const data_;
    std::vector<std::weak_ptr<TxnNode>> r_txns_;
};

template <UniAlgs ALG, typename Data>
class RowManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_SSI>>
{
  public:
    using Txn = TxnManager<ALG, Data>;

    RowManager(const uint64_t row_id, Data init_data) {}

    std::optional<Data> Read(Txn& txn)
    {
        std::lock_guard<std::mutex> l(m_);
        const auto& [latest_w_ts, latest_version] = versions_.back();
        if (latest_w_ts > l)
    }

    bool Prewrite(Data data, Txn& txn)
    {
        std::lock_guard<std::mutex> l(m_);
        assert(!version_.empty());
        const auto& [latest_w_ts, latest_version] = versions_.back();
        if (latest_w_ts > txn.start_ts_ || w_lock_txn_ != nullptr) { // prevent S1W2 precedence
            return false;
        }
        version.foreach_r_txn([](TxnNode& from_txn) {
            if (from_txn.state() == from_txnNode::State::ACTIVE ||
                from_txn.state() == from_txnNode::State::PREPARING ||
                (from_txn.state() == from_txnNode::State::COMMITTED && from_txn.commit_ts_ > txn.start_ts_)) {
                build_preces_from_r_txns_(from_txn, txn);
            }
        });

    }

    void Write(Data _, Txn& txn)
    {
    }

    void Revoke(Data _, Txn& txn)
    {
        // do nothing
    }

  private:
    const uint64_t row_id_;
    std::mutex m_;
    std::deque<std::pair<uint64_t, VersionInfo<Data>>> versions_; // first is w_ts
    std::shared_ptr<Txn> w_lock_txn_;
};
