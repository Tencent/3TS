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
#include "row_ssi.h"

namespace ttts {

template <UniAlgs ALG, typename Data>
class TxnManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_SSI>>
        : public std::enable_shared_from_this<TxnManager<ALG, Data>>
{
  public:
    enum class State { ACTIVE, PREPARING, COMMITTED, ABORTED };

    TxnManager(const uint64_t txn_id, const uint64_t start_time)
        : txn_id_(txn_id), start_ts_(start_time), state_(State::ACTIVE), commit_ts_(UINT64_MAX),
          w_side_conf_(false), r_side_conf_(false) {}

    uint64_t txn_id() const { return txn_id_; }
    uint64_t start_ts() const { return start_ts_; }
    uint64_t commit_ts() const { return commit_ts_.load(); }

    void Commit(const uint64_t commit_ts)
    {
        commit_ts_ = commit_ts;
        state_ = State::COMMITTED;
    }

    void Abort()
    {
        state_ = State::ABORTED;
    }

    State state() const { return state_.load(); }

    bool WConf() const { return w_side_conf_.load(); }
    void SetWConf() { w_side_conf_ = true; }

    bool RConf() const { return r_side_conf_.load(); }
    void SetRConf() { r_side_conf_ = true; }

    static bool BuildRWConf(TxnManager& r_side, TxnManager& w_side)
    {
        if (r_side.WConf() || w_side.RConf()) {
            return false;
        }
        r_side.SetRConf();
        w_side.SetWConf();
        return true;
    }

  private:
    const uint64_t txn_id_;
    const uint64_t start_ts_;
    std::atomic<State> state_;
    std::atomic<uint64_t> commit_ts_;
    std::atomic<bool> w_side_conf_;
    std::atomic<bool> r_side_conf_;
};

}
