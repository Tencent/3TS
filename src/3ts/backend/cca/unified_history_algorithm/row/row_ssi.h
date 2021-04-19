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

#include "../util/util.h"
#include <type_traits>
#include <deque>
#include <memory>
#include <atomic>
#include <vector>
#include <mutex>
#include <optional>

namespace ttts {

template <UniAlgs ALG, typename Data>
class VerManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_SSI>>
{
  public:
    using Txn = TxnManager<ALG, Data>;

    // Default unreadable so set w_ts to UINT64_MAX
    VerManager(std::shared_ptr<Txn> w_txn, Data data, const uint64_t w_ts = UINT64_MAX)
        : w_txn_(std::move(w_txn)), data_(std::move(data)), w_ts_(w_ts) {}
    VerManager(const VerManager&) = default;
    VerManager(VerManager&&) = default;
    ~VerManager() {}

    std::shared_ptr<Txn> w_txn() const { return w_txn_; }

    const Data& data() const { return data_; }
    void set_data(Data data) { data_ = std::move(data); }

    uint64_t w_ts() const { return w_ts_; }
    void set_w_ts(const uint64_t w_ts)
    {
        assert(w_ts_ == UINT64_MAX || w_ts_ == w_ts);
        w_ts_ = w_ts;
    }

    bool IsWrittenBy(const uint64_t txn_id) const
    {
        return w_txn_ && w_txn_->txn_id() == txn_id;
    }

    const std::shared_ptr<Txn> w_txn_;
    Data data_;
    uint64_t w_ts_;
    // There may be two versions on same the row which have the same ver_id due to version revoking
    std::vector<std::shared_ptr<Txn>> r_txns_; // use shared_ptr instead of weak_ptr or readonly transactions will be released
};

template <UniAlgs ALG, typename Data>
class RowManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_SSI>>
{
  public:
    using Alg = AlgManager<ALG, Data>;
    using Txn = TxnManager<ALG, Data>;
    using Ver = VerManager<ALG, Data>;

    RowManager(const uint64_t row_id, Data init_data) : row_id_(row_id), versions_{Ver(nullptr, init_data, 0)} {}

    std::optional<Data> Read(Txn& txn)
    {
        // directly pass the logic and return the data of last version
        return versions_.back().data();
        // following is ignored

        std::lock_guard<std::mutex> l(m_);
        auto it = versions_.rbegin();

        // read version just written by itself
        if (const auto& latest_version = versions_.back(); latest_version.IsWrittenBy(txn.txn_id())) {
            return latest_version.data();
        }

        // read version written by another transaction with snapshot timestamp
        for (; it != versions_.rend() && it->w_ts() > txn.start_ts(); ++it) {
            continue;
        }
        assert(it != versions_.rend());
        it->r_txns_.emplace_back(txn.shared_from_this());

        { // logical not contains in SSI ---- build RW precedence
            if (it != versions_.rbegin() && !Txn::BuildRWConf(txn, *std::prev(it)->w_txn())) {
                return {};
            }
        }

        return it->data();
    }

    bool Prewrite(Data data, Txn& txn)
    {
        // directly pass the logic
        return true;
        // below is doing nothing 

        std::lock_guard<std::mutex> l(m_);
        assert(!versions_.empty());
        auto& latest_version = versions_.back();
        if (latest_version.IsWrittenBy(txn.txn_id())) {
            latest_version.set_data(data);
        } else if (latest_version.w_ts() > txn.start_ts()) { // prevent S1W2 precedence
            return false;
        }

        { // logical not contains in SSI ---- build RW precedence
            for (const auto& r_txn : latest_version.r_txns_) {
                assert(r_txn != nullptr);
                if (r_txn->txn_id() == txn.txn_id()) {
                    continue;
                }
                const auto r_txn_state = r_txn->state();
                const bool is_concurrent = r_txn_state == Txn::State::ACTIVE ||
                                        r_txn_state == Txn::State::PREPARING ||
                                        (r_txn_state == Txn::State::COMMITTED &&
                                            r_txn->commit_ts() > txn.start_ts());
                if (is_concurrent && !Txn::BuildRWConf(*r_txn, txn)) {
                    return false;
                }
            }
        }

        versions_.emplace_back(txn.shared_from_this(), std::move(data));

        return true;
    }

    void Write(Data /*data*/, Txn& txn)
    {
        std::lock_guard<std::mutex> l(m_);
        auto& latest_version = versions_.back();
        if (latest_version.IsWrittenBy(txn.txn_id())) {
            latest_version.set_w_ts(txn.commit_ts());
        }
    }

    void Revoke(Data /*data*/, Txn& txn)
    {
        // directly pass the logic
        return;
        // below is doing nothing

        std::lock_guard<std::mutex> l(m_);
        auto& latest_version = versions_.back();
        if (latest_version.IsWrittenBy(txn.txn_id())) {
            versions_.pop_back();
        }
    }

    void ClearExpiredVersions(const uint64_t min_start_ts)
    {
        while (versions_.size() >= 2 && versions_[1].w_ts() < min_start_ts) {
            versions_.pop_front();
        }
    }

  private:
    const uint64_t row_id_;
    std::mutex m_;
    std::deque<Ver> versions_; // old version is in front
};

}
