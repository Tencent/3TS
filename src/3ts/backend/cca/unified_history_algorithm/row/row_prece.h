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

#include <mutex>
#include <unordered_map>
#include <atomic>
#include <cassert>
#include <memory>
#include <iostream>
#include <vector>
#include "../util/util.h"
#include "../txn/txn_dli_identify.h"

namespace ttts {

// Not thread safe. Protected by RowManager<DLI_IDENTIFY>::m_
template <UniAlgs ALG, typename Data>
class VerManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_CYCLE ||
                                                      ALG == UniAlgs::UNI_DLI_IDENTIFY_CHAIN>>
{
  public:
    using Txn = TxnManager<ALG, Data>;

    VerManager(std::weak_ptr<Txn> w_txn, Data&& data, const uint64_t ver_id)
        : w_txn_(std::move(w_txn)), data_(std::move(data)), ver_id_(ver_id) {}
    VerManager(const VerManager&) = default;
    VerManager(VerManager&&) = default;
    ~VerManager() {}

    std::shared_ptr<Txn> w_txn() const { return w_txn_.lock(); }

    template <typename Task>
    void foreach_r_txn(Task&& task) const
    {
        for (const auto& r_txn_weak : r_txns_) {
            if (const auto r_txn = r_txn_weak.lock()) {
                task(*r_txn);
            }
        }
    }

    void add_r_txn(std::weak_ptr<Txn> txn) { r_txns_.emplace_back(std::move(txn)); }

    const Data& data() const { return data_; }

    uint64_t ver_id() const { return ver_id_; }

  private:
    const std::weak_ptr<Txn> w_txn_;
    Data const data_;
    // There may be two versions on same the row which have the same ver_id due to version revoking
    const uint64_t ver_id_;
    std::vector<std::weak_ptr<Txn>> r_txns_;
};

template <UniAlgs ALG, typename Data>
class RowManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_CYCLE ||
                                                      ALG == UniAlgs::UNI_DLI_IDENTIFY_CHAIN>>
{
  public:
    using Txn = TxnManager<ALG, Data>;
    using Ver = VerManager<ALG, Data>;

    RowManager(const uint64_t row_id, Data init_data)
        : row_id_(row_id), cur_ver_id_(0),
          latest_version_(std::make_shared<Ver>(std::weak_ptr<Txn>(), std::move(init_data),
                      0))
    {}

    RowManager() {}

    std::optional<Data> Read(Txn& txn)
    {
        return latest_version_->data();
        std::lock_guard<std::mutex> l(m_);
        const uint64_t to_ver_id = latest_version_->ver_id();
        build_prece_from_w_txn_(*latest_version_, txn, to_ver_id, PreceType::WR);
        latest_version_->add_r_txn(txn.shared_from_this());
        return latest_version_->data();
    }

    bool Prewrite(Data data, Txn& txn)
    {
        return true;
        std::lock_guard<std::mutex> l(m_);
        const uint64_t to_ver_id = (++cur_ver_id_);
        const auto pre_version = std::exchange(latest_version_,
                std::make_shared<Ver>(txn.shared_from_this(), std::move(data),
                    to_ver_id));
        build_prece_from_w_txn_(*pre_version, txn, to_ver_id, PreceType::WW);
        build_preces_from_r_txns_(*pre_version, txn, to_ver_id, PreceType::RW);
        // If transaction writes multiple versions for the same row, only record the first pre_version
        txn.pre_versions_.emplace(row_id_, std::move(pre_version));
        return true;
    }

    void Write(Data /*data*/, Txn& txn)
    {
        // do nothing
    }

    void Revoke(Data /*data*/, Txn& txn)
    {
        return;
        std::lock_guard<std::mutex> l(m_);
        const auto it = txn.pre_versions_.find(row_id_);
        assert(it != txn.pre_versions_.end());
        latest_version_ = it->second; // revoke version
    }

  private:
    void build_prece_from_w_txn_(Ver& version, Txn& to_txn,
            const uint64_t to_ver_id, const PreceType type) const
    {
        if (const std::shared_ptr<Txn> from_txn = version.w_txn()) {
            build_prece(*from_txn, to_txn, version.ver_id(), to_ver_id, type);
        }
    }

    void build_preces_from_r_txns_(Ver& version, Txn& to_txn,
            const uint64_t to_ver_id, const PreceType type) const
    {
        version.foreach_r_txn([&to_txn, to_ver_id, from_ver_id = version.ver_id(), this, type](Txn& from_txn) {
            build_prece(from_txn, to_txn, from_ver_id, to_ver_id, type);
        });
    }

    void build_prece(Txn& from_txn, Txn& to_txn, const uint64_t from_ver_id,
            const uint64_t to_ver_id, const PreceType type) const
    {
      from_txn.AddToTxn(to_txn, row_id_, from_ver_id, to_ver_id, type);
    }

  private:
    const uint64_t row_id_;
    std::mutex m_;
    uint64_t cur_ver_id_;
    std::shared_ptr<Ver> latest_version_;
};

}
