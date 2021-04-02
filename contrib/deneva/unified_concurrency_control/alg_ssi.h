
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

#include "dli_identify_util.h"
#include "row_prece.h"
#include <mutex>
#include <type_traits>
#include <list>
#include <algorithm>
#include <deque>
#include "row_ssi.h"

namespace ttts {

template <UniAlgs ALG, typename Data>
class AlgManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_SSI>>
{
  public:
    using Txn = TxnManager<ALG, Data>;

    bool Validate(Txn& txn)
    {
        return !txn.node_->WConf() || !txn.node_->RConf();
    }

    void Commit(Txn& txn, const uint64_t commit_ts)
    {
        txn.node_->Commit(commit_ts);
    }

    void Abort(Txn& txn)
    {
        txn.node_->Abort();
    }
};


}
