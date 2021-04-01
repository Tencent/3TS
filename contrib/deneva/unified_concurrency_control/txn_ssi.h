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

namespace ttts {


template <UniAlgs ALG, typename Data>
class TxnManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_SSI>>
{
  public:
    enum class State { ACTIVE, PREPARING, COMMITTED, ABORTED };

    TxnManager(const uint64_t txn_id, const uint64_t start_time) : node_(std::make_shared<TxnNode>(txn_id)) {}

  private:
    std::shared_ptr<TxnNode<ALG, Data>> node_;
};

}
