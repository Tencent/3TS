/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: williamcliu@tencent.com
 *
 */

#ifndef _ROW_UNIFIED_UTIL_H_
#define _ROW_UNIFIED_UTIL_H_

#include "../unified_concurrency_control/row_prece.h"
#include "../unified_concurrency_control/alg_dli_identify_cycle.h"
#include "../unified_concurrency_control/alg_dli_identify_chain.h"
#include "../unified_concurrency_control/txn_dli_identify.h"

class row_t;

template <int ALG> constexpr const ttts::UniAlgs uni_alg;

template <> constexpr const ttts::UniAlgs uni_alg<DLI_IDENTIFY> = ttts::UniAlgs::UNI_DLI_IDENTIFY_CYCLE;
template <> constexpr const ttts::UniAlgs uni_alg<DLI_IDENTIFY_2> = ttts::UniAlgs::UNI_DLI_IDENTIFY_CHAIN;

template <int ALG> using UniRowManager = ttts::RowManager<uni_alg<ALG>, row_t*>;
template <int ALG> using UniTxnManager = ttts::TxnManager<uni_alg<ALG>, row_t*>;
template <int ALG> using UniAlgManager = ttts::AlgManager<uni_alg<ALG>, row_t*>;

#endif
