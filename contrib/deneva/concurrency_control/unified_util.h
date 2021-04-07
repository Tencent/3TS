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

#include "../../../src/3ts/backend/cca/unified_history_algorithm/row/row_prece.h"
#include "../../../src/3ts/backend/cca/unified_history_algorithm/row/row_ssi.h"
#include "../../../src/3ts/backend/cca/unified_history_algorithm/alg/alg_dli_identify_cycle.h"
#include "../../../src/3ts/backend/cca/unified_history_algorithm/alg/alg_dli_identify_chain.h"
#include "../../../src/3ts/backend/cca/unified_history_algorithm/alg/alg_ssi.h"
#include "../../../src/3ts/backend/cca/unified_history_algorithm/txn/txn_dli_identify.h"
#include "../../../src/3ts/backend/cca/unified_history_algorithm/txn/txn_ssi.h"

class row_t;

template <int ALG> constexpr const ttts::UniAlgs uni_alg;

template <> constexpr const ttts::UniAlgs uni_alg<DLI_IDENTIFY_CYCLE> = ttts::UniAlgs::UNI_DLI_IDENTIFY_CYCLE;
template <> constexpr const ttts::UniAlgs uni_alg<DLI_IDENTIFY_CHAIN> = ttts::UniAlgs::UNI_DLI_IDENTIFY_CHAIN;
template <> constexpr const ttts::UniAlgs uni_alg<DLI_IDENTIFY_SSI> = ttts::UniAlgs::UNI_DLI_IDENTIFY_SSI;

template <int ALG> using UniRowManager = ttts::RowManager<uni_alg<ALG>, row_t*>;
template <int ALG> using UniTxnManager = ttts::TxnManager<uni_alg<ALG>, row_t*>;
template <int ALG> using UniAlgManager = ttts::AlgManager<uni_alg<ALG>, row_t*>;

#endif
