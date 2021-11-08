/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: williamcliu@tencent.com
 *
 */

#ifdef ENUM_BEGIN
#ifdef ENUM_MEMBER
#ifdef ENUM_END

ENUM_BEGIN(UniAlgs)
ENUM_MEMBER(UniAlgs, UNI_DLI_IDENTIFY_CHAIN)
ENUM_MEMBER(UniAlgs, UNI_DLI_IDENTIFY_CYCLE)
ENUM_MEMBER(UniAlgs, UNI_DLI_IDENTIFY_SSI)
ENUM_END(UniAlgs)

#endif
#endif
#endif

#ifndef TTTS_DENEVA_UNI_H_
#define TTTS_DENEVA_UNI_H_

namespace ttts {

#define ENUM_FILE "../cca/unified_history_algorithm/util/util.h"
#include "../../../util/extend_enum.h"

// Each history has only one AlgManager which stores global infomation for algorithm.
template <UniAlgs ALG, typename Data, typename T = void> class AlgManager;

// Each row or variable has a RowManager.
template <UniAlgs ALG, typename Data, typename T = void> class RowManager;

// Each transaction has a TxnManager. We recommend that enable shared_from_this for TxnManager and make constructor private to prevent misused shared_from_this().
//
// A TxnManager can be released only when no more transactions build precedence before it. In this case, the transaction cannot be a part of cycle anymore.
//
// For latest read, it should satisfies:
// (1) The transaction is already finished (for latest read, no future transactions build RW precedence before it).
// (2) There are no transactions built precedence before it.
// We use std::shared_ptr to realize it. When the pointer expired, the two conditions are satisified.
//
// For snapshot read, it should also satisfies:
// (3) Minimum active transaction snapshot timestamp (start timestamp) > this transaction's largest write timestamp (commit timestamp). (no future transactions build RW precedence before it)
template <UniAlgs ALG, typename Data, typename T = void> class TxnManager;

// Each version has a VerManager. (nonessential)
//
// A VerManager can be released when it will not be read anymore.
//
// For latest read, it should satisfies:
// (1) It is not the latest version.
// (2) The later version's write transaction is finished. (to prevent version revoke)
//
// For snapshot read, it should also satisfies:
// (3) Minimum active transaction snapshot timestamp (start timestamp) > the later version's write timestamp.
template <UniAlgs ALG, typename Data, typename T = void> class VerManager;

}

#endif // TTTS_DENEVA_UNI_ALGS_H_
