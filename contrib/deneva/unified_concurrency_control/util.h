/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 Tencent.  All rights reserved.
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
ENUM_MEMBER(UniAlgs, ALL)
ENUM_END(UniAlgs)

#endif
#endif
#endif

#ifndef TTTS_DENEVA_UNI_H_
#define TTTS_DENEVA_UNI_H_

namespace ttts {

#define ENUM_FILE "../../../../contrib/deneva/unified_concurrency_control/util.h"
#include "../../../src/3ts/backend/util/extend_enum.h"

template <UniAlgs ALG, typename Data, typename T = void> class AlgManager;
template <UniAlgs ALG, typename Data, typename T = void> class RowManager;
template <UniAlgs ALG, typename Data, typename T = void> class TxnManager;

}

#endif // TTTS_DENEVA_UNI_ALGS_H_
