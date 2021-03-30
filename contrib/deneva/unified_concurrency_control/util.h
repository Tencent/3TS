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

namespace ttts {

enum class UniAlgs
{
    UNI_DLI_IDENTIFY_CYCLE,
    UNI_DLI_IDENTIFY_MERGE,
};

template <UniAlgs ALG, typename Data, typename T = void> class AlgManager;
template <UniAlgs ALG, typename Data, typename T = void> class RowManager;
template <UniAlgs ALG, typename Data, typename T = void> class TxnManager;

}
