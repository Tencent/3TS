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
