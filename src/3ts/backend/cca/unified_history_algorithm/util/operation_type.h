#ifdef ENUM_BEGIN
#ifdef ENUM_MEMBER
#ifdef ENUM_END

ENUM_BEGIN(OperationType)
ENUM_MEMBER(OperationType, W)
ENUM_MEMBER(OperationType, R)
ENUM_MEMBER(OperationType, C)
ENUM_MEMBER(OperationType, A)
ENUM_END(OperationType)

#endif
#endif
#endif

#ifndef TTTS_DENEVA_OPERATION_TYPE_H_
#define TTTS_DENEVA_OPERATION_TYPE_H_

namespace ttts {
#define ENUM_FILE "../cca/unified_history_algorithm/util/operation_type.h"
#include "../../../util/extend_enum.h"
}

#endif // TTTS_DENEVA_OPERATION_TYPE_H_
