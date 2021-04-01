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

#pragma once

namespace ttts {
#define ENUM_FILE "../../../../contrib/deneva/unified_concurrency_control/operation_type.h"
#include "../../../src/3ts/backend/util/extend_enum.h"
}
