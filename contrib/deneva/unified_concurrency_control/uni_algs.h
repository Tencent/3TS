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

#pragma once

namespace ttts {
#define ENUM_FILE "../../../../contrib/deneva/unified_concurrency_control/uni_algs.h"
#include "../../../src/3ts/backend/util/extend_enum.h"
}
