#ifdef ENUM_BEGIN
#ifdef ENUM_MEMBER
#ifdef ENUM_END

ENUM_BEGIN(PreceType)
ENUM_MEMBER(PreceType, RW)
ENUM_MEMBER(PreceType, WR)
ENUM_MEMBER(PreceType, WCR)
ENUM_MEMBER(PreceType, WW)
ENUM_MEMBER(PreceType, WCW)
ENUM_MEMBER(PreceType, RA)
ENUM_MEMBER(PreceType, WC)
ENUM_MEMBER(PreceType, WA)
ENUM_END(PreceType)

#endif
#endif
#endif

#ifndef TTTS_CCA_PRECE_TYPE_H_
#define TTTS_CCA_PRECE_TYPE_H_

namespace ttts {
#define ENUM_FILE "../cca/unified_history_algorithm/util/prece_type.h"
#include "../../../util/extend_enum.h"
}

#endif // TTTS_CCA_PRECE_TYPE_H_
