#ifdef ENUM_BEGIN
#ifdef ENUM_MEMBER
#ifdef ENUM_END

ENUM_BEGIN(AnomalyType)
// ======== WAT - 1 =========
ENUM_MEMBER(AnomalyType, WAT_1_DIRTY_WRITE)
ENUM_MEMBER(AnomalyType, WAT_1_FULL_WRITE)
ENUM_MEMBER(AnomalyType, WAT_1_LOST_SELF_UPDATE)
ENUM_MEMBER(AnomalyType, WAT_1_LOST_UPDATE)
// ======== WAT - 2 =========
ENUM_MEMBER(AnomalyType, WAT_2_DOUBLE_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, WAT_2_DOUBLE_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, WAT_2_READ_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, WAT_2_READ_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, WAT_2_FULL_WRITE_SKEW)
// ======== WAT - 3 =========
ENUM_MEMBER(AnomalyType, WAT_STEP)
// ======== RAT - 1 =========
ENUM_MEMBER(AnomalyType, RAT_1_DIRTY_READ)
ENUM_MEMBER(AnomalyType, RAT_1_INTERMEDIATE_READ)
ENUM_MEMBER(AnomalyType, RAT_1_NON_REPEATABLE_READ)
// ======== RAT - 2 =========
ENUM_MEMBER(AnomalyType, RAT_2_WRITE_READ_SKEW)
ENUM_MEMBER(AnomalyType, RAT_2_DOUBLE_WRITE_SKEW_COMMITTED)
ENUM_MEMBER(AnomalyType, RAT_2_READ_SKEW)
ENUM_MEMBER(AnomalyType, RAT_2_READ_SKEW_2)
// ======== RAT - 3 =========
ENUM_MEMBER(AnomalyType, RAT_STEP)
// ======== IAT - 1 =========
ENUM_MEMBER(AnomalyType, IAT_1_LOST_UPDATE_COMMITTED)
// ======== IAT - 2 =========
ENUM_MEMBER(AnomalyType, IAT_2_READ_WRITE_SKEW_COMMITTED)
ENUM_MEMBER(AnomalyType, IAT_2_WRITE_SKEW)
// ======== IAT - 3 =========
ENUM_MEMBER(AnomalyType, IAT_STEP)
// ======== Unknown =========
ENUM_MEMBER(AnomalyType, UNKNOWN_1)
ENUM_MEMBER(AnomalyType, UNKNOWN_2)
ENUM_END(AnomalyType)

#endif
#endif
#endif

#ifndef TTTS_CCA_ANOMALY_TYPE_H_
#define TTTS_CCA_ANOMALY_TYPE_H_

namespace ttts {
#define ENUM_FILE "../cca/anomaly_type.h"
#include "../util/extend_enum.h"
}

#endif // TTTS_CCA_ANOMALY_TYPE_H_
