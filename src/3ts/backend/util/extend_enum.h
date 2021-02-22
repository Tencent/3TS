/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: williamcliu@tencent.com
 *
 */

#ifdef ENUM_FILE

#define ENUM_BEGIN(name) enum class name : uint32_t {
#define ENUM_MEMBER(_, member) member,
#define ENUM_END(name) name##_MAX }; constexpr inline uint32_t name##Count = static_cast<uint32_t>(name::name##_MAX);

#include ENUM_FILE

#undef ENUM_BEGIN
#undef ENUM_MEMBER
#undef ENUM_END

#define ENUM_BEGIN(name)\
inline const char* ToString(const name e);\
inline std::ostream& operator<<(std::ostream& os, const name e) { return os << ToString(e); }\
inline const char* ToString(const name e)\
{\
  static std::vector<const char*> strings {
#define ENUM_MEMBER(_, member) #member,
#define ENUM_END(_)\
  };\
  return strings.at(static_cast<uint32_t>(e));\
}

#include ENUM_FILE

#undef ENUM_BEGIN
#undef ENUM_MEMBER
#undef ENUM_END

#define ENUM_BEGIN(name) const inline std::array<name, name##Count> All##name {
#define ENUM_MEMBER(name, member) name::member,
#define ENUM_END(_) };

#include ENUM_FILE

#undef ENUM_BEGIN
#undef ENUM_MEMBER
#undef ENUM_END
#endif
