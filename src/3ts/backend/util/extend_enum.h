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

#include <type_traits>


#ifdef ENUM_FILE

template <typename EnumType, typename = typename std::enable_if_t<std::is_enum_v<EnumType>>> constexpr uint32_t Count();
template <typename EnumType, typename = typename std::enable_if_t<std::is_enum_v<EnumType>>> const char* ToString(const EnumType e);
template <typename EnumType, typename = typename std::enable_if_t<std::is_enum_v<EnumType>>> std::ostream& operator<<(std::ostream& os, const EnumType e);
template <typename EnumType, typename = typename std::enable_if_t<std::is_enum_v<EnumType>>> const std::array<EnumType, Count<EnumType>()>& Members();

#define ENUM_BEGIN(name) enum class name : uint32_t {
#define ENUM_MEMBER(_, member) member,
#define ENUM_END(name) name##_MAX }; template <> constexpr uint32_t Count<name>() { return static_cast<uint32_t>(name::name##_MAX); }

#include ENUM_FILE

#undef ENUM_BEGIN
#undef ENUM_MEMBER
#undef ENUM_END

#define ENUM_BEGIN(name)\
template <> const char* ToString<name>(const name e)\
{\
  static std::vector<const char*> strings {
#define ENUM_MEMBER(_, member) #member,
#define ENUM_END(name)\
  };\
  return strings.at(static_cast<uint32_t>(e));\
}\
template <> std::ostream& operator<<<name>(std::ostream& os, const name e) { return os << ToString(e); }

#include ENUM_FILE

#undef ENUM_BEGIN
#undef ENUM_MEMBER
#undef ENUM_END

#define ENUM_BEGIN(name)\
template <> const std::array<name, Count<name>()>& Members()\
{\
  static const std::array<name, Count<name>()> members {
#define ENUM_MEMBER(name, member) name::member,
#define ENUM_END(_)\
  };\
  return members;\
}

#include ENUM_FILE

#undef ENUM_BEGIN
#undef ENUM_MEMBER
#undef ENUM_END
#endif
