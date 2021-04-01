#ifdef ENUM_FILE

template <typename EnumType, typename = typename std::enable_if_t<std::is_enum_v<EnumType>>> inline constexpr uint32_t Count();
template <typename EnumType, typename = typename std::enable_if_t<std::is_enum_v<EnumType>>> inline const char* ToString(const EnumType e);
template <typename EnumType, typename = typename std::enable_if_t<std::is_enum_v<EnumType>>> inline const std::array<EnumType, Count<EnumType>()>& Members();

#define ENUM_BEGIN(name) enum class name : uint32_t {
#define ENUM_MEMBER(_, member) member,
#define ENUM_END(name) name##_MAX }; template <> constexpr uint32_t Count<name>() { return static_cast<uint32_t>(name::name##_MAX); }

#include ENUM_FILE

#undef ENUM_BEGIN
#undef ENUM_MEMBER
#undef ENUM_END

#define ENUM_BEGIN(name)\
template <> inline const char* ToString<name>(const name e)\
{\
  static std::array<const char*, Count<name>()> strings {
#define ENUM_MEMBER(_, member) #member,
#define ENUM_END(name)\
  };\
  return strings.at(static_cast<uint32_t>(e));\
}\

#include ENUM_FILE

#undef ENUM_BEGIN
#undef ENUM_MEMBER
#undef ENUM_END

#define ENUM_BEGIN(name)\
template <> inline const std::array<name, Count<name>()>& Members()\
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

#undef ENUM_FILE
#endif

#ifndef EXTEND_ENUM_H_
#define EXTEND_ENUM_H_

template <typename EnumType, typename = typename std::enable_if_t<std::is_enum_v<EnumType>>> inline std::ostream& operator<<(std::ostream& os, const EnumType e) { return os << ToString(e); }

#endif
