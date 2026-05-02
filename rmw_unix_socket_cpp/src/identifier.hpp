#ifndef RMW_UNIX_SOCKET_CPP__IDENTIFIER_HPP_
#define RMW_UNIX_SOCKET_CPP__IDENTIFIER_HPP_

namespace rmw_uds
{
// Must be char[] (not const char*) so that `inline` guarantees a single
// address across all translation units.  The RMW framework compares
// implementation_identifier by pointer, not by string value.
inline constexpr char identifier[] = "rmw_unix_socket_cpp";
inline constexpr char serialization_format[] = "uds_introspection";
}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__IDENTIFIER_HPP_
