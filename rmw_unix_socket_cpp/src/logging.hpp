#ifndef RMW_UNIX_SOCKET_CPP__LOGGING_HPP_
#define RMW_UNIX_SOCKET_CPP__LOGGING_HPP_

#include "rcutils/logging_macros.h"
#include "rcutils/time.h"

// Logger name as it appears in ROS log output. Match the package name so
// users can filter with `--log-level rmw_unix_socket_cpp:=...` the same way
// they would for any other rcl/rclcpp logger.
#define RMW_UDS_LOG_NAME "rmw_unix_socket_cpp"

#define RMW_UDS_LOG_DEBUG(...) \
  RCUTILS_LOG_DEBUG_NAMED(RMW_UDS_LOG_NAME, __VA_ARGS__)
#define RMW_UDS_LOG_INFO(...) \
  RCUTILS_LOG_INFO_NAMED(RMW_UDS_LOG_NAME, __VA_ARGS__)
#define RMW_UDS_LOG_WARN(...) \
  RCUTILS_LOG_WARN_NAMED(RMW_UDS_LOG_NAME, __VA_ARGS__)
#define RMW_UDS_LOG_ERROR(...) \
  RCUTILS_LOG_ERROR_NAMED(RMW_UDS_LOG_NAME, __VA_ARGS__)

// Throttled variants. Use these on hot paths (per-message events) so a
// persistently-failing peer can't flood the log. `period_ms` is the minimum
// gap between two prints from the same call site.
#define RMW_UDS_LOG_WARN_THROTTLE(period_ms, ...) \
  RCUTILS_LOG_WARN_THROTTLE_NAMED( \
    RCUTILS_STEADY_TIME, period_ms, RMW_UDS_LOG_NAME, __VA_ARGS__)
#define RMW_UDS_LOG_ERROR_THROTTLE(period_ms, ...) \
  RCUTILS_LOG_ERROR_THROTTLE_NAMED( \
    RCUTILS_STEADY_TIME, period_ms, RMW_UDS_LOG_NAME, __VA_ARGS__)
#define RMW_UDS_LOG_INFO_THROTTLE(period_ms, ...) \
  RCUTILS_LOG_INFO_THROTTLE_NAMED( \
    RCUTILS_STEADY_TIME, period_ms, RMW_UDS_LOG_NAME, __VA_ARGS__)

#endif  // RMW_UNIX_SOCKET_CPP__LOGGING_HPP_
