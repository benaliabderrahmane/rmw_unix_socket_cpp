#include "identifier.hpp"

#include <cstring>

#include "rmw/error_handling.h"
#include "rmw/rmw.h"
#include "rmw/dynamic_message_type_support.h"
#include "rmw/get_network_flow_endpoints.h"

extern "C"
{

// --- Loaned messages ---
// Cannot implement: Unix sockets copy data through kernel buffers.
// Zero-copy requires shared memory (iceoryx). Return UNSUPPORTED per RMW spec.

rmw_ret_t rmw_borrow_loaned_message(
  const rmw_publisher_t * publisher,
  const rosidl_message_type_support_t * type_support,
  void ** ros_message)
{
  (void)publisher; (void)type_support; (void)ros_message;
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_return_loaned_message_from_publisher(
  const rmw_publisher_t * publisher,
  void * loaned_message)
{
  (void)publisher; (void)loaned_message;
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_publish_loaned_message(
  const rmw_publisher_t * publisher,
  void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  (void)publisher; (void)ros_message; (void)allocation;
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_take_loaned_message(
  const rmw_subscription_t * subscription,
  void ** loaned_message,
  bool * taken,
  rmw_subscription_allocation_t * allocation)
{
  (void)subscription; (void)loaned_message; (void)taken; (void)allocation;
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_take_loaned_message_with_info(
  const rmw_subscription_t * subscription,
  void ** loaned_message,
  bool * taken,
  rmw_message_info_t * message_info,
  rmw_subscription_allocation_t * allocation)
{
  (void)subscription; (void)loaned_message; (void)taken;
  (void)message_info; (void)allocation;
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_return_loaned_message_from_subscription(
  const rmw_subscription_t * subscription,
  void * loaned_message)
{
  (void)subscription; (void)loaned_message;
  return RMW_RET_UNSUPPORTED;
}

// --- Publisher/Subscription allocations ---
// Our implementation doesn't use pre-allocated buffers. Succeed silently.

rmw_ret_t rmw_init_publisher_allocation(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds,
  rmw_publisher_allocation_t * allocation)
{
  (void)type_support; (void)message_bounds;
  if (allocation) {
    std::memset(allocation, 0, sizeof(*allocation));
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_fini_publisher_allocation(rmw_publisher_allocation_t * allocation)
{
  (void)allocation;
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_subscription_allocation(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds,
  rmw_subscription_allocation_t * allocation)
{
  (void)type_support; (void)message_bounds;
  if (allocation) {
    std::memset(allocation, 0, sizeof(*allocation));
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_fini_subscription_allocation(rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  return RMW_RET_OK;
}

// --- Dynamic message support ---
// Requires middleware-level type discovery (DDS Type Object).
// Not applicable to static introspection-based serialization.

rmw_ret_t rmw_take_dynamic_message(
  const rmw_subscription_t * subscription,
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message,
  bool * taken,
  rmw_subscription_allocation_t * allocation)
{
  (void)subscription; (void)dynamic_message; (void)taken; (void)allocation;
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_take_dynamic_message_with_info(
  const rmw_subscription_t * subscription,
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message,
  bool * taken,
  rmw_message_info_t * message_info,
  rmw_subscription_allocation_t * allocation)
{
  (void)subscription; (void)dynamic_message; (void)taken;
  (void)message_info; (void)allocation;
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_serialization_support_init(
  const char * serialization_lib_name,
  rcutils_allocator_t * allocator,
  rosidl_dynamic_typesupport_serialization_support_t * serialization_support)
{
  (void)serialization_lib_name; (void)allocator; (void)serialization_support;
  return RMW_RET_UNSUPPORTED;
}

// --- Network flow endpoints ---
// Not applicable for AF_UNIX sockets (no IP endpoints).

rmw_ret_t rmw_publisher_get_network_flow_endpoints(
  const rmw_publisher_t * publisher,
  rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * network_flow_endpoint_array)
{
  (void)publisher; (void)allocator; (void)network_flow_endpoint_array;
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_subscription_get_network_flow_endpoints(
  const rmw_subscription_t * subscription,
  rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * network_flow_endpoint_array)
{
  (void)subscription; (void)allocator; (void)network_flow_endpoint_array;
  return RMW_RET_UNSUPPORTED;
}

}  // extern "C"
