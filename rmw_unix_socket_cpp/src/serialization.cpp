// Serialization using fastcdr + rosidl_typesupport_fastrtps generated callbacks.
// This approach uses compiled per-message-type serialization code, avoiding
// runtime introspection walking which is fragile with C/C++ ABI differences.
// Based on the approach used by rmw_zenoh_cpp.

#include "serialization.hpp"

#include <cstring>

#include "fastcdr/Cdr.h"
#include "fastcdr/FastBuffer.h"

#include "rcutils/error_handling.h"
#include "rosidl_typesupport_fastrtps_c/identifier.h"
#include "rosidl_typesupport_fastrtps_cpp/identifier.hpp"
#include "rosidl_typesupport_fastrtps_cpp/message_type_support.h"
#include "rosidl_typesupport_fastrtps_cpp/service_type_support.h"

namespace rmw_uds
{

const message_type_support_callbacks_t *
get_callbacks(const rosidl_message_type_support_t * type_support)
{
  // Try C++ fastrtps type support first
  auto ts = get_message_typesupport_handle(
    type_support,
    rosidl_typesupport_fastrtps_cpp::typesupport_identifier);
  if (ts) {
    return static_cast<const message_type_support_callbacks_t *>(ts->data);
  }
  rcutils_reset_error();

  // Fall back to C fastrtps type support
  ts = get_message_typesupport_handle(
    type_support,
    rosidl_typesupport_fastrtps_c__identifier);
  if (ts) {
    return static_cast<const message_type_support_callbacks_t *>(ts->data);
  }
  rcutils_reset_error();
  return nullptr;
}

ServiceCallbacks get_service_callbacks(
  const rosidl_service_type_support_t * type_support)
{
  ServiceCallbacks result;

  // Try C++ first
  auto ts = get_service_typesupport_handle(
    type_support,
    rosidl_typesupport_fastrtps_cpp::typesupport_identifier);
  if (ts) {
    auto * srv_cb = static_cast<const service_type_support_callbacks_t *>(ts->data);
    // Resolve request/response through get_message_typesupport_handle
    // (srv_cb->request_members_ may be a dispatch trampoline, not the actual callbacks)
    auto req_ts = get_message_typesupport_handle(
      srv_cb->request_members_,
      rosidl_typesupport_fastrtps_cpp::typesupport_identifier);
    auto resp_ts = get_message_typesupport_handle(
      srv_cb->response_members_,
      rosidl_typesupport_fastrtps_cpp::typesupport_identifier);
    if (req_ts && resp_ts) {
      result.request = static_cast<const message_type_support_callbacks_t *>(req_ts->data);
      result.response = static_cast<const message_type_support_callbacks_t *>(resp_ts->data);
      result.service_namespace = srv_cb->service_namespace_;
      result.service_name = srv_cb->service_name_;
      return result;
    }
    rcutils_reset_error();
  }
  rcutils_reset_error();

  // Fall back to C
  ts = get_service_typesupport_handle(
    type_support,
    rosidl_typesupport_fastrtps_c__identifier);
  if (ts) {
    auto * srv_cb = static_cast<const service_type_support_callbacks_t *>(ts->data);
    auto req_ts = get_message_typesupport_handle(
      srv_cb->request_members_,
      rosidl_typesupport_fastrtps_c__identifier);
    auto resp_ts = get_message_typesupport_handle(
      srv_cb->response_members_,
      rosidl_typesupport_fastrtps_c__identifier);
    if (req_ts && resp_ts) {
      result.request = static_cast<const message_type_support_callbacks_t *>(req_ts->data);
      result.response = static_cast<const message_type_support_callbacks_t *>(resp_ts->data);
      result.service_namespace = srv_cb->service_namespace_;
      result.service_name = srv_cb->service_name_;
      return result;
    }
    rcutils_reset_error();
  }
  rcutils_reset_error();
  return result;
}

bool serialize(
  const void * ros_message,
  const message_type_support_callbacks_t * callbacks,
  std::vector<uint8_t> & buffer)
{
  // Get estimated serialized size (4 bytes encapsulation + message)
  auto data_length = 4 + callbacks->get_serialized_size(ros_message);
  buffer.resize(data_length);

  eprosima::fastcdr::FastBuffer fastbuffer(
    reinterpret_cast<char *>(buffer.data()), data_length);
  eprosima::fastcdr::Cdr ser(
    fastbuffer,
    eprosima::fastcdr::Cdr::DEFAULT_ENDIAN,
    eprosima::fastcdr::CdrVersion::DDS_CDR);

  ser.serialize_encapsulation();
  if (!callbacks->cdr_serialize(ros_message, ser)) {
    return false;
  }

  // Trim buffer to actual serialized size
  buffer.resize(ser.get_serialized_data_length());
  return true;
}

bool deserialize(
  const uint8_t * data,
  size_t length,
  const message_type_support_callbacks_t * callbacks,
  void * ros_message)
{
  eprosima::fastcdr::FastBuffer fastbuffer(
    reinterpret_cast<char *>(const_cast<uint8_t *>(data)), length);
  eprosima::fastcdr::Cdr deser(
    fastbuffer,
    eprosima::fastcdr::Cdr::DEFAULT_ENDIAN,
    eprosima::fastcdr::CdrVersion::DDS_CDR);

  try {
    deser.read_encapsulation();
    if (!callbacks->cdr_deserialize(deser, ros_message)) {
      return false;
    }
  } catch (const eprosima::fastcdr::exception::Exception &) {
    return false;
  }
  return true;
}

}  // namespace rmw_uds
