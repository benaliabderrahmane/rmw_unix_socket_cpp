// Copyright 2026 Abderahmane BENALI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RMW_UNIX_SOCKET_CPP__SERIALIZATION_HPP_
#define RMW_UNIX_SOCKET_CPP__SERIALIZATION_HPP_

#include <cstdint>
#include <vector>

#include "rosidl_runtime_c/message_type_support_struct.h"
#include "rosidl_runtime_c/service_type_support_struct.h"
#include "rosidl_typesupport_fastrtps_cpp/message_type_support.h"

namespace rmw_uds
{

// Resolve type support to fastrtps callbacks for CDR serialization.
// Returns nullptr on failure.
const message_type_support_callbacks_t *
get_callbacks(const rosidl_message_type_support_t * type_support);

// Resolve service type support to fastrtps callbacks.
struct ServiceCallbacks
{
  const message_type_support_callbacks_t * request = nullptr;
  const message_type_support_callbacks_t * response = nullptr;
  const char * service_namespace = nullptr;
  const char * service_name = nullptr;
};

ServiceCallbacks get_service_callbacks(
  const rosidl_service_type_support_t * type_support);

// Serialize a ROS message into a byte buffer using CDR (fastcdr).
bool serialize(
  const void * ros_message,
  const message_type_support_callbacks_t * callbacks,
  std::vector<uint8_t> & buffer);

// Serialized size of a message (4-byte CDR encapsulation included) without
// serializing it. Exact or a small upper bound — serialize_into's actual
// length can only be <= this. False if the size walk throws (broken message).
bool serialized_size(
  const void * ros_message,
  const message_type_support_callbacks_t * callbacks,
  size_t & size_out);

// Serialize a ROS message directly into caller-provided memory of `capacity`
// bytes (e.g. a reserved shm ring record) — same bytes as serialize(), minus
// the intermediate heap buffer. On success `actual_out` is the real serialized
// length (<= capacity). False on serializer failure or capacity overflow; the
// buffer contents are then undefined and must not be published.
bool serialize_into(
  const void * ros_message,
  const message_type_support_callbacks_t * callbacks,
  uint8_t * buffer,
  size_t capacity,
  size_t & actual_out);

// Deserialize a byte buffer into a ROS message using CDR (fastcdr).
bool deserialize(
  const uint8_t * data,
  size_t length,
  const message_type_support_callbacks_t * callbacks,
  void * ros_message);

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__SERIALIZATION_HPP_
