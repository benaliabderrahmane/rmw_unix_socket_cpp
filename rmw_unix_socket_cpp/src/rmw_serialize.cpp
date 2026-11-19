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

#include "serialization.hpp"

#include <cstring>

#include "rmw/error_handling.h"
#include "rmw/rmw.h"

extern "C"
{

rmw_ret_t rmw_serialize(
  const void * ros_message,
  const rosidl_message_type_support_t * type_support,
  rmw_serialized_message_t * serialized_message)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(ros_message, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(type_support, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(serialized_message, RMW_RET_INVALID_ARGUMENT);

  const auto * callbacks = rmw_uds::get_callbacks(type_support);
  if (!callbacks) {
    RMW_SET_ERROR_MSG("failed to get fastrtps type support callbacks");
    return RMW_RET_ERROR;
  }

  std::vector<uint8_t> buffer;
  if (!rmw_uds::serialize(ros_message, callbacks, buffer)) {
    RMW_SET_ERROR_MSG("serialization failed");
    return RMW_RET_ERROR;
  }

  auto ret = rmw_serialized_message_resize(serialized_message, buffer.size());
  if (ret != RMW_RET_OK) {return ret;}
  std::memcpy(serialized_message->buffer, buffer.data(), buffer.size());
  serialized_message->buffer_length = buffer.size();

  return RMW_RET_OK;
}

rmw_ret_t rmw_deserialize(
  const rmw_serialized_message_t * serialized_message,
  const rosidl_message_type_support_t * type_support,
  void * ros_message)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(serialized_message, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(type_support, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(ros_message, RMW_RET_INVALID_ARGUMENT);

  const auto * callbacks = rmw_uds::get_callbacks(type_support);
  if (!callbacks) {
    RMW_SET_ERROR_MSG("failed to get fastrtps type support callbacks");
    return RMW_RET_ERROR;
  }

  if (!rmw_uds::deserialize(
      serialized_message->buffer,
      serialized_message->buffer_length,
      callbacks, ros_message))
  {
    RMW_SET_ERROR_MSG("deserialization failed");
    return RMW_RET_ERROR;
  }

  return RMW_RET_OK;
}

}  // extern "C"
