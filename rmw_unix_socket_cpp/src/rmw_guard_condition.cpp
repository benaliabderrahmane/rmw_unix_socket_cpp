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

#include "identifier.hpp"
#include "types.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include "rmw/allocators.h"
#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/rmw.h"

extern "C"
{

rmw_guard_condition_t * rmw_create_guard_condition(rmw_context_t * context)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(context, nullptr);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    context, context->implementation_identifier,
    rmw_uds::identifier, return nullptr);

  auto * gc_data = new (std::nothrow) rmw_uds::UdsGuardCondition();
  if (!gc_data) {
    RMW_SET_ERROR_MSG("failed to allocate guard condition data");
    return nullptr;
  }

  gc_data->eventfd_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (gc_data->eventfd_fd < 0) {
    delete gc_data;
    RMW_SET_ERROR_MSG("failed to create eventfd");
    return nullptr;
  }

  auto * gc = rmw_guard_condition_allocate();
  if (!gc) {
    close(gc_data->eventfd_fd);
    delete gc_data;
    RMW_SET_ERROR_MSG("failed to allocate guard condition");
    return nullptr;
  }

  gc->implementation_identifier = rmw_uds::identifier;
  gc->data = gc_data;
  gc->context = context;
  return gc;
}

rmw_ret_t rmw_destroy_guard_condition(rmw_guard_condition_t * guard_condition)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(guard_condition, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    guard_condition, guard_condition->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * gc_data = static_cast<rmw_uds::UdsGuardCondition *>(guard_condition->data);
  if (gc_data) {
    if (gc_data->eventfd_fd >= 0) {
      close(gc_data->eventfd_fd);
    }
    delete gc_data;
  }

  rmw_guard_condition_free(guard_condition);
  return RMW_RET_OK;
}

rmw_ret_t rmw_trigger_guard_condition(
  const rmw_guard_condition_t * guard_condition)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(guard_condition, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    guard_condition, guard_condition->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * gc_data = static_cast<rmw_uds::UdsGuardCondition *>(guard_condition->data);
  if (!gc_data || gc_data->eventfd_fd < 0) {
    return RMW_RET_INVALID_ARGUMENT;
  }
  uint64_t val = 1;
  ssize_t ret = write(gc_data->eventfd_fd, &val, sizeof(val));
  (void)ret;
  return RMW_RET_OK;
}

}  // extern "C"
