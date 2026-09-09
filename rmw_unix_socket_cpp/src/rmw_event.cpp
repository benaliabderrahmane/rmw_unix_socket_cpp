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

#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/event.h"
#include "rmw/rmw.h"

extern "C"
{

rmw_ret_t rmw_publisher_event_init(
  rmw_event_t * rmw_event,
  const rmw_publisher_t * publisher,
  rmw_event_type_t event_type)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(rmw_event, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(publisher, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    publisher, publisher->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  if (!rmw_event_type_is_supported(event_type)) {
    RMW_SET_ERROR_MSG("event type not supported");
    return RMW_RET_UNSUPPORTED;
  }
  // Initialize the event struct so rcl can add it to a wait set without crashing.
  // We don't actually generate events, but the struct must be valid.
  rmw_event->implementation_identifier = rmw_uds::identifier;
  rmw_event->data = publisher->data;
  rmw_event->event_type = event_type;
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_event_init(
  rmw_event_t * rmw_event,
  const rmw_subscription_t * subscription,
  rmw_event_type_t event_type)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(rmw_event, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(subscription, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    subscription, subscription->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  if (!rmw_event_type_is_supported(event_type)) {
    RMW_SET_ERROR_MSG("event type not supported");
    return RMW_RET_UNSUPPORTED;
  }
  rmw_event->implementation_identifier = rmw_uds::identifier;
  rmw_event->data = subscription->data;
  rmw_event->event_type = event_type;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_event(
  const rmw_event_t * event_handle,
  void * event_info,
  bool * taken)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(event_handle, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    event_handle, event_handle->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  // No event type is supported, so there is never anything to take. Leave
  // `event_info` untouched: a taker may only write into the caller's status
  // struct when it reports `taken`, and rcl reuses that buffer across calls.
  (void)event_info;
  *taken = false;
  return RMW_RET_OK;
}

rmw_ret_t rmw_event_fini(rmw_event_t * event)
{
  // Left unvalidated on purpose. rcl_event_fini skips this call entirely when
  // event->impl is NULL, which is exactly what rcl_*_event_init leaves behind
  // after it frees impl on our RMW_RET_UNSUPPORTED - so a rejected init never
  // reaches here. There is nothing to release either: rmw_event_t::data
  // aliases the endpoint's impl struct, owned by rmw_destroy_publisher /
  // rmw_destroy_subscription.
  (void)event;
  return RMW_RET_OK;
}

bool rmw_event_type_is_supported(rmw_event_type_t event_type)
{
  // No QoS status event is reported. matched, incompatible-QoS and
  // incompatible-type would need cross-process endpoint diffing the registry
  // generation counter does not provide, and deadline and liveliness need a
  // periodic timer there is no thread to run (see DESIGN.md, "No background
  // threads, and why"). Reporting false here is what makes *_event_init reject
  // the type, which is the only refusal rclcpp handles cleanly.
  (void)event_type;
  return false;
}

rmw_ret_t rmw_event_set_callback(
  rmw_event_t * event,
  rmw_event_callback_t callback,
  const void * user_data)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(event, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    event, event->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  (void)callback;
  (void)user_data;

  // Unreachable through rclcpp, which cannot hold an initialized event handle
  // now that *_event_init rejects every type. Set an error message anyway:
  // returning a failure without one is what made the original EventsExecutor
  // crash unreadable ("failed to set the on new message callback for Event:
  // error not set").
  RMW_SET_ERROR_MSG("event callbacks are not supported: no event type is reported");
  return RMW_RET_UNSUPPORTED;
}

}  // extern "C"
