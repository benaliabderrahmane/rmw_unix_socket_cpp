#include "identifier.hpp"
#include "types.hpp"

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
  (void)event_handle;
  (void)event_info;
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);
  *taken = false;
  return RMW_RET_OK;
}

rmw_ret_t rmw_event_fini(rmw_event_t * event)
{
  (void)event;
  return RMW_RET_OK;
}

bool rmw_event_type_is_supported(rmw_event_type_t event_type)
{
  (void)event_type;
  return false;
}

rmw_ret_t rmw_event_set_callback(
  rmw_event_t * event,
  rmw_event_callback_t callback,
  const void * user_data)
{
  (void)event;
  (void)callback;
  (void)user_data;
  return RMW_RET_UNSUPPORTED;
}

}  // extern "C"
