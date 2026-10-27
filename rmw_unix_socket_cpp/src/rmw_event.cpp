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

#include <algorithm>
#include <array>
#include <cstring>
#include <mutex>
#include <set>
#include <string>

#include "events.hpp"
#include "identifier.hpp"
#include "listener.hpp"
#include "logging.hpp"
#include "registry.hpp"
#include "types.hpp"

#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/event.h"
#include "rmw/events_statuses/events_statuses.h"
#include "rmw/rmw.h"

// QoS status events. message_lost and the two matched events are reported;
// everything else is refused, because deadline and liveliness need a clock
// nothing here runs, and incompatible QoS and type would claim a delivery
// failure this transport does not have (it sends to every subscriber on the
// topic). See DESIGN.md, "QoS status events".

namespace rmw_uds
{
namespace
{

bool is_matched_type(rmw_event_type_t type)
{
  return type == RMW_EVENT_PUBLICATION_MATCHED || type == RMW_EVENT_SUBSCRIPTION_MATCHED;
}

// A matched event's state, and what it is matched against: a publisher's
// peers are the subscriptions on its topic and vice versa - exactly who a
// publish reaches (refresh_sub_paths matches on the topic alone).
struct MatchedRef
{
  std::recursive_mutex * mutex;
  UdsMatchedState * state;
  UdsContext * ctx;
  const std::string * topic;
  RegistryEntryType peer_type;
};

MatchedRef matched_of(void * endpoint, rmw_event_type_t type)
{
  if (type == RMW_EVENT_PUBLICATION_MATCHED) {
    auto * pub = static_cast<UdsPublisher *>(endpoint);
    return {&pub->event_mutex, &pub->matched, pub->context, &pub->topic_name,
      ENTRY_SUBSCRIPTION};
  }
  auto * sub = static_cast<UdsSubscription *>(endpoint);
  return {&sub->event_mutex, &sub->matched, sub->context, &sub->topic_name, ENTRY_PUBLISHER};
}

// Re-read the peers if the registry generation moved since the last
// recompute, and record the difference. The first recompute reports every
// peer that already exists, as DDS reports the matches an endpoint is created
// into. With `notify`, a change that starts an untaken batch fires the
// callback - from whichever path noticed it, so a wait that absorbs a change
// cannot leave an EventsExecutor unaware of it. Returns true when something
// changed. The caller holds *ref.mutex.
bool recompute(const MatchedRef & ref, bool notify)
{
  if (!ref.ctx || !ref.ctx->registry_ptr) {
    return false;
  }
  auto * header = registry_header(ref.ctx->registry_ptr);
  // Loaded before the query: a mutation during it leaves this generation
  // behind, so the next recompute queries again (as refresh_sub_paths does).
  const uint64_t gen = registry_generation(header);
  UdsMatchedState & st = *ref.state;
  if (st.computed && gen == st.generation) {
    return false;
  }
  std::set<std::array<uint8_t, 16>> now;
  for (const auto & r : registry_query(header, ref.peer_type, ref.topic->c_str(), nullptr, nullptr)) {
    std::array<uint8_t, 16> gid;
    std::memcpy(gid.data(), r.gid, gid.size());
    now.insert(gid);
  }
  size_t added = 0;
  size_t removed = 0;
  for (const auto & gid : now) {
    added += st.peers.count(gid) == 0 ? 1 : 0;
  }
  for (const auto & gid : st.peers) {
    removed += now.count(gid) == 0 ? 1 : 0;
  }
  st.peers = std::move(now);
  st.generation = gen;
  st.computed = true;
  if (added == 0 && removed == 0) {
    return false;
  }
  st.total_count += added;
  st.total_count_change += added;
  st.current_count_change += static_cast<int32_t>(added) - static_cast<int32_t>(removed);
  // One take credit per batch, as in record_message_lost.
  if (st.unread++ == 0 && notify && st.cb.callback) {
    st.cb.callback(st.cb.user_data, 1);
  }
  return true;
}

bool event_init(
  rmw_event_t * rmw_event, void * endpoint, rmw_event_type_t event_type, bool supported)
{
  if (!supported) {
    RMW_SET_ERROR_MSG("event type not supported");
    return false;
  }
  rmw_event->implementation_identifier = identifier;
  rmw_event->data = endpoint;
  rmw_event->event_type = event_type;
  if (is_matched_type(event_type)) {
    const MatchedRef ref = matched_of(endpoint, event_type);
    std::lock_guard<std::recursive_mutex> lock(*ref.mutex);
    if (ref.ctx && !ref.state->counted) {
      ref.state->counted = true;
      ref.ctx->matched_events.fetch_add(1, std::memory_order_release);
    }
    // Recompute now, so matches that already exist are reported as soon as
    // the event is waited on, taken or given a callback. With notify: the
    // state is per endpoint, so another handle's callback may be registered,
    // and the listener will not announce a generation this read first.
    recompute(ref, true);
  }
  return true;
}

// Unlist (endpoint, type) from ctx->graph_events. True when that emptied the
// list: the caller, holding listener_registration_mutex, then unwatches the
// doorbell no matched callback needs any more.
bool unlist_graph_event(UdsContext * ctx, void * endpoint, rmw_event_type_t type)
{
  std::lock_guard<std::mutex> lock(ctx->listener_mutex);
  auto & ev = ctx->graph_events;
  const auto before = ev.size();
  ev.erase(
    std::remove(ev.begin(), ev.end(), std::make_pair(endpoint, type)), ev.end());
  return ev.size() < before && ev.empty();
}

}  // namespace

bool event_has_unread(const rmw_event_t * event)
{
  if (event->event_type == RMW_EVENT_MESSAGE_LOST) {
    auto * sub = static_cast<UdsSubscription *>(event->data);
    std::lock_guard<std::recursive_mutex> lock(sub->event_mutex);
    return sub->message_lost.unread > 0;
  }
  if (is_matched_type(event->event_type)) {
    const MatchedRef ref = matched_of(event->data, event->event_type);
    std::lock_guard<std::recursive_mutex> lock(*ref.mutex);
    recompute(ref, true);
    return ref.state->unread > 0;
  }
  return false;
}

bool event_is_graph_driven(const rmw_event_t * event)
{
  return is_matched_type(event->event_type);
}

void record_message_lost(UdsSubscription * sub, size_t lost)
{
  {
    std::lock_guard<std::recursive_mutex> lock(sub->event_mutex);
    auto & st = sub->message_lost;
    st.total_count += lost;
    st.total_count_change += lost;
    // One take credit per batch: a callback fires only when the event goes
    // from nothing untaken to untaken, so every take it causes finds the batch.
    if (st.unread++ == 0 && st.cb.callback) {
      st.cb.callback(st.cb.user_data, 1);
    }
  }
  // Strictly after the record, from whichever thread drained: the drain may
  // have emptied a socket another wait set is blocked on, and its epoll then
  // never sees that socket readable. The wait re-checks its events on a
  // delivery wake (see rmw_wait).
  if (sub->context) {
    signal_delivery_fds(sub->context);
  }
}

void notify_graph_events(UdsContext * ctx)
{
  for (const auto & [endpoint, type] : ctx->graph_events) {
    const MatchedRef ref = matched_of(endpoint, type);
    std::lock_guard<std::recursive_mutex> lock(*ref.mutex);
    recompute(ref, true);
  }
}

bool message_lost_callback_set(UdsSubscription * sub)
{
  std::lock_guard<std::recursive_mutex> lock(sub->event_mutex);
  return sub->message_lost.cb.callback != nullptr;
}

void forget_graph_events(UdsContext * ctx, void * endpoint, rmw_event_type_t matched_type)
{
  if (!ctx) {
    return;
  }
  // Serialised with the callback setters, like their clear path: a set on
  // another endpoint between the check and the unwatch below would otherwise
  // lose the doorbell watch it just made.
  std::lock_guard<std::recursive_mutex> registration(ctx->listener_registration_mutex);
  if (unlist_graph_event(ctx, endpoint, matched_type)) {
    // Destroyed with its matched callback still set (rclcpp clears it first):
    // stop waking the listener on each registry mutation, as a clear does.
    listener_unwatch(ctx, ctx->doorbell_fd);
  }
  const MatchedRef ref = matched_of(endpoint, matched_type);
  std::lock_guard<std::recursive_mutex> lock(*ref.mutex);
  if (ref.state->counted) {
    ref.state->counted = false;
    ctx->matched_events.fetch_sub(1, std::memory_order_release);
  }
}

}  // namespace rmw_uds

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
  if (!rmw_uds::event_init(
      rmw_event, publisher->data, event_type, event_type == RMW_EVENT_PUBLICATION_MATCHED))
  {
    return RMW_RET_UNSUPPORTED;
  }
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
  const bool supported = event_type == RMW_EVENT_SUBSCRIPTION_MATCHED ||
    event_type == RMW_EVENT_MESSAGE_LOST;
  if (!rmw_uds::event_init(rmw_event, subscription->data, event_type, supported)) {
    return RMW_RET_UNSUPPORTED;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_event(
  const rmw_event_t * event_handle,
  void * event_info,
  bool * taken)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(event_handle, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(event_info, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_ARGUMENT_FOR_NULL(taken, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    event_handle, event_handle->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);
  RMW_CHECK_ARGUMENT_FOR_NULL(event_handle->data, RMW_RET_INVALID_ARGUMENT);
  // Like the DDS RMWs, a take always succeeds and reports the current status,
  // with zero changes when nothing is new: rcl turns taken == false into
  // RCL_RET_EVENT_TAKE_FAILED, which rclcpp logs as an error.
  *taken = false;
  if (event_handle->event_type == RMW_EVENT_MESSAGE_LOST) {
    auto * sub = static_cast<rmw_uds::UdsSubscription *>(event_handle->data);
    std::lock_guard<std::recursive_mutex> lock(sub->event_mutex);
    auto & st = sub->message_lost;
    auto * info = static_cast<rmw_message_lost_status_t *>(event_info);
    info->total_count = st.total_count;
    info->total_count_change = st.total_count_change;
    st.total_count_change = 0;
    st.unread = 0;
    *taken = true;
    return RMW_RET_OK;
  }
  if (rmw_uds::is_matched_type(event_handle->event_type)) {
    const auto ref = rmw_uds::matched_of(event_handle->data, event_handle->event_type);
    std::lock_guard<std::recursive_mutex> lock(*ref.mutex);
    rmw_uds::recompute(ref, false);  // no callback: this take collects the change
    auto & st = *ref.state;
    auto * info = static_cast<rmw_matched_status_t *>(event_info);
    info->total_count = st.total_count;
    info->total_count_change = st.total_count_change;
    info->current_count = st.peers.size();
    info->current_count_change = st.current_count_change;
    st.total_count_change = 0;
    st.current_count_change = 0;
    st.unread = 0;
    *taken = true;
    return RMW_RET_OK;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_event_fini(rmw_event_t * event)
{
  // Nothing to release, and nothing may depend on this running: rcl's
  // rcl_event_fini calls librmw's own rmw_event_fini, which rmw_implementation
  // does not forward to an RMW. Event state belongs to the endpoint, and
  // rmw_event_t::data aliases the endpoint's impl, so its destroy releases it
  // (forget_graph_events, listener_unwatch); rclcpp clears a callback before
  // it finalizes the event. And rclcpp can finalize an event after its
  // endpoint - EventHandler drops its parent handle before the base destructor
  // finalizes the event - so touching event->data here could read freed memory.
  (void)event;
  return RMW_RET_OK;
}

bool rmw_event_type_is_supported(rmw_event_type_t event_type)
{
  return event_type == RMW_EVENT_MESSAGE_LOST || rmw_uds::is_matched_type(event_type);
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
  if (!rmw_event_type_is_supported(event->event_type)) {
    // Set a message on this failure path too: returning one without it is
    // what made the original EventsExecutor crash unreadable ("failed to set
    // the on new message callback for Event: error not set").
    RMW_SET_ERROR_MSG("event callbacks are not supported for this event type");
    return RMW_RET_UNSUPPORTED;
  }
  RMW_CHECK_ARGUMENT_FOR_NULL(event->data, RMW_RET_INVALID_ARGUMENT);

  // Untaken changes are paid out as one take credit, and only when a callback
  // takes over from none: rclcpp sets it twice in a row on purpose (a stack
  // temporary, then its permanent storage), and paying out twice would hand
  // the executor a take with nothing new.
  if (event->event_type == RMW_EVENT_MESSAGE_LOST) {
    auto * sub = static_cast<rmw_uds::UdsSubscription *>(event->data);
    rmw_uds::UdsContext * ctx = sub->context;
    if (!ctx) {
      RMW_SET_ERROR_MSG("event endpoint has no context");
      return RMW_RET_ERROR;
    }
    // Serialised with listener_set_callback, which shares this socket watch.
    std::lock_guard<std::recursive_mutex> registration(ctx->listener_registration_mutex);
    if (callback) {
      // A loss is found by draining. With only this callback set, nothing
      // else may drain the socket - an executor driven by callbacks never
      // waits or takes - so the listener watches it.
      const rmw_ret_t ret = rmw_uds::listener_watch(
        ctx, sub->socket_fd, rmw_uds::ARMED_SUBSCRIPTION, sub, sub->uid);
      if (ret != RMW_RET_OK) {
        return ret;  // nothing is set yet
      }
    }
    {
      std::lock_guard<std::recursive_mutex> lock(sub->event_mutex);
      auto & st = sub->message_lost;
      const bool taking_over = !st.cb.callback;
      st.cb = {callback, user_data};
      if (callback && taking_over && st.unread > 0) {
        callback(user_data, 1);
      }
    }
    if (!callback) {
      bool message_callback = false;
      {
        std::lock_guard<std::recursive_mutex> lock(sub->callback_mutex);
        message_callback = sub->on_new_message_cb != nullptr;
      }
      if (!message_callback) {
        rmw_uds::listener_unwatch(ctx, sub->socket_fd);
      }
    }
    return RMW_RET_OK;
  }

  const auto ref = rmw_uds::matched_of(event->data, event->event_type);
  rmw_uds::UdsContext * ctx = ref.ctx;
  if (!ctx) {
    RMW_SET_ERROR_MSG("event endpoint has no context");
    return RMW_RET_ERROR;
  }
  // Serialised per context like listener_set_callback, so a concurrent set and
  // clear leave the callback and its graph_events entry agreeing. event_mutex
  // is never held while listener_mutex is taken here: the listener takes them
  // in the other order, in notify_graph_events().
  std::lock_guard<std::recursive_mutex> registration(ctx->listener_registration_mutex);
  if (!callback) {
    // Unlisted first: that waits out a listener recompute in flight, so no
    // listener-side fire can follow the clear.
    const bool none_left = rmw_uds::unlist_graph_event(ctx, event->data, event->event_type);
    {
      std::lock_guard<std::recursive_mutex> lock(*ref.mutex);
      ref.state->cb = {};
    }
    if (none_left) {
      // Stop waking the listener on each registry mutation. A no-op if the
      // doorbell was never watched.
      rmw_uds::listener_unwatch(ctx, ctx->doorbell_fd);
    }
    return RMW_RET_OK;
  }

  // Watch, then list, then recompute - the register-before-snapshot order
  // rmw_wait uses - so a registry change at any point either lands in the
  // recompute below or rings a doorbell the listener serves with this event
  // listed.
  if (ctx->doorbell_fd >= 0 && rmw_uds::ensure_doorbell_registered(ctx)) {
    const rmw_ret_t ret = rmw_uds::listener_watch(
      ctx, ctx->doorbell_fd, rmw_uds::ARMED_DOORBELL, nullptr, 0);
    if (ret != RMW_RET_OK) {
      return ret;  // nothing is set yet
    }
  } else {
    // No doorbell (registry full, or its socket never bound): nothing wakes
    // the listener for this context. The callback still fires for what this
    // registration finds and for what a wait in this process sees, so the
    // event is degraded rather than failed.
    // shortcut: no retry - a callback registration made while the registry is
    // full stays deaf to other processes until a later matched callback
    // registration finds a free slot; add a retry if full registries happen.
    RMW_UDS_LOG_WARN_THROTTLE(
      5000,
      "no graph doorbell (registry full?): matched callbacks hear no other "
      "process's graph changes until a matched callback is registered with a "
      "free slot");
  }
  {
    std::lock_guard<std::mutex> lock(ctx->listener_mutex);
    auto & ev = ctx->graph_events;
    const auto entry = std::make_pair(event->data, event->event_type);
    if (std::find(ev.begin(), ev.end(), entry) == ev.end()) {
      ev.push_back(entry);
    }
  }
  {
    std::lock_guard<std::recursive_mutex> lock(*ref.mutex);
    const bool taking_over = !ref.state->cb.callback;
    ref.state->cb = {callback, user_data};
    // A takeover pays any untaken batch out once below; a re-registration
    // notifies a new batch like any other recompute.
    rmw_uds::recompute(ref, !taking_over);
    if (taking_over && ref.state->unread > 0) {
      callback(user_data, 1);
    }
  }
  return RMW_RET_OK;
}

}  // extern "C"
