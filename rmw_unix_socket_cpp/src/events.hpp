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

#ifndef RMW_UNIX_SOCKET_CPP__EVENTS_HPP_
#define RMW_UNIX_SOCKET_CPP__EVENTS_HPP_

#include <cstddef>

#include "rmw/event.h"

#include "types.hpp"

// QoS status events: what rmw_wait, drain_endpoint() and the listener thread
// call into. The rmw_event_* entry points are in rmw_event.cpp. Supported:
// message_lost and both matched events, none of which needs a clock (see
// DESIGN.md, "QoS status events").

namespace rmw_uds
{

// True when the event has status changes not taken yet. A matched event first
// re-reads the registry if its generation moved.
bool event_has_unread(const rmw_event_t * event);

// True for events driven by registry changes (matched): a wait set holding one
// needs the doorbell, as a graph guard condition does.
bool event_is_graph_driven(const rmw_event_t * event);

// Record `lost` messages a drain found missing, fire the callback, then signal
// every wait set. The caller must not hold the subscription's drain_mutex or
// queue_mutex.
void record_message_lost(UdsSubscription * sub, size_t lost);

// Listener thread, with listener_mutex held, after a doorbell ring: recompute
// every matched event in ctx->graph_events and fire the ones that changed.
void notify_graph_events(UdsContext * ctx);

// True while the subscription has a message_lost callback, which keeps its
// socket watched by the listener (see listener_set_callback).
bool message_lost_callback_set(UdsSubscription * sub);

// Drop the endpoint's entries from ctx->graph_events and its count in
// ctx->matched_events. `matched_type` is the endpoint's matched event type.
// Called by the endpoint destroys before they free anything; takes
// listener_mutex, so it also waits out a notify_graph_events() in flight.
void forget_graph_events(UdsContext * ctx, void * endpoint, rmw_event_type_t matched_type);

// Register the context's doorbell slot if it is not registered yet, so
// registry mutations in any process ring it. Returns false only when the
// registry is full. Implemented in rmw_wait.cpp, its first user.
bool ensure_doorbell_registered(UdsContext * ctx);

// A doorbell ring is consumed by whichever reader gets it first - any rmw_wait
// in the process, or the listener - so the one that sees the registry
// generation move owns telling everyone else. observe_generation() advances
// ctx->last_registry_generation and is true only for the caller that moved
// it, which then calls trigger_graph_gcs() and listener_graph_changed().
// Implemented in rmw_wait.cpp.
bool observe_generation(UdsContext * ctx);
void trigger_graph_gcs(UdsContext * ctx);

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__EVENTS_HPP_
