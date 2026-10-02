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

#ifndef RMW_UNIX_SOCKET_CPP__LISTENER_HPP_
#define RMW_UNIX_SOCKET_CPP__LISTENER_HPP_

#include "types.hpp"

#include <cstdint>

#include "rmw/ret_types.h"

namespace rmw_uds
{

// The context's listener thread, which drains an endpoint's socket and fires
// its listener callback with no application thread involved.
//
// This exists for one reason: rclcpp's EventsExecutor never calls rmw_wait and
// only calls rmw_take once a callback has told it there is something to take.
// With delivery happening exclusively inside rmw_wait, such an executor waits
// forever on a socket nobody drains. Everything here is therefore inert until
// an endpoint actually registers a callback - see the lazy start below.
//
// A listener callback runs on this thread, and listener_mutex is one per
// CONTEXT and is held across the drain that fires it. So the restriction is
// context-wide, not endpoint-local: a callback must not block, and must not
// destroy, register on, or clear the callback of ANY endpoint in the context,
// nor create or destroy a wait set - each of those takes listener_mutex and
// would deadlock against the drain it was called from. Calling rmw_take on its
// own endpoint IS allowed: drain_endpoint releases both drain_mutex and
// queue_mutex before it notifies, so a callback runs holding only
// callback_mutex. rclcpp's callbacks only push to a queue, which is what the
// contract in rmw/event_callback_type.h expects.

// Watch `fd` on the context's listener thread, starting the thread if this is
// the first watch. Re-watching a live fd replaces its entry. Returns
// RMW_RET_OK, or an error when the thread or its fds cannot be created - in
// which case nothing is watched and the endpoint keeps working through
// rmw_wait.
rmw_ret_t listener_watch(
  UdsContext * ctx, int fd, uint8_t kind, void * entity, uint64_t uid);

// Stop watching `fd`. Blocks until an in-flight drain has returned - of any
// endpoint, since listener_mutex is per context - so the caller may destroy
// the endpoint afterwards. A no-op for an fd that was never watched.
void listener_unwatch(UdsContext * ctx, int fd);

// Add/remove a wait set's delivery eventfd to the set the listener signals
// after each enqueue. Called from rmw_create_wait_set and
// rmw_destroy_wait_set; the remove must happen before the fd is closed, or the
// listener writes into a recycled fd number.
void listener_add_delivery_fd(UdsContext * ctx, int fd);
void listener_remove_delivery_fd(UdsContext * ctx, int fd);

// Stop and join the thread and release its fds. Idempotent, and a no-op when
// the thread was never started.
void listener_stop(UdsContext * ctx);

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__LISTENER_HPP_
