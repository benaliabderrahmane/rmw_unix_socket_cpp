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

#ifndef RMW_UNIX_SOCKET_CPP__DRAIN_HPP_
#define RMW_UNIX_SOCKET_CPP__DRAIN_HPP_

#include "types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>

namespace rmw_uds
{

// Requests and responses carry no QoS depth of their own. This is the bound
// rmw_wait has always applied to both queues; drain_target() now hands it to
// the take path too, so a caller that only ever calls rmw_take_request()
// cannot grow one without limit.
//
// Deliberate: this makes the take path drop-oldest where it used to be
// unbounded, so a client with >100 outstanding responses can lose the one it
// is waiting on. rclcpp always waits before it takes, and rmw_wait was
// already trimming to this bound on every call, so nothing that goes through
// an executor changes. The only caller affected is one that polls
// rmw_take_response() and never waits - and for that caller unbounded growth
// is the worse failure of the two.
static constexpr size_t SERVICE_QUEUE_DEPTH = 100;

// Everything drain_endpoint() needs to move datagrams off one endpoint's
// socket into its queue. Every endpoint drains identically; only the policy
// below differs, so each type fills this in from its own impl struct via the
// drain_target() overloads.
struct DrainTarget
{
  int fd = -1;
  std::mutex * queue_mutex = nullptr;
  std::deque<ReceivedMessage> * queue = nullptr;
  // Trim the queue to this many entries after each push.
  size_t max_depth = SERVICE_QUEUE_DEPTH;
  // The WireHeader::msg_type this endpoint accepts: 0 topic, 1 request,
  // 2 response. One process reuses a socket path pattern across endpoint
  // kinds, so anything else on this fd belongs to someone else and is
  // dropped.
  uint8_t msg_type = 0;
  ShmReaderCache * shm_cache = nullptr;
  size_t domain_id = 0;
  // Subscriptions only: rmw_subscription_options_t::ignore_local_publications.
  bool ignore_local = false;
  uint64_t context_id = 0;
  // Subscriptions only: TRANSIENT_LOCAL replay dedup. Read under
  // *queue_mutex, which is what guards the map itself.
  std::map<std::array<uint8_t, 16>, int64_t> * replay_watermarks = nullptr;
  // Fired under *callback_mutex once per enqueued datagram. Null on endpoints
  // that have no listener callback wired up.
  std::mutex * callback_mutex = nullptr;
  rmw_event_callback_t * callback = nullptr;
  const void ** callback_user_data = nullptr;
  // Topic or service name, named in the queue-overflow warning.
  const char * log_name = nullptr;
};

DrainTarget drain_target(UdsSubscription * sub);
DrainTarget drain_target(UdsService * srv);
DrainTarget drain_target(UdsClient * cli);

// Move every datagram waiting on the endpoint's socket into its queue. The
// caller must not hold *queue_mutex.
void drain_endpoint(const DrainTarget & target);

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__DRAIN_HPP_
