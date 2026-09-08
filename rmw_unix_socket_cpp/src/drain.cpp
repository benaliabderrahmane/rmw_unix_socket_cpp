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

#include "drain.hpp"

#include "logging.hpp"
#include "shm_transport.hpp"
#include "transport.hpp"

#include <chrono>
#include <cstring>
#include <utility>
#include <vector>

namespace rmw_uds
{

static int64_t received_now_ns()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

DrainTarget drain_target(UdsSubscription * sub)
{
  DrainTarget t;
  t.fd = sub->socket_fd;
  t.queue_mutex = &sub->queue_mutex;
  t.queue = &sub->message_queue;
  t.max_depth = sub->queue_depth;
  t.msg_type = 0;
  t.shm_cache = &sub->shm_cache;
  t.domain_id = sub->context->domain_id;
  t.ignore_local = sub->ignore_local_publications;
  t.context_id = sub->context->context_id;
  t.replay_watermarks = &sub->replayed_watermarks;
  t.callback_mutex = &sub->callback_mutex;
  t.callback = &sub->on_new_message_cb;
  t.callback_user_data = &sub->on_new_message_user_data;
  t.log_name = sub->topic_name.c_str();
  return t;
}

DrainTarget drain_target(UdsService * srv)
{
  DrainTarget t;
  t.fd = srv->socket_fd;
  t.queue_mutex = &srv->queue_mutex;
  t.queue = &srv->request_queue;
  t.max_depth = SERVICE_QUEUE_DEPTH;
  t.msg_type = 1;
  t.shm_cache = &srv->shm_cache;
  t.domain_id = srv->context->domain_id;
  t.callback_mutex = &srv->callback_mutex;
  t.callback = &srv->on_new_request_cb;
  t.callback_user_data = &srv->on_new_request_user_data;
  t.log_name = srv->service_name.c_str();
  return t;
}

DrainTarget drain_target(UdsClient * cli)
{
  DrainTarget t;
  t.fd = cli->socket_fd;
  t.queue_mutex = &cli->queue_mutex;
  t.queue = &cli->response_queue;
  t.max_depth = SERVICE_QUEUE_DEPTH;
  t.msg_type = 2;
  t.shm_cache = &cli->shm_cache;
  t.domain_id = cli->context->domain_id;
  t.callback_mutex = &cli->callback_mutex;
  t.callback = &cli->on_new_response_cb;
  t.callback_user_data = &cli->on_new_response_user_data;
  t.log_name = cli->service_name.c_str();
  return t;
}

void drain_endpoint(const DrainTarget & t)
{
  WireHeader hdr;
  std::vector<uint8_t> payload;
  size_t enqueued = 0;

  while (recv_from(t.fd, hdr, payload)) {
    if ((hdr.msg_type & ~SHM_PAYLOAD_FLAG) != t.msg_type) {
      continue;
    }
    if (t.replay_watermarks) {
      // TRANSIENT_LOCAL dedup: drop this datagram when its sample was already
      // delivered by the creation-time pull - the sender's GID has a watermark
      // and the sequence number is at or below it. Sequence numbers are
      // assigned inside the publisher's latch critical section and the pull
      // never extends the watermark across a scan-overlap gap, so anything
      // <= the watermark was either pulled or already lapped out of the
      // publisher's ring at pull time - history DDS would not owe a late
      // joiner either. Checked before the descriptor resolve so a duplicate
      // never maps a segment.
      std::array<uint8_t, 16> key;
      std::memcpy(key.data(), hdr.gid, key.size());
      bool dup = false;
      {
        std::lock_guard<std::mutex> lock(*t.queue_mutex);
        auto it = t.replay_watermarks->find(key);
        dup = it != t.replay_watermarks->end() &&
          hdr.sequence_number <= it->second;
      }
      if (dup) {
        continue;
      }
    }
    if (!shm_resolve_incoming(*t.shm_cache, t.domain_id, hdr, payload)) {
      continue;  // shm descriptor unresolvable (sender gone / ring lapped)
    }
    if (t.ignore_local && is_same_context(hdr, t.context_id)) {
      continue;  // ignore_local_publications: drop same-context publications
    }

    ReceivedMessage msg;
    msg.header = hdr;
    msg.payload = std::move(payload);
    msg.received_timestamp_ns = received_now_ns();

    bool overflow = false;
    {
      std::lock_guard<std::mutex> lock(*t.queue_mutex);
      t.queue->push_back(std::move(msg));
      // Any pop here drops a datagram the kernel already delivered to us
      // because the take() side isn't keeping up. This is the rcl-layer
      // equivalent of "slow subscriber".
      while (t.queue->size() > t.max_depth) {
        t.queue->pop_front();
        overflow = true;
      }
    }

    if (overflow) {
      RMW_UDS_LOG_WARN_THROTTLE(
        1000,
        "queue overflow on '%s' (depth=%zu) — dropping oldest. "
        "Application is not calling take() fast enough.",
        t.log_name, t.max_depth);
    }
    ++enqueued;
  }

  if (enqueued == 0 || !t.callback_mutex) {
    return;
  }
  // One notification for the whole batch. rmw_event_callback_t takes the
  // number of events since it was last called and must never be handed zero
  // (rmw/event_callback_type.h), so a drain that enqueued nothing stays
  // silent and a drain that enqueued ten reports ten rather than calling ten
  // times. The count is datagrams enqueued, not datagrams retained: on an
  // overflow the executor is told about deliveries the queue no longer holds
  // and its extra take() calls report nothing taken, which is the same benign
  // race rmw_wait already has when another thread takes first.
  //
  // Notified with queue_mutex released. set_on_new_*_callback() takes
  // callback_mutex and then queue_mutex to flush a backlog, so holding both
  // in the other order here would deadlock.
  std::lock_guard<std::mutex> lock(*t.callback_mutex);
  if (*t.callback) {
    (*t.callback)(*t.callback_user_data, enqueued);
  }
}

}  // namespace rmw_uds
