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
#include "registry.hpp"
#include "transport.hpp"
#include "types.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <set>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include "rmw/allocators.h"
#include "rmw/check_type_identifiers_match.h"
#include "rmw/error_handling.h"
#include "rmw/rmw.h"

static int64_t steady_now_ns()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}

static int64_t wall_now_ns()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

// Drain a socket into a message queue (subscription, service, or client).
// `shm_cache`/`domain_id` resolve large-payload descriptors: topic, request,
// and response messages can all carry SHM_PAYLOAD_FLAG, so every caller passes
// its own reader cache. If `ignore_local` is true, `context_id` is compared
// against the sender context id embedded in WireHeader::gid and matching
// same-context publications are dropped (subscriptions only; services/clients
// leave ignore_local=false to disable the check).
static void drain_socket(
  int fd,
  std::mutex & queue_mutex,
  std::deque<rmw_uds::ReceivedMessage> & queue,
  size_t max_depth,
  uint8_t expected_msg_type,
  rmw_uds::ShmReaderCache & shm_cache,
  size_t domain_id,
  bool ignore_local = false,
  uint64_t context_id = 0)
{
  rmw_uds::WireHeader hdr;
  std::vector<uint8_t> payload;

  while (rmw_uds::recv_from(fd, hdr, payload)) {
    if ((hdr.msg_type & ~rmw_uds::SHM_PAYLOAD_FLAG) != expected_msg_type) {
      payload.clear();
      continue;
    }
    if (!rmw_uds::shm_resolve_incoming(shm_cache, domain_id, hdr, payload)) {
      payload.clear();
      continue;  // shm descriptor unresolvable (sender gone / ring lapped)
    }
    if (ignore_local && rmw_uds::is_same_context(hdr, context_id)) {
      payload.clear();
      continue;  // ignore_local_publications: drop same-context publications
    }

    rmw_uds::ReceivedMessage msg;
    msg.header = hdr;
    msg.payload = std::move(payload);
    msg.received_timestamp_ns = wall_now_ns();

    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      queue.push_back(std::move(msg));
      while (queue.size() > max_depth) {
        queue.pop_front();
      }
    }
  }
}

extern "C"
{

rmw_wait_set_t * rmw_create_wait_set(rmw_context_t * context, size_t max_conditions)
{
  (void)max_conditions;
  RMW_CHECK_ARGUMENT_FOR_NULL(context, nullptr);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    context, context->implementation_identifier,
    rmw_uds::identifier, return nullptr);

  auto * ws_data = new (std::nothrow) rmw_uds::UdsWaitSet();
  if (!ws_data) {
    RMW_SET_ERROR_MSG("failed to allocate wait set data");
    return nullptr;
  }
  ws_data->context = reinterpret_cast<rmw_uds::UdsContext *>(context->impl);

  ws_data->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
  if (ws_data->epoll_fd < 0) {
    delete ws_data;
    RMW_SET_ERROR_MSG("failed to create epoll fd");
    return nullptr;
  }

  auto * ws = rmw_wait_set_allocate();
  if (!ws) {
    close(ws_data->epoll_fd);
    delete ws_data;
    RMW_SET_ERROR_MSG("failed to allocate rmw_wait_set_t");
    return nullptr;
  }

  ws->implementation_identifier = rmw_uds::identifier;
  ws->data = ws_data;
  ws->guard_conditions = nullptr;
  return ws;
}

rmw_ret_t rmw_destroy_wait_set(rmw_wait_set_t * wait_set)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(wait_set, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    wait_set, wait_set->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ws_data = static_cast<rmw_uds::UdsWaitSet *>(wait_set->data);
  if (ws_data) {
    if (ws_data->epoll_fd >= 0) {
      close(ws_data->epoll_fd);
    }
    delete ws_data;
  }

  rmw_wait_set_free(wait_set);
  return RMW_RET_OK;
}

rmw_ret_t rmw_wait(
  rmw_subscriptions_t * subscriptions,
  rmw_guard_conditions_t * guard_conditions,
  rmw_services_t * services,
  rmw_clients_t * clients,
  rmw_events_t * events,
  rmw_wait_set_t * wait_set,
  const rmw_time_t * wait_timeout)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(wait_set, RMW_RET_INVALID_ARGUMENT);
  RMW_CHECK_TYPE_IDENTIFIERS_MATCH(
    wait_set, wait_set->implementation_identifier,
    rmw_uds::identifier, return RMW_RET_INCORRECT_RMW_IMPLEMENTATION);

  auto * ws_data = static_cast<rmw_uds::UdsWaitSet *>(wait_set->data);

  // 1. Drain all sockets
  if (subscriptions) {
    for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
      if (!subscriptions->subscribers[i]) {continue;}
      auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
      drain_socket(sub->socket_fd, sub->queue_mutex, sub->message_queue,
        sub->queue_depth, 0, sub->shm_cache, sub->context->domain_id,
        sub->ignore_local_publications, sub->context->context_id);
    }
  }

  if (services) {
    for (size_t i = 0; i < services->service_count; ++i) {
      if (!services->services[i]) {continue;}
      auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
      drain_socket(srv->socket_fd, srv->queue_mutex, srv->request_queue, 100, 1,
        srv->shm_cache, srv->context->domain_id);
    }
  }

  if (clients) {
    for (size_t i = 0; i < clients->client_count; ++i) {
      if (!clients->clients[i]) {continue;}
      auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
      drain_socket(cli->socket_fd, cli->queue_mutex, cli->response_queue, 100, 2,
        cli->shm_cache, cli->context->domain_id);
    }
  }

  // 2. Check graph generation changes — trigger graph guard conditions.
  // The context comes from the wait set itself (set at rmw_create_wait_set):
  // a guard-condition-only wait set (rclcpp's GraphListener) has no entity to
  // scavenge it from, and this check must run for those waits too. Wrapped in
  // a lambda so the step-4 loop can re-run it on each doorbell wake.
  rmw_uds::UdsContext * ctx = ws_data->context;
  const int doorbell_fd = ctx ? ctx->doorbell_fd : -1;
  auto run_generation_check = [&]() {
      // Drain the doorbell strictly BEFORE reading the generation: paired with
      // ring_doorbells running strictly AFTER the bump, a mutation either lands
      // in this generation read or leaves a queued datagram that keeps the
      // level-triggered fd readable — no lost wakeup.
      if (doorbell_fd >= 0) {
        uint8_t buf[16];
        while (recv(doorbell_fd, buf, sizeof(buf), MSG_DONTWAIT) > 0) {
        }
      }
      if (!(ctx && ctx->registry_ptr)) {
        return;
      }
    auto * header = rmw_uds::registry_header(ctx->registry_ptr);
    uint64_t gen = rmw_uds::registry_generation(header);
    if (gen != ctx->last_registry_generation.load(std::memory_order_relaxed)) {
      ctx->last_registry_generation.store(gen, std::memory_order_relaxed);

      // TRANSIENT_LOCAL late-joiner replay. Lock held across the loop —
      // rmw_destroy_publisher takes the same mutex then deletes, so this
      // is what keeps each pub pointer alive while we dereference it.
      std::lock_guard<std::mutex> tl_lock(ctx->transient_local_pubs_mutex);
      for (auto * pub : ctx->transient_local_pubs) {
        std::shared_ptr<const std::vector<std::string>> sub_paths;
        {
          std::lock_guard<std::mutex> sc_lock(pub->sub_cache_mutex);
          if (rmw_uds::registry_generation(header) != pub->cached_generation) {
            auto subs = rmw_uds::registry_query(
              header, rmw_uds::ENTRY_SUBSCRIPTION,
              pub->topic_name.c_str(), nullptr, nullptr);
            auto fresh = std::make_shared<std::vector<std::string>>();
            fresh->reserve(subs.size());
            for (const auto & s : subs) {
              if (!s.socket_path.empty()) {
                fresh->push_back(s.socket_path);
              }
            }
            pub->cached_generation = rmw_uds::registry_generation(header);
            pub->cached_subscriber_paths = std::move(fresh);

            // Prune known subscribers no longer present, against the freshly-
            // built canonical list while STILL holding sub_cache_mutex (nested
            // lock order sub_cache_mutex -> cache_mutex), so a concurrent
            // refresh (e.g. from rmw_publish) cannot make the pruning set stale
            // and erase a still-live late-joiner.
            std::lock_guard<std::mutex> prune_lock(pub->cache_mutex);
            std::set<std::string> current(
              pub->cached_subscriber_paths->begin(),
              pub->cached_subscriber_paths->end());
            for (auto it = pub->known_subscriber_paths.begin();
              it != pub->known_subscriber_paths.end(); )
            {
              it = (current.count(*it) == 0) ?
                pub->known_subscriber_paths.erase(it) : std::next(it);
            }
          }
          sub_paths = pub->cached_subscriber_paths;
        }
        std::lock_guard<std::mutex> c_lock(pub->cache_mutex);
        if (!sub_paths) {
          continue;
        }
        for (const auto & path : *sub_paths) {
          if (pub->known_subscriber_paths.count(path) != 0) {
            continue;
          }
          pub->known_subscriber_paths.insert(path);
          for (const auto & cm : pub->message_cache) {
            rmw_uds::send_to(
              ctx->send_socket_fd,
              path, cm.header, cm.payload.data(), cm.payload.size());
          }
        }
      }

      // Wake graph listeners: trigger every node's graph guard condition
      // (rclcpp's GraphListener waits on these). rmw_destroy_node removes a
      // node's GC from this list under the same mutex before destroying it,
      // so a freed guard condition is never triggered.
      {
        std::lock_guard<std::mutex> gc_lock(ctx->graph_gcs_mutex);
        for (auto * gc : ctx->graph_gcs) {
          auto _r [[maybe_unused]] = rmw_trigger_guard_condition(gc);
        }
      }
    }
  };
  run_generation_check();

  // Arm every entity fd with epoll on every wait. EPOLL_CTL_ADD is idempotent
  // here: a still-live fd returns EEXIST (already armed), while a fd number
  // reused after its previous owner was closed gets freshly armed. The kernel
  // auto-removes closed fds, so no EPOLL_CTL_DEL is needed.
  {
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    auto register_fd = [&](int fd) {
        if (fd < 0) {return;}
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        // EEXIST means the fd is already armed -> treat as success.
        if (epoll_ctl(ws_data->epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0 &&
          errno != EEXIST)
        {
          // Other errors are ignored, as before.
        }
      };

    if (subscriptions) {
      for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
        if (!subscriptions->subscribers[i]) {continue;}
        auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
        register_fd(sub->socket_fd);
      }
    }
    if (services) {
      for (size_t i = 0; i < services->service_count; ++i) {
        if (!services->services[i]) {continue;}
        auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
        register_fd(srv->socket_fd);
      }
    }
    if (clients) {
      for (size_t i = 0; i < clients->client_count; ++i) {
        if (!clients->clients[i]) {continue;}
        auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
        register_fd(cli->socket_fd);
      }
    }
    if (guard_conditions) {
      for (size_t i = 0; i < guard_conditions->guard_condition_count; ++i) {
        if (!guard_conditions->guard_conditions[i]) {continue;}
        auto * gc = static_cast<rmw_uds::UdsGuardCondition *>(
          guard_conditions->guard_conditions[i]);
        register_fd(gc->eventfd_fd);
      }
    }
    // The context's doorbell: rung by any process after a registry mutation.
    register_fd(doorbell_fd);
  }

  // 3. Check if anything is already ready
  bool something_ready = false;

  // Per-GC readiness from the step-3 consuming read, carried to step 5. One
  // read drains the whole eventfd counter, so we never write it back (a
  // non-atomic read-back would race a concurrent trigger and inflate it).
  std::vector<bool> gc_triggered;

  if (subscriptions) {
    for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
      if (!subscriptions->subscribers[i]) {continue;}
      auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
      std::lock_guard<std::mutex> lock(sub->queue_mutex);
      if (!sub->message_queue.empty()) {
        something_ready = true;
      }
    }
  }

  if (services) {
    for (size_t i = 0; i < services->service_count; ++i) {
      if (!services->services[i]) {continue;}
      auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
      std::lock_guard<std::mutex> lock(srv->queue_mutex);
      if (!srv->request_queue.empty()) {
        something_ready = true;
      }
    }
  }

  if (clients) {
    for (size_t i = 0; i < clients->client_count; ++i) {
      if (!clients->clients[i]) {continue;}
      auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
      std::lock_guard<std::mutex> lock(cli->queue_mutex);
      if (!cli->response_queue.empty()) {
        something_ready = true;
      }
    }
  }

  if (guard_conditions) {
    gc_triggered.assign(guard_conditions->guard_condition_count, false);
    for (size_t i = 0; i < guard_conditions->guard_condition_count; ++i) {
      if (!guard_conditions->guard_conditions[i]) {continue;}
      auto * gc = static_cast<rmw_uds::UdsGuardCondition *>(
        guard_conditions->guard_conditions[i]);
      uint64_t val;
      ssize_t r = read(gc->eventfd_fd, &val, sizeof(val));
      if (r == static_cast<ssize_t>(sizeof(val))) {
        something_ready = true;
        gc_triggered[i] = true;
      }
    }
  }

  // Compute timeout. -1 means block forever (epoll_wait sentinel).
  int timeout_ms = -1;
  if (wait_timeout) {
    // Accumulate in int64_t; RMW_DURATION_INFINITE (~9.2e12 ms) overflows int.
    int64_t ms = static_cast<int64_t>(wait_timeout->sec) * 1000 +
      static_cast<int64_t>(wait_timeout->nsec) / 1000000;
    if (ms > std::numeric_limits<int>::max()) {
      timeout_ms = -1;  // Infinite (or beyond epoll's range) -> block forever
    } else {
      timeout_ms = static_cast<int>(ms);
      if (timeout_ms == 0 && wait_timeout->nsec > 0) {
        timeout_ms = 1;  // At least 1ms
      }
    }
  }

  // Block until something the caller waits on fires, or the caller's own
  // deadline. There is no internal poll: a registry mutation in any process
  // rings this context's doorbell (ring_doorbells in registry.cpp), which
  // wakes the epoll; the doorbell is drained, the registry re-checked
  // (TRANSIENT_LOCAL late-joiner replay + graph guard conditions), and — if
  // nothing the caller waits on became ready — the wait re-blocks.
  // RMW_RET_TIMEOUT surfaces only at the caller's own deadline; an infinite
  // wait never surfaces a synthetic timeout. EINTR re-enters the loop, so a
  // signal neither returns TIMEOUT early nor busy-loops.
  const bool infinite = (timeout_ms < 0);
  const int64_t caller_deadline_ns =
    infinite ? 0 : steady_now_ns() + static_cast<int64_t>(timeout_ms) * 1000000;
  // 4. If nothing ready, block with epoll
  if (!something_ready) {
    struct epoll_event ready_events[64];
    while (true) {
      int block_ms = -1;
      if (!infinite) {
        const int64_t rem_ns = caller_deadline_ns - steady_now_ns();
        const int64_t rem_ms = (rem_ns > 0) ? (rem_ns + 999999) / 1000000 : 0;  // ceil
        block_ms = static_cast<int>(
          std::min<int64_t>(rem_ms, std::numeric_limits<int>::max()));
      }
      int n = epoll_wait(ws_data->epoll_fd, ready_events, 64, block_ms);
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        RMW_SET_ERROR_MSG("epoll_wait failed");
        return RMW_RET_ERROR;
      }
      if (n == 0) {
        break;  // The caller's deadline passed -> timeout; fall through to drain.
      }
      bool only_doorbell = true;
      bool rang = false;
      for (int e = 0; e < n; ++e) {
        if (ready_events[e].data.fd == doorbell_fd) {
          rang = true;
        } else {
          only_doorbell = false;
        }
      }
      if (rang) {
        run_generation_check();  // Drains the doorbell, replays, triggers GCs.
      }
      if (!only_doorbell) {
        break;  // Something the caller waits on fired -> fall through to drain.
      }
      if (!infinite && steady_now_ns() >= caller_deadline_ns) {
        break;  // Doorbell-only wake at the deadline -> timeout.
      }
      // Doorbell-only wake: re-block for the caller's remaining time.
    }
    // No EPOLL_CTL_DEL needed — fds stay registered across calls.

    // Drain again after epoll
    if (subscriptions) {
      for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
        if (!subscriptions->subscribers[i]) {continue;}
        auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
        drain_socket(sub->socket_fd, sub->queue_mutex, sub->message_queue,
          sub->queue_depth, 0, sub->shm_cache, sub->context->domain_id,
          sub->ignore_local_publications, sub->context->context_id);
      }
    }
    if (services) {
      for (size_t i = 0; i < services->service_count; ++i) {
        if (!services->services[i]) {continue;}
        auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
        drain_socket(srv->socket_fd, srv->queue_mutex, srv->request_queue, 100, 1,
          srv->shm_cache, srv->context->domain_id);
      }
    }
    if (clients) {
      for (size_t i = 0; i < clients->client_count; ++i) {
        if (!clients->clients[i]) {continue;}
        auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
        drain_socket(cli->socket_fd, cli->queue_mutex, cli->response_queue, 100, 2,
          cli->shm_cache, cli->context->domain_id);
      }
    }
  }

  // 5. Set output: ready entities stay, non-ready set to NULL
  bool any_ready = false;

  if (subscriptions) {
    for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
      if (!subscriptions->subscribers[i]) {continue;}
      auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
      std::lock_guard<std::mutex> lock(sub->queue_mutex);
      if (sub->message_queue.empty()) {
        subscriptions->subscribers[i] = nullptr;
      } else {
        any_ready = true;
      }
    }
  }

  if (guard_conditions) {
    for (size_t i = 0; i < guard_conditions->guard_condition_count; ++i) {
      if (!guard_conditions->guard_conditions[i]) {continue;}
      auto * gc = static_cast<rmw_uds::UdsGuardCondition *>(
        guard_conditions->guard_conditions[i]);
      uint64_t val;
      // Consume a trigger that landed during the epoll block; combine with the
      // step-3 read so a GC seen ready then stays reported without a read-back.
      ssize_t r = read(gc->eventfd_fd, &val, sizeof(val));
      if (r == static_cast<ssize_t>(sizeof(val)) || gc_triggered[i]) {
        any_ready = true;
      } else {
        guard_conditions->guard_conditions[i] = nullptr;
      }
    }
  }

  if (services) {
    for (size_t i = 0; i < services->service_count; ++i) {
      if (!services->services[i]) {continue;}
      auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
      std::lock_guard<std::mutex> lock(srv->queue_mutex);
      if (srv->request_queue.empty()) {
        services->services[i] = nullptr;
      } else {
        any_ready = true;
      }
    }
  }

  if (clients) {
    for (size_t i = 0; i < clients->client_count; ++i) {
      if (!clients->clients[i]) {continue;}
      auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
      std::lock_guard<std::mutex> lock(cli->queue_mutex);
      if (cli->response_queue.empty()) {
        clients->clients[i] = nullptr;
      } else {
        any_ready = true;
      }
    }
  }

  // Events — not supported, set all to null
  if (events) {
    for (size_t i = 0; i < events->event_count; ++i) {
      events->events[i] = nullptr;
    }
  }

  // Only the caller's own deadline may report a timeout. Waking up and finding
  // nothing to take is a spurious wake: report OK with every entry nulled.
  if (!any_ready && !infinite && steady_now_ns() >= caller_deadline_ns) {
    return RMW_RET_TIMEOUT;
  }

  return RMW_RET_OK;
}

}  // extern "C"
