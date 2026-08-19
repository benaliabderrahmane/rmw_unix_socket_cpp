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
#include "logging.hpp"
#include "registry.hpp"
#include "transport.hpp"
#include "types.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <map>
#include <unordered_map>
#include <unordered_set>

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
  uint64_t context_id = 0,
  std::map<std::array<uint8_t, 16>, int64_t> * replay_watermarks = nullptr)
{
  rmw_uds::WireHeader hdr;
  std::vector<uint8_t> payload;

  while (rmw_uds::recv_from(fd, hdr, payload)) {
    if ((hdr.msg_type & ~rmw_uds::SHM_PAYLOAD_FLAG) != expected_msg_type) {
      payload.clear();
      continue;
    }
    if (replay_watermarks) {
      // TRANSIENT_LOCAL dedup (subscriptions only): drop a datagram whose
      // sample the creation-time pull already delivered. Checked before the
      // descriptor resolve so a duplicate never maps a segment.
      std::array<uint8_t, 16> key;
      std::memcpy(key.data(), hdr.gid, key.size());
      bool dup = false;
      {
        std::lock_guard<std::mutex> lock(queue_mutex);
        auto it = replay_watermarks->find(key);
        dup = it != replay_watermarks->end() &&
          hdr.sequence_number <= it->second;
      }
      if (dup) {
        payload.clear();
        continue;
      }
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

  // 1. Graph-event wiring. The context comes from the wait set itself (set at
  // rmw_create_wait_set): a guard-condition-only wait set (rclcpp's
  // GraphListener) has no entity to scavenge it from, and this must run for
  // those waits too.
  rmw_uds::UdsContext * ctx = ws_data->context;

  // Lazy doorbell registration: only graph-event consumers need registry
  // wakeups (TRANSIENT_LOCAL replay is pull-based and needs no wake at all),
  // so the ENTRY_DOORBELL slot is registered on the first wait whose set
  // holds one of this context's graph guard conditions. Ordering mirrors the
  // eager rmw_init recipe this replaces: the socket was bound at rmw_init,
  // registration happens HERE — strictly before the generation check below —
  // so a mutation either predates the registration (caught by the check) or
  // rings the already-bound socket. Registration failure (registry full)
  // leaves the flag unlatched and retries on the next wait; it never turns
  // the wait into an error.
  bool graph_gc_unwired = false;  // graph GC waited on, doorbell unregistered
  if (ctx && ctx->registry_ptr && guard_conditions &&
    !ctx->doorbell_registered.load(std::memory_order_acquire))
  {
    bool has_graph_gc = false;
    {
      std::lock_guard<std::mutex> gc_lock(ctx->graph_gcs_mutex);
      for (size_t i = 0;
        !has_graph_gc && i < guard_conditions->guard_condition_count; ++i)
      {
        for (auto * gc : ctx->graph_gcs) {
          if (gc && gc->data == guard_conditions->guard_conditions[i]) {
            has_graph_gc = true;
            break;
          }
        }
      }
    }
    if (has_graph_gc) {
      std::lock_guard<std::mutex> reg_lock(ctx->doorbell_reg_mutex);
      if (!ctx->doorbell_registered.load(std::memory_order_relaxed)) {
        auto * reg_header = rmw_uds::registry_header(ctx->registry_ptr);
        rmw_uds::RegistryEntry dentry;
        std::memset(&dentry, 0, sizeof(dentry));
        dentry.type = rmw_uds::ENTRY_DOORBELL;
        dentry.pid = getpid();
        std::strncpy(dentry.socket_path, ctx->doorbell_path.c_str(),
          sizeof(dentry.socket_path) - 1);
        const int32_t idx = rmw_uds::registry_add(reg_header, dentry);
        if (idx >= 0) {
          ctx->doorbell_registry_index = idx;
          ctx->doorbell_registered.store(true, std::memory_order_release);
        } else {
          // Registry full: this graph wait has no wakeup wiring. Returning an
          // error would be executor-fatal, and blocking unbounded would lose
          // graph events forever, so the block below is bounded instead: the
          // wait degrades to a coarse retry loop (spurious OK wakes) until a
          // slot frees up and registration succeeds.
          graph_gc_unwired = true;
          RMW_UDS_LOG_WARN_THROTTLE(
            5000,
            "registry full — graph-event doorbell unregistered; graph waits "
            "degrade to polling until a slot frees");
        }
      }
    }
  }

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

  // 2. Arm every entity fd with epoll, but only when it is not already armed
  // for this same entity. ws_data->armed remembers what each fd was armed for
  // and survives across calls, so in the steady state this pass costs no
  // syscall at all. A fd number reused after its previous owner closed carries
  // a different uid and is re-armed. A ready fd is dispatched below only when
  // this call armed it (armed_this_call); stale entries are pruned there.
  //
  // gc_index maps a guard condition back to its slot in the caller's array, so
  // an epoll result can mark the right entry in gc_triggered below.
  std::unordered_map<const void *, size_t> gc_index;
  // Fds armed by THIS call: the caller's entities plus the doorbell. The
  // dispatch loop never touches an entity outside this set — it may already
  // be destroyed, or belong to a wait set that is actually waiting on it.
  std::unordered_set<int> armed_this_call;
  // Entities epoll could not watch (e.g. watch exhaustion): drained directly
  // each call below so they degrade to polling instead of vanishing.
  std::vector<rmw_uds::ArmedEntry> unarmed;
  {
    struct epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    auto register_fd = [&](int fd, uint8_t kind, void * entity, uint64_t uid) {
        if (fd < 0) {return;}
        armed_this_call.insert(fd);
        auto it = ws_data->armed.find(fd);
        if (it != ws_data->armed.end() && it->second.uid == uid) {
          return;  // Already armed for this entity.
        }
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        // EEXIST means the fd is already armed in the kernel -> treat as
        // success so the entry still lands in `armed`. Returning here instead
        // would leave the fd armed but unmapped, and every event on it would
        // then be dropped by the lookup below.
        if (epoll_ctl(ws_data->epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0 &&
          errno != EEXIST)
        {
          // Without a watch this entity is never reported ready, and there is
          // no unconditional pre-drain any more — a silent skip would leave it
          // permanently invisible. Surface the error (epoll watch exhaustion
          // is an operator-level sysctl problem) and fall back to polling.
          RMW_UDS_LOG_ERROR_THROTTLE(
            1000,
            "epoll_ctl ADD failed for fd %d: %s (errno=%d) — entity degraded "
            "to per-wait polling",
            fd, std::strerror(errno), errno);
          armed_this_call.erase(fd);
          unarmed.push_back(rmw_uds::ArmedEntry{kind, entity, uid});
          return;
        }
        ws_data->armed[fd] = rmw_uds::ArmedEntry{kind, entity, uid};
      };

    if (subscriptions) {
      for (size_t i = 0; i < subscriptions->subscriber_count; ++i) {
        if (!subscriptions->subscribers[i]) {continue;}
        auto * sub = static_cast<rmw_uds::UdsSubscription *>(subscriptions->subscribers[i]);
        register_fd(sub->socket_fd, rmw_uds::ARMED_SUBSCRIPTION, sub, sub->uid);
      }
    }
    if (services) {
      for (size_t i = 0; i < services->service_count; ++i) {
        if (!services->services[i]) {continue;}
        auto * srv = static_cast<rmw_uds::UdsService *>(services->services[i]);
        register_fd(srv->socket_fd, rmw_uds::ARMED_SERVICE, srv, srv->uid);
      }
    }
    if (clients) {
      for (size_t i = 0; i < clients->client_count; ++i) {
        if (!clients->clients[i]) {continue;}
        auto * cli = static_cast<rmw_uds::UdsClient *>(clients->clients[i]);
        register_fd(cli->socket_fd, rmw_uds::ARMED_CLIENT, cli, cli->uid);
      }
    }
    if (guard_conditions) {
      for (size_t i = 0; i < guard_conditions->guard_condition_count; ++i) {
        if (!guard_conditions->guard_conditions[i]) {continue;}
        auto * gc = static_cast<rmw_uds::UdsGuardCondition *>(
          guard_conditions->guard_conditions[i]);
        gc_index[gc] = i;
        register_fd(gc->eventfd_fd, rmw_uds::ARMED_GUARD_CONDITION, gc, gc->uid);
      }
    }
    // The context's doorbell: rung by another process after a registry
    // mutation, once this context lazily registers it (graph-GC waits only).
    // It has no entity, so uid 0 (never handed out by next_entity_uid) arms
    // it exactly once for the life of the wait set.
    register_fd(doorbell_fd, rmw_uds::ARMED_DOORBELL, nullptr, 0);
  }

  // Fallback for entities epoll cannot watch: drain their sockets directly so
  // the queue check below still sees their data, and bound the block below so
  // the drain recurs. Guard-condition eventfds need no drain here — step 3
  // reads every caller GC directly.
  for (const auto & ue : unarmed) {
    switch (ue.kind) {
      case rmw_uds::ARMED_SUBSCRIPTION: {
          auto * sub = static_cast<rmw_uds::UdsSubscription *>(ue.entity);
          drain_socket(sub->socket_fd, sub->queue_mutex, sub->message_queue,
            sub->queue_depth, 0, sub->shm_cache, sub->context->domain_id,
            sub->ignore_local_publications, sub->context->context_id,
            &sub->replayed_watermarks);
          break;
        }
      case rmw_uds::ARMED_SERVICE: {
          auto * srv = static_cast<rmw_uds::UdsService *>(ue.entity);
          drain_socket(srv->socket_fd, srv->queue_mutex, srv->request_queue, 100, 1,
            srv->shm_cache, srv->context->domain_id);
          break;
        }
      case rmw_uds::ARMED_CLIENT: {
          auto * cli = static_cast<rmw_uds::UdsClient *>(ue.entity);
          drain_socket(cli->socket_fd, cli->queue_mutex, cli->response_queue, 100, 2,
            cli->shm_cache, cli->context->domain_id);
          break;
        }
      default:
        break;
    }
  }

  // 3. Check if anything is already ready, without blocking. drain_socket()
  // reads until EAGAIN, so an earlier wait can leave more messages queued than
  // rmw_take has consumed since. Those are invisible to epoll (their socket is
  // already empty), so the internal queues must be checked directly. This is a
  // mutex and a deque test per entity, no syscalls.
  bool something_ready = false;

  // Per-GC readiness from the consuming read below, carried to the output
  // pass. One read drains the whole eventfd counter, so we never write it back
  // (a non-atomic read-back would race a concurrent trigger and inflate it).
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
  // rings this context's doorbell if it registered one (ring_doorbells in
  // registry.cpp), which wakes the epoll; the doorbell is drained, the
  // registry re-checked (graph guard conditions; TRANSIENT_LOCAL replay is
  // pull-based), and — if nothing the caller waits on became ready — the wait
  // re-blocks. RMW_RET_TIMEOUT surfaces only at the caller's own deadline; an
  // infinite wait never surfaces a synthetic timeout. EINTR re-enters the
  // loop, so a signal neither returns TIMEOUT early nor busy-loops.
  const bool infinite = (timeout_ms < 0);
  const int64_t caller_deadline_ns =
    infinite ? 0 : steady_now_ns() + static_cast<int64_t>(timeout_ms) * 1000000;
  // 4. Block on epoll, then drain only the fds it reported ready. This is what
  // keeps the wait O(ready) instead of O(entities in the wait set). When step 3
  // already found queued work we still make one non-blocking pass, so an entity
  // whose data is sitting unread in its socket is reported in this call rather
  // than the next one. That is what the old unconditional pre-drain bought, for
  // a single syscall instead of one per entity.
  const bool poll_only = something_ready;
  {
    struct epoll_event ready_events[64];
    while (true) {
      int block_ms = 0;
      if (!poll_only) {
        block_ms = -1;
        if (!infinite) {
          const int64_t rem_ns = caller_deadline_ns - steady_now_ns();
          const int64_t rem_ms = (rem_ns > 0) ? (rem_ns + 999999) / 1000000 : 0;  // ceil
          block_ms = static_cast<int>(
            std::min<int64_t>(rem_ms, std::numeric_limits<int>::max()));
        }
        if ((graph_gc_unwired || !unarmed.empty()) &&
          (block_ms < 0 || block_ms > 200))
        {
          // No doorbell wiring (registry full), or an entity epoll cannot
          // watch: never block unbounded, or its event is silently lost
          // forever. The early return is a spurious wake (OK, nothing ready)
          // that lets the next rmw_wait retry registration / re-drain.
          block_ms = 200;
        }
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
        break;  // Caller's deadline passed, or the poll-only pass found nothing.
      }
      // Only actual progress ends the wait: an entity the caller waits on now
      // holds data, or a guard condition fired. A wake alone is not enough. A
      // doorbell ring carries no caller work, and a datagram that drain_socket
      // filters out (an ignored local publication, a replayed-duplicate
      // TRANSIENT_LOCAL sample, a foreign message type, an unresolvable shm
      // descriptor) leaves the queue empty. Ending the wait on those would
      // return RMW_RET_OK with nothing ready and wake the executor for every
      // such message.
      bool progressed = false;
      bool rang = false;
      for (int e = 0; e < n; ++e) {
        const int rfd = ready_events[e].data.fd;
        auto it = ws_data->armed.find(rfd);
        if (it == ws_data->armed.end() ||
          armed_this_call.find(rfd) == armed_this_call.end())
        {
          // Not armed by this call: the entity may belong to another wait set
          // now, or may already be destroyed. Never touch it — consuming its
          // event here would steal a wakeup the owning wait set never gets
          // back, and dereferencing a destroyed entity is a use-after-free.
          // Withdraw the fd so a readable level-triggered fd cannot spin this
          // loop; a later call that waits on the entity re-arms it.
          (void)epoll_ctl(ws_data->epoll_fd, EPOLL_CTL_DEL, rfd, nullptr);
          if (it != ws_data->armed.end()) {
            ws_data->armed.erase(it);
          }
          continue;  // Not progress: nothing the caller waits on changed.
        }
        const rmw_uds::ArmedEntry & entry = it->second;
        switch (entry.kind) {
          case rmw_uds::ARMED_SUBSCRIPTION: {
              auto * sub = static_cast<rmw_uds::UdsSubscription *>(entry.entity);
              drain_socket(sub->socket_fd, sub->queue_mutex, sub->message_queue,
                sub->queue_depth, 0, sub->shm_cache, sub->context->domain_id,
                sub->ignore_local_publications, sub->context->context_id,
                &sub->replayed_watermarks);
              std::lock_guard<std::mutex> lock(sub->queue_mutex);
              if (!sub->message_queue.empty()) {
                progressed = true;
              }
              break;
            }
          case rmw_uds::ARMED_SERVICE: {
              auto * srv = static_cast<rmw_uds::UdsService *>(entry.entity);
              drain_socket(srv->socket_fd, srv->queue_mutex, srv->request_queue, 100, 1,
                srv->shm_cache, srv->context->domain_id);
              std::lock_guard<std::mutex> lock(srv->queue_mutex);
              if (!srv->request_queue.empty()) {
                progressed = true;
              }
              break;
            }
          case rmw_uds::ARMED_CLIENT: {
              auto * cli = static_cast<rmw_uds::UdsClient *>(entry.entity);
              drain_socket(cli->socket_fd, cli->queue_mutex, cli->response_queue, 100, 2,
                cli->shm_cache, cli->context->domain_id);
              std::lock_guard<std::mutex> lock(cli->queue_mutex);
              if (!cli->response_queue.empty()) {
                progressed = true;
              }
              break;
            }
          case rmw_uds::ARMED_GUARD_CONDITION: {
              // Consume the trigger here and remember it by the GC's slot in the
              // caller's array, so the output pass reports the right entry even
              // though this read already emptied the eventfd.
              auto * gc = static_cast<rmw_uds::UdsGuardCondition *>(entry.entity);
              uint64_t val;
              const ssize_t r = read(gc->eventfd_fd, &val, sizeof(val));
              if (r == static_cast<ssize_t>(sizeof(val))) {
                progressed = true;
                const auto gi = gc_index.find(gc);
                if (gi != gc_index.end()) {
                  gc_triggered[gi->second] = true;
                }
              }
              break;
            }
          case rmw_uds::ARMED_DOORBELL:
            rang = true;
            break;
          default:
            break;
        }
      }
      if (rang) {
        run_generation_check();  // Drains the doorbell, triggers graph GCs.
      }
      if (poll_only) {
        if (n == 64) {
          continue;  // A full batch: more fds may be ready than one epoll_wait
                     // reports. Terminates because drained fds stop being ready.
        }
        break;  // Non-blocking sweep: a single pass is all it is for.
      }
      if (progressed) {
        break;  // Something the caller waits on became ready.
      }
      if (!infinite && steady_now_ns() >= caller_deadline_ns) {
        break;  // Doorbell-only wake at the deadline -> timeout.
      }
      if (graph_gc_unwired || !unarmed.empty()) {
        break;  // Bounded degraded wait: surface the spurious wake so the
                // caller's next rmw_wait retries registration / re-drains.
      }
      // Doorbell-only wake: re-block for the caller's remaining time.
    }
    // Fds stay registered across calls; the gate above prunes stale ones.
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
      // earlier reads so a GC seen ready then stays reported without a
      // read-back.
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
