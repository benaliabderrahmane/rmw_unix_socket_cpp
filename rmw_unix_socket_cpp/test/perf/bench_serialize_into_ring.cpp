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

// Standalone benchmark (not a ctest): the publish-side staging cost of the
// two large-payload paths, with the real fastCDR serializer.
//
//   old:  serialize() into a fresh heap vector, then shm_prepare_send()
//         (zero-init + CDR write + staging copy)
//   new:  shm_serialize_prepare_send() — CDR writes directly into the
//         reserved ring record
//
// Build with the test suite, run manually:
//   ./build/rmw_unix_socket_cpp/bench_serialize_into_ring

#include <chrono>
#include <cstdio>
#include <mutex>
#include <vector>

#include "test_msgs/msg/unbounded_sequences.hpp"
#include "rosidl_typesupport_cpp/message_type_support.hpp"

#include "../../src/serialization.hpp"
#include "../../src/transport.hpp"

namespace
{
double now_us()
{
  return std::chrono::duration<double, std::micro>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}
}  // namespace

int main()
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  const auto * cb = rmw_uds::get_callbacks(ts);
  if (!cb) {std::fprintf(stderr, "no callbacks\n"); return 1;}

  const size_t domain_id = 94;  // private to this benchmark
  std::mutex mtx;
  rmw_uds::ShmRingWriter ring_old, ring_new;

  std::printf(
    "%-10s %-6s | %14s %14s | %10s %8s\n",
    "payload", "iters", "old us/msg", "new us/msg", "saved us", "speedup");

  for (size_t n : {64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024}) {
    test_msgs::msg::UnboundedSequences msg;
    msg.uint8_values.resize(n);
    for (size_t i = 0; i < n; ++i) {
      msg.uint8_values[i] = static_cast<uint8_t>((i * 131 + 7) & 0xFF);
    }
    const int iters = n >= 1024 * 1024 ? 300 : 1500;

    rmw_uds::WireHeader hdr{};
    rmw_uds::ShmPayloadDescriptor desc{};

    // warmup both rings (segment create, page faults)
    for (int w = 0; w < 10; ++w) {
      std::vector<uint8_t> payload;
      rmw_uds::serialize(&msg, cb, payload);
      rmw_uds::shm_prepare_send(
        ring_old, mtx, domain_id, payload.data(), payload.size(), hdr, desc);
      std::vector<uint8_t> fallback;
      rmw_uds::OutboundPayload wire{nullptr, 0};
      rmw_uds::shm_serialize_prepare_send(
        ring_new, mtx, domain_id, &msg, cb, hdr, desc, fallback, wire);
    }

    double t0 = now_us();
    for (int i = 0; i < iters; ++i) {
      std::vector<uint8_t> payload;  // fresh vector per publish, like rmw_publish
      rmw_uds::serialize(&msg, cb, payload);
      rmw_uds::shm_prepare_send(
        ring_old, mtx, domain_id, payload.data(), payload.size(), hdr, desc);
    }
    const double t_old = (now_us() - t0) / iters;

    t0 = now_us();
    for (int i = 0; i < iters; ++i) {
      std::vector<uint8_t> fallback;
      rmw_uds::OutboundPayload wire{nullptr, 0};
      rmw_uds::shm_serialize_prepare_send(
        ring_new, mtx, domain_id, &msg, cb, hdr, desc, fallback, wire);
    }
    const double t_new = (now_us() - t0) / iters;

    std::printf(
      "%-10zu %-6d | %14.1f %14.1f | %10.1f %7.2fx\n",
      n / 1024, iters, t_old, t_new, t_old - t_new, t_old / t_new);
  }

  rmw_uds::shm_writer_close(ring_old);
  rmw_uds::shm_writer_close(ring_new);
  return 0;
}
