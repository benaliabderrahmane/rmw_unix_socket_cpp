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

// Standalone benchmark (not a ctest): the fan-in of issue #84 in miniature.
// Five publishers publish one message each, one rmw_wait drains the five
// subscriptions, then each takes its message, through the public RMW API, in one process, on a worker
// thread (as an executor thread runs it, so its allocations use a non-main
// glibc arena). Prints the CPU time and minor page faults per message, which
// is where a payload buffer allocated and freed per message shows up.
//
// Build with the test suite, run manually:
//   ./build/rmw_unix_socket_cpp/bench_take_path
// Syscall counts: strace -c -f ./build/rmw_unix_socket_cpp/bench_take_path

#include <sys/resource.h>
#include <time.h>

#include <cstdio>
#include <cstring>
#include <thread>

#include "rmw/rmw.h"
#include "rmw/init.h"
#include "rmw/init_options.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"
#include "test_msgs/msg/unbounded_sequences.hpp"

namespace
{
constexpr int kTopics = 5;

double thread_cpu_us()
{
  timespec ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

long thread_minflt()
{
  rusage ru;
  getrusage(RUSAGE_THREAD, &ru);
  return ru.ru_minflt;
}
}  // namespace

int main()
{
  rmw_init_options_t options = rmw_get_zero_initialized_init_options();
  rmw_context_t context = rmw_get_zero_initialized_context();
  if (rmw_init_options_init(&options, rcutils_get_default_allocator()) != RMW_RET_OK) {return 1;}
  options.domain_id = 95;  // private to this benchmark
  if (rmw_init(&options, &context) != RMW_RET_OK) {return 1;}
  rmw_node_t * node = rmw_create_node(&context, "bench_take_path", "/");
  if (!node) {return 1;}

  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  rmw_qos_profile_t qos = rmw_qos_profile_default;
  auto pub_opts = rmw_get_default_publisher_options();
  auto sub_opts = rmw_get_default_subscription_options();

  std::printf("%-8s %-6s | %12s %12s\n", "KiB", "msgs", "cpu us/msg", "faults/msg");
  int rc = 0;
  rmw_wait_set_t * ws = rmw_create_wait_set(&context, kTopics);
  if (!ws) {return 1;}
  std::thread worker([&]() {
      for (size_t kib : {16, 32, 128, 256, 1024}) {
        rmw_publisher_t * pubs[kTopics];
        rmw_subscription_t * subs[kTopics];
        for (int t = 0; t < kTopics; ++t) {
          char topic[32];
          std::snprintf(topic, sizeof(topic), "/bench_%zu_%d", kib, t);
          pubs[t] = rmw_create_publisher(node, ts, topic, &qos, &pub_opts);
          subs[t] = rmw_create_subscription(node, ts, topic, &qos, &sub_opts);
          if (!pubs[t] || !subs[t]) {rc = 1; return;}
        }
        rmw_message_info_t info;
        const int rounds = kib >= 1024 ? 400 : 2000;
        // A fresh message on each side, as an application publishing a new
        // message and rclcpp's default message memory strategy taking into a
        // new one do: the heap sees their buffers come and go alongside ours.
        auto round = [&]() {
            for (int t = 0; t < kTopics; ++t) {
              test_msgs::msg::UnboundedSequences out;
              out.uint8_values.assign(kib * 1024, 0x5a);
              if (rmw_publish(pubs[t], &out, nullptr) != RMW_RET_OK) {rc = 1;}
            }
            // An executor's rmw_wait drains every ready subscription before
            // it takes from any, so their payloads are all live at once.
            void * ready[kTopics];
            for (int t = 0; t < kTopics; ++t) {ready[t] = subs[t]->data;}
            rmw_subscriptions_t sub_set{kTopics, ready};
            rmw_time_t zero{0, 0};
            if (rmw_wait(&sub_set, nullptr, nullptr, nullptr, nullptr, ws, &zero) != RMW_RET_OK) {
              rc = 1;
            }
            for (int t = 0; t < kTopics; ++t) {
              bool taken = false;
              test_msgs::msg::UnboundedSequences in;
              if (rmw_take_with_info(subs[t], &in, &taken, &info, nullptr) != RMW_RET_OK ||
                !taken)
              {
                rc = 1;
              }
            }
          };
        for (int i = 0; i < 20; ++i) {round();}  // warm up rings, caches
        const long f0 = thread_minflt();
        const double t0 = thread_cpu_us();
        for (int i = 0; i < rounds; ++i) {round();}
        const int msgs = rounds * kTopics;
        const double cpu = (thread_cpu_us() - t0) / msgs;
        const double faults = static_cast<double>(thread_minflt() - f0) / msgs;
        std::printf("%-8zu %-6d | %12.2f %12.2f\n", kib, msgs, cpu, faults);
        for (int t = 0; t < kTopics; ++t) {
          rmw_destroy_subscription(node, subs[t]);
          rmw_destroy_publisher(node, pubs[t]);
        }
      }
    });
  worker.join();

  rmw_destroy_wait_set(ws);
  rmw_destroy_node(node);
  rmw_shutdown(&context);
  rmw_context_fini(&context);
  rmw_init_options_fini(&options);
  return rc;
}
