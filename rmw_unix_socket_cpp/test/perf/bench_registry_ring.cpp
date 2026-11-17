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

// Standalone benchmark (not a ctest): the cost of one registry mutation on a
// populated registry (issue #53). Fills <slots> live entries, registers
// <doorbells> bound doorbell sockets after them, frees slot 0, then times
// add + remove of one node in slot 0, so the add's free-slot scan is a single
// CAS and what is left is the mutation itself plus ring_doorbells. "warm"
// repeats back to back; "cold" first walks 64 MiB so the slot array is out of
// cache, as it is between mutations during a fleet launch.
//
// Build with the test suite, run manually:
//   ./build/rmw_unix_socket_cpp/bench_registry_ring [slots=7400] [doorbells=4] [iters=2000]

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "../../src/registry.hpp"

namespace
{
rmw_uds::RegistryEntry make_entry(rmw_uds::RegistryEntryType type, const std::string & name)
{
  rmw_uds::RegistryEntry e;
  std::memset(&e, 0, sizeof(e));
  e.type = type;
  e.pid = getpid();
  std::snprintf(e.node_name, sizeof(e.node_name), "%s", name.c_str());
  if (type == rmw_uds::ENTRY_DOORBELL) {  // only a doorbell needs a path; a remove unlinks it
    std::snprintf(e.socket_path, sizeof(e.socket_path), "%s", name.c_str());
  }
  return e;
}

volatile uint64_t g_sink = 0;

void evict(std::vector<uint64_t> & buf)
{
  uint64_t s = 0;
  for (size_t i = 0; i < buf.size(); i += 8) {
    buf[i] += 1;
    s += buf[i];
  }
  g_sink = g_sink + s;
}

double percentile(std::vector<double> v, double p)
{
  std::sort(v.begin(), v.end());
  return v[static_cast<size_t>(p * static_cast<double>(v.size() - 1))];
}
}  // namespace

int main(int argc, char ** argv)
{
  const int slots = argc > 1 ? std::atoi(argv[1]) : 7400;
  const int doorbells = argc > 2 ? std::atoi(argv[2]) : 4;
  const int iters = argc > 3 ? std::atoi(argv[3]) : 2000;
  const size_t domain = 197;

  shm_unlink("/ros2_uds_197");
  void * ptr = nullptr;
  size_t size = 0;
  const int fd = rmw_uds::registry_open(domain, &ptr, &size);
  if (fd < 0) {
    std::perror("registry_open");
    return 1;
  }
  auto * header = rmw_uds::registry_header(ptr);

  for (int i = 0; i < slots; ++i) {
    rmw_uds::registry_add(header, make_entry(rmw_uds::ENTRY_NODE, "fill" + std::to_string(i)));
  }
  std::vector<int> bells;
  std::vector<std::string> bell_paths;
  for (int d = 0; d < doorbells; ++d) {
    const std::string path = "/tmp/bench_ring_" + std::to_string(getpid()) + "_" + std::to_string(d);
    unlink(path.c_str());
    const int s = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", path.c_str());
    if (s < 0 || bind(s, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
      std::perror("doorbell bind");
      return 1;
    }
    bells.push_back(s);
    bell_paths.push_back(path);
    rmw_uds::registry_add(header, make_entry(rmw_uds::ENTRY_DOORBELL, path));
  }
  rmw_uds::registry_remove(header, 0);  // the churned slot: the add claims it at once
  for (int s : bells) {  // that remove rang them: start from empty queues
    uint8_t b;
    while (recv(s, &b, 1, MSG_DONTWAIT) == 1) {}
  }

  std::vector<uint64_t> big(64u << 17);  // 64 MiB
  uint64_t received = 0;
  for (const bool cold : {false, true}) {
    std::vector<double> ns;
    ns.reserve(static_cast<size_t>(iters) * 2);
    for (int i = 0; i < iters; ++i) {
      for (int pass = 0; pass < 2; ++pass) {
        if (cold) {
          evict(big);
        }
        const auto t0 = std::chrono::steady_clock::now();
        if (pass == 0) {
          if (rmw_uds::registry_add(header, make_entry(rmw_uds::ENTRY_NODE, "churn")) != 0) {
            std::fprintf(stderr, "churn entry did not land in slot 0\n");
            return 1;
          }
        } else {
          rmw_uds::registry_remove(header, 0);
        }
        const auto t1 = std::chrono::steady_clock::now();
        ns.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count());
        uint8_t b;
        for (int s : bells) {
          while (recv(s, &b, 1, MSG_DONTWAIT) == 1) {
            ++received;
          }
        }
      }
    }
    std::printf(
      "%s slots=%d doorbells=%d mutations=%zu  median=%.0f ns  p90=%.0f ns  p99=%.0f ns\n",
      cold ? "cold" : "warm", slots, doorbells, ns.size(),
      percentile(ns, 0.5), percentile(ns, 0.9), percentile(ns, 0.99));
  }
  // Every mutation must ring every doorbell, or the numbers mean nothing.
  const uint64_t expected = static_cast<uint64_t>(iters) * 4 * static_cast<uint64_t>(doorbells);
  std::printf("doorbell octets received %llu of %llu\n",
    static_cast<unsigned long long>(received), static_cast<unsigned long long>(expected));

  for (size_t d = 0; d < bells.size(); ++d) {
    close(bells[d]);
    unlink(bell_paths[d].c_str());
  }
  rmw_uds::registry_close(fd, ptr, size);
  shm_unlink("/ros2_uds_197");
  return received == expected ? 0 : 2;
}
