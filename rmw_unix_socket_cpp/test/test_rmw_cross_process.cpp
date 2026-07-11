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

// Cross-process integration tests: every other binary in this suite runs
// publisher and subscriber (or service and client) inside ONE process, so the
// shared-memory path is only ever exercised against the process's own
// segments. These tests fork() a real second process and push >4 MB payloads
// across the PID boundary — above the kernel's single-datagram cap, so a
// byte-identical arrival proves the bytes crossed through /dev/shm, not the
// socket.
//
// Choreography rules that keep this safe and CI-friendly:
//  - fork() happens BEFORE either process touches rmw, so no registry mapping,
//    socket fd, or robust-mutex state is ever inherited mid-flight.
//  - The child never uses gtest assertions; it reports through _exit() codes
//    (listed below) and the parent asserts on the wait status. _exit skips
//    atexit/gtest teardown that belongs to the parent.
//  - Every wait is bounded (poll() on pipes, take loops with deadlines) and
//    the child arms alarm() as a last-resort kill, so a wedged run fails
//    instead of hanging CI. The parent reaps with a bounded waitpid loop and
//    SIGKILLs on expiry.

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstring>
#include <string>

#include "rmw/rmw.h"
#include "rmw/init.h"
#include "rmw/init_options.h"
#include "rmw/qos_profiles.h"

#include "test_msgs/msg/unbounded_sequences.hpp"
#include "test_msgs/srv/basic_types.hpp"
#include "rosidl_typesupport_cpp/message_type_support.hpp"
#include "rosidl_typesupport_cpp/service_type_support.hpp"

// Child exit codes, asserted by the parent.
static constexpr int kChildOk = 0;
static constexpr int kChildPipeTimeout = 2;
static constexpr int kChildInitFailed = 3;
static constexpr int kChildEntityFailed = 4;
static constexpr int kChildRecvTimeout = 5;
static constexpr int kChildPayloadMismatch = 6;

namespace
{

constexpr size_t kBigSize = 5 * 1024 * 1024;  // > the ~4 MB datagram cap

// Minimal per-process rmw bring-up (the fixture can't be used: each process
// must init after the fork, never before). The destructor makes early ASSERT
// returns in the parent clean up its context too.
struct ProcContext
{
  rmw_context_t context = rmw_get_zero_initialized_context();
  rmw_init_options_t options = rmw_get_zero_initialized_init_options();
  rmw_node_t * node = nullptr;
  bool live = false;

  bool init(size_t domain_id, const char * node_name)
  {
    if (rmw_init_options_init(&options, rcutils_get_default_allocator()) != RMW_RET_OK) {
      return false;
    }
    options.domain_id = domain_id;
    if (rmw_init(&options, &context) != RMW_RET_OK) {
      return false;
    }
    live = true;
    node = rmw_create_node(&context, node_name, "/xproc");
    return node != nullptr;
  }

  void fini()
  {
    if (!live) {return;}
    live = false;
    if (node) {auto _r [[maybe_unused]] = rmw_destroy_node(node);}
    auto _s [[maybe_unused]] = rmw_shutdown(&context);
    auto _f [[maybe_unused]] = rmw_context_fini(&context);
    auto _o [[maybe_unused]] = rmw_init_options_fini(&options);
  }

  ~ProcContext() {fini();}
};

// Kills and reaps the forked child if the parent bails out of the test early
// (a failed ASSERT would otherwise leave a live orphan that stalls ctest on
// the inherited stdio pipe and can poison an immediate re-run). Disarmed once
// the child has been reaped normally — the pid may be recycled after that, so
// the destructor must never fire on a reaped pid.
struct ChildGuard
{
  pid_t pid;
  explicit ChildGuard(pid_t p)
  : pid(p) {}
  void disarm() {pid = -1;}
  ~ChildGuard()
  {
    if (pid > 0) {
      kill(pid, SIGKILL);
      waitpid(pid, nullptr, 0);
    }
  }
};

// One-byte pipe handshake with a bounded wait.
bool wait_byte(int fd, int timeout_ms)
{
  struct pollfd pfd{fd, POLLIN, 0};
  if (poll(&pfd, 1, timeout_ms) != 1) {
    return false;
  }
  char b;
  return read(fd, &b, 1) == 1;
}

void send_byte(int fd)
{
  char b = 'x';
  auto _w [[maybe_unused]] = write(fd, &b, 1);
}

// Bounded reap: returns the child's exit code, or -1 on timeout/signal (the
// child is SIGKILLed on expiry so a wedged test can never hang the suite).
int reap_child(pid_t pid, int timeout_sec)
{
  for (int i = 0; i < timeout_sec * 20; ++i) {
    int status = 0;
    pid_t r = waitpid(pid, &status, WNOHANG);
    if (r == pid) {
      return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }
    usleep(50 * 1000);
  }
  kill(pid, SIGKILL);
  waitpid(pid, nullptr, 0);
  return -1;
}

rmw_qos_profile_t make_qos(rmw_qos_durability_policy_e durability)
{
  rmw_qos_profile_t qos;
  std::memset(&qos, 0, sizeof(qos));
  qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
  qos.depth = 5;
  qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  qos.durability = durability;
  return qos;
}

uint8_t big_pattern(size_t i)
{
  return static_cast<uint8_t>((i * 131 + 7) & 0xFF);
}

}  // namespace

TEST(CrossProcessTest, LargeTransientLocalLateJoiner)
{
  // Parent latches a 5 MB message (staged in a durable shm segment), then a
  // subscriber in a SECOND process joins late and must receive it byte-equal:
  // the child maps the parent's durable segment across the PID boundary.
  // Domain 91: private to this test (the single-process suite uses 99).
  constexpr size_t kDomain = 91;
  signal(SIGPIPE, SIG_IGN);  // a dead child must fail the test, not the binary
  int published[2], subscribed[2];
  ASSERT_EQ(0, pipe(published));
  ASSERT_EQ(0, pipe(subscribed));

  pid_t pid = fork();
  ASSERT_GE(pid, 0);

  if (pid == 0) {
    // ---- child: late-joining subscriber ----
    alarm(45);  // last-resort kill; every wait below is already bounded
    close(published[1]);
    close(subscribed[0]);
    if (!wait_byte(published[0], 15000)) {_exit(kChildPipeTimeout);}

    ProcContext ctx;
    if (!ctx.init(kDomain, "xproc_sub")) {_exit(kChildInitFailed);}
    auto ts = rosidl_typesupport_cpp::get_message_type_support_handle<
      test_msgs::msg::UnboundedSequences>();
    auto qos = make_qos(RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL);
    auto sub_opts = rmw_get_default_subscription_options();
    auto * sub = rmw_create_subscription(ctx.node, ts, "/xproc_latched", &qos, &sub_opts);
    if (!sub) {_exit(kChildEntityFailed);}
    send_byte(subscribed[1]);  // registration is synchronous: parent may replay now

    // The replayed 5 MB message (and later the small trigger) land on our
    // socket whenever the parent notices the graph change; poll rmw_take.
    test_msgs::msg::UnboundedSequences msg;
    for (int i = 0; i < 15 * 50; ++i) {  // <= 15 s
      bool taken = false;
      if (rmw_take(sub, &msg, &taken, nullptr) == RMW_RET_OK && taken &&
        msg.uint8_values.size() == kBigSize)
      {
        for (size_t j = 0; j < kBigSize; ++j) {
          if (msg.uint8_values[j] != big_pattern(j)) {_exit(kChildPayloadMismatch);}
        }
        // Clean teardown on success so no socket file outlives the child;
        // failure paths _exit directly and rely on the orphan sweeps, which
        // is exactly the ungraceful-exit story they simulate.
        auto _r [[maybe_unused]] = rmw_destroy_subscription(ctx.node, sub);
        ctx.fini();
        _exit(kChildOk);
      }
      usleep(20 * 1000);
    }
    _exit(kChildRecvTimeout);
  }

  // ---- parent: latching publisher ----
  ChildGuard guard(pid);
  close(published[0]);
  close(subscribed[1]);

  ProcContext ctx;
  ASSERT_TRUE(ctx.init(kDomain, "xproc_pub"));
  auto ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto qos = make_qos(RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL);
  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(ctx.node, ts, "/xproc_latched", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  test_msgs::msg::UnboundedSequences big;
  big.uint8_values.resize(kBigSize);
  for (size_t i = 0; i < kBigSize; ++i) {
    big.uint8_values[i] = big_pattern(i);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &big, nullptr));  // before the sub exists
  send_byte(published[1]);

  // Once the child's subscription is registered, a small trigger publish makes
  // this process observe the graph change and replay the cached 5 MB message.
  ASSERT_TRUE(wait_byte(subscribed[0], 15000)) << "child never subscribed";
  test_msgs::msg::UnboundedSequences trigger;
  trigger.uint8_values = {1, 2, 3};
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &trigger, nullptr));

  int code = reap_child(pid, 30);
  guard.disarm();  // reaped either way; the pid may be recycled from here on
  EXPECT_EQ(kChildOk, code)
    << "child exit " << code << " (2=pipe timeout, 3=init, 4=entity, "
    << "5=recv timeout, 6=payload mismatch, -1=killed/signalled)";

  auto _p [[maybe_unused]] = rmw_destroy_publisher(ctx.node, pub);
  ctx.fini();
  close(published[1]);
  close(subscribed[0]);
}

TEST(CrossProcessTest, LargeServiceRoundTrip)
{
  // A 5 MB request and a 5 MB response cross the PID boundary: the service
  // process maps the client's ring for the request, and the client process
  // maps the service's ring for the response. Domain 92: private to this test.
  constexpr size_t kDomain = 92;
  signal(SIGPIPE, SIG_IGN);  // a dead child must fail the test, not the binary
  int ready[2];
  ASSERT_EQ(0, pipe(ready));

  const auto fill = [](std::string & s, char base) {
      for (size_t i = 0; i < s.size(); ++i) {
        s[i] = static_cast<char>(base + (i % 26));
      }
    };

  pid_t pid = fork();
  ASSERT_GE(pid, 0);

  if (pid == 0) {
    // ---- child: client ----
    alarm(45);
    close(ready[1]);
    if (!wait_byte(ready[0], 15000)) {_exit(kChildPipeTimeout);}

    ProcContext ctx;
    if (!ctx.init(kDomain, "xproc_client")) {_exit(kChildInitFailed);}
    auto ts = rosidl_typesupport_cpp::get_service_type_support_handle<
      test_msgs::srv::BasicTypes>();
    auto qos = make_qos(RMW_QOS_POLICY_DURABILITY_VOLATILE);
    auto * cli = rmw_create_client(ctx.node, ts, "/xproc_srv", &qos);
    if (!cli) {_exit(kChildEntityFailed);}

    test_msgs::srv::BasicTypes::Request request;
    request.int32_value = 7;
    request.string_value.resize(kBigSize);
    fill(request.string_value, 'A');
    int64_t seq = 0;
    if (rmw_send_request(cli, &request, &seq) != RMW_RET_OK) {_exit(kChildEntityFailed);}

    std::string expected_resp(kBigSize, '\0');
    fill(expected_resp, 'a');
    test_msgs::srv::BasicTypes::Response resp;
    rmw_service_info_t info;
    for (int i = 0; i < 20 * 50; ++i) {  // <= 20 s
      bool taken = false;
      std::memset(&info, 0, sizeof(info));
      if (rmw_take_response(cli, &info, &resp, &taken) == RMW_RET_OK && taken) {
        if (resp.int32_value != 9 || resp.string_value != expected_resp) {
          _exit(kChildPayloadMismatch);
        }
        // Clean teardown on success (unlinks the client's request ring); the
        // failure paths _exit directly and rely on the orphan sweeps.
        auto _r [[maybe_unused]] = rmw_destroy_client(ctx.node, cli);
        ctx.fini();
        _exit(kChildOk);
      }
      usleep(20 * 1000);
    }
    _exit(kChildRecvTimeout);
  }

  // ---- parent: service ----
  ChildGuard guard(pid);
  close(ready[0]);

  ProcContext ctx;
  ASSERT_TRUE(ctx.init(kDomain, "xproc_service"));
  auto ts = rosidl_typesupport_cpp::get_service_type_support_handle<
    test_msgs::srv::BasicTypes>();
  auto qos = make_qos(RMW_QOS_POLICY_DURABILITY_VOLATILE);
  auto * srv = rmw_create_service(ctx.node, ts, "/xproc_srv", &qos);
  ASSERT_NE(nullptr, srv);
  send_byte(ready[1]);

  std::string expected_req(kBigSize, '\0');
  fill(expected_req, 'A');
  test_msgs::srv::BasicTypes::Request req;
  rmw_service_info_t info;
  bool got_request = false;
  for (int i = 0; i < 20 * 50 && !got_request; ++i) {  // <= 20 s
    bool taken = false;
    std::memset(&info, 0, sizeof(info));
    ASSERT_EQ(RMW_RET_OK, rmw_take_request(srv, &info, &req, &taken));
    if (taken) {got_request = true;} else {usleep(20 * 1000);}
  }
  ASSERT_TRUE(got_request) << "5 MB request never arrived from the client process";
  EXPECT_EQ(7, req.int32_value);
  EXPECT_EQ(expected_req, req.string_value);

  test_msgs::srv::BasicTypes::Response resp;
  resp.int32_value = 9;
  resp.string_value.resize(kBigSize);
  fill(resp.string_value, 'a');
  EXPECT_EQ(RMW_RET_OK, rmw_send_response(srv, &info.request_id, &resp));

  int code = reap_child(pid, 30);
  guard.disarm();  // reaped either way; the pid may be recycled from here on
  EXPECT_EQ(kChildOk, code)
    << "child exit " << code << " (2=pipe timeout, 3=init, 4=entity, "
    << "5=recv timeout, 6=payload mismatch, -1=killed/signalled)";

  auto _s [[maybe_unused]] = rmw_destroy_service(ctx.node, srv);
  ctx.fini();
  close(ready[1]);
}
