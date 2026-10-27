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

#include "test_base.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "test_msgs/msg/basic_types.hpp"

#include "rmw/error_handling.h"
#include "rmw/event.h"
#include "rmw/events_statuses/events_statuses.h"
#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"

#include "shm_transport.hpp"
#include "types.hpp"

class EventTest : public RmwUdsNodeTest
{
protected:
  rmw_publisher_t * pub = nullptr;
  rmw_subscription_t * sub = nullptr;
  const rosidl_message_type_support_t * ts = nullptr;
  rmw_qos_profile_t qos;

  void SetUp() override
  {
    RmwUdsNodeTest::SetUp();
    ts = rosidl_typesupport_cpp::get_message_type_support_handle<
      test_msgs::msg::BasicTypes>();
    std::memset(&qos, 0, sizeof(qos));
    qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    qos.depth = 10;
    qos.reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
    qos.durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;

    auto pub_opts = rmw_get_default_publisher_options();
    pub = rmw_create_publisher(node, ts, "/event_test_topic", &qos, &pub_opts);
    ASSERT_NE(nullptr, pub);

    auto sub_opts = rmw_get_default_subscription_options();
    sub = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
    ASSERT_NE(nullptr, sub);
  }

  void TearDown() override
  {
    if (sub) { auto _r [[maybe_unused]] = rmw_destroy_subscription(node, sub); }
    if (pub) { auto _r [[maybe_unused]] = rmw_destroy_publisher(node, pub); }
    rmw_reset_error();
    RmwUdsNodeTest::TearDown();
  }

  // An event handle stamped as ours, for the entry points that validate one.
  rmw_event_t our_event(rmw_event_type_t type) const
  {
    rmw_event_t event = rmw_get_zero_initialized_event();
    event.implementation_identifier = uds_id();
    event.data = sub->data;
    event.event_type = type;
    return event;
  }

  void publish(int32_t value)
  {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = value;
    ASSERT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  // Blocks in rmw_wait on `sub` alone until it is ready, so its socket is
  // drained; the message is left queued.
  void wait_on_sub(rmw_subscription_t * s)
  {
    rmw_wait_set_t * ws = rmw_create_wait_set(&context, 1);
    ASSERT_NE(nullptr, ws);
    void * arr[1] = {s->data};
    rmw_subscriptions_t subs{1, arr};
    rmw_time_t timeout{2, 0};
    EXPECT_EQ(RMW_RET_OK, rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &timeout));
    EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  }
};

struct EventCounter
{
  std::atomic<size_t> calls{0};
  std::atomic<size_t> events{0};
  static void fire(const void * user_data, size_t n)
  {
    auto * self = const_cast<EventCounter *>(static_cast<const EventCounter *>(user_data));
    self->calls.fetch_add(1);
    self->events.fetch_add(n);
  }
};

// Runs on every exit, so a failed ASSERT cannot leave a joinable thread
// (std::terminate) or a callback pointing at a stack object.
struct OnExit
{
  std::function<void()> f;
  ~OnExit() {f();}
};

// Spins rmw_wait on the node's graph guard condition in its own thread, as
// rclcpp's GraphListener does. Every rmw_wait drains the context's doorbell,
// so this competes for every ring.
struct GraphWaiter
{
  std::atomic<bool> stop{false};
  rmw_wait_set_t * ws = nullptr;
  std::thread thread;
  GraphWaiter(rmw_context_t & context, const rmw_node_t * node)
  {
    ws = rmw_create_wait_set(&context, 1);
    const rmw_guard_condition_t * gc = rmw_node_get_graph_guard_condition(node);
    thread = std::thread([this, gc]() {
        while (!stop.load()) {
          void * arr[1] = {gc->data};
          rmw_guard_conditions_t gcs{1, arr};
          rmw_time_t timeout{0, 20000000};
          (void)rmw_wait(nullptr, &gcs, nullptr, nullptr, nullptr, ws, &timeout);
        }
      });
  }
  ~GraphWaiter()
  {
    stop.store(true);
    thread.join();
    (void)rmw_destroy_wait_set(ws);
  }
};

static bool await_calls(const EventCounter & c, size_t target, int budget_ms = 2000)
{
  for (int waited = 0; waited < budget_ms; waited += 5) {
    if (c.calls.load() >= target) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return c.calls.load() >= target;
}

// Puts one forged datagram on an endpoint's socket, laid out as send_to()
// does (header, then payload), for states a real publisher cannot be made to
// produce on cue. send_to() itself is not exported.
static bool send_raw(
  const std::string & path, const rmw_uds::WireHeader & hdr, const void * payload, size_t size)
{
  const int fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return false;
  }
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
  iovec iov[2] = {
    {const_cast<rmw_uds::WireHeader *>(&hdr), sizeof(hdr)},
    {const_cast<void *>(payload), size}};
  msghdr msg{};
  msg.msg_name = &addr;
  msg.msg_namelen = sizeof(addr);
  msg.msg_iov = iov;
  msg.msg_iovlen = 2;
  const bool sent = sendmsg(fd, &msg, MSG_NOSIGNAL) == static_cast<ssize_t>(sizeof(hdr) + size);
  close(fd);
  return sent;
}

// Iterating the whole enum rather than naming types keeps this honest across
// the distro matrix: the ordinals shift as upstream adds event types, and
// RMW_EVENT_INVALID is the last enumerator on Jazzy but the first (0) from
// Kilted on - so the bound is whichever of it and the last real type is larger.
static constexpr int LAST_EVENT_TYPE =
  std::max<int>(RMW_EVENT_INVALID, RMW_EVENT_PUBLICATION_MATCHED);

static bool is_subscription_type(rmw_event_type_t t)
{
  return t == RMW_EVENT_MESSAGE_LOST || t == RMW_EVENT_SUBSCRIPTION_MATCHED;
}

// message_lost and both matched events need no clock; deadline and liveliness
// do, and incompatible QoS and type would report a delivery failure this
// transport does not have. Every refused type must be refused through
// *_event_init with RMW_RET_UNSUPPORTED, the one failure rclcpp swallows
// (UnsupportedEventTypeException); any other failure surfaces later,
// unhandled, when the executor registers its callback.
TEST_F(EventTest, OnlyClockFreeEventTypesAreSupported)
{
  for (int i = 0; i <= LAST_EVENT_TYPE; ++i) {
    const auto type = static_cast<rmw_event_type_t>(i);
    const bool expected = is_subscription_type(type) || type == RMW_EVENT_PUBLICATION_MATCHED;
    EXPECT_EQ(expected, rmw_event_type_is_supported(type)) << "event type " << i;
  }
}

TEST_F(EventTest, SubscriptionEventInitAcceptsOnlyItsTypes)
{
  for (int i = 0; i <= LAST_EVENT_TYPE; ++i) {
    const auto type = static_cast<rmw_event_type_t>(i);
    rmw_event_t event = rmw_get_zero_initialized_event();
    if (is_subscription_type(type)) {
      EXPECT_EQ(RMW_RET_OK, rmw_subscription_event_init(&event, sub, type)) << "event type " << i;
      EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&event)) << "event type " << i;
      continue;
    }
    EXPECT_EQ(RMW_RET_UNSUPPORTED, rmw_subscription_event_init(&event, sub, type))
      << "event type " << i;
    // rcl logs whatever the RMW left behind, so a bare return code is a
    // dead end for anyone debugging the rejection.
    EXPECT_TRUE(rmw_error_is_set()) << "event type " << i;
    rmw_reset_error();
  }
}

TEST_F(EventTest, PublisherEventInitAcceptsOnlyPublicationMatched)
{
  for (int i = 0; i <= LAST_EVENT_TYPE; ++i) {
    const auto type = static_cast<rmw_event_type_t>(i);
    rmw_event_t event = rmw_get_zero_initialized_event();
    if (type == RMW_EVENT_PUBLICATION_MATCHED) {
      EXPECT_EQ(RMW_RET_OK, rmw_publisher_event_init(&event, pub, type));
      EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&event));
      continue;
    }
    EXPECT_EQ(RMW_RET_UNSUPPORTED, rmw_publisher_event_init(&event, pub, type))
      << "event type " << i;
    EXPECT_TRUE(rmw_error_is_set()) << "event type " << i;
    rmw_reset_error();
  }
}

// A rejected init leaves the handle exactly as rmw_get_zero_initialized_event()
// made it, so a failed EventHandler construction cannot leave rcl holding a
// handle that looks like ours.
TEST_F(EventTest, RejectedEventInitLeavesTheHandleZeroInitialized)
{
  rmw_event_t event = rmw_get_zero_initialized_event();
  ASSERT_EQ(
    RMW_RET_UNSUPPORTED,
    rmw_subscription_event_init(&event, sub, RMW_EVENT_REQUESTED_DEADLINE_MISSED));
  rmw_reset_error();

  EXPECT_EQ(nullptr, event.implementation_identifier);
  EXPECT_EQ(nullptr, event.data);
}

TEST_F(EventTest, EventInitRejectsForeignEndpoints)
{
  rmw_subscription_t foreign_sub = *sub;
  foreign_sub.implementation_identifier = "rmw_bogus_cpp";
  rmw_event_t event = rmw_get_zero_initialized_event();
  EXPECT_EQ(
    RMW_RET_INCORRECT_RMW_IMPLEMENTATION,
    rmw_subscription_event_init(&event, &foreign_sub, RMW_EVENT_MESSAGE_LOST));
  rmw_reset_error();

  rmw_publisher_t foreign_pub = *pub;
  foreign_pub.implementation_identifier = "rmw_bogus_cpp";
  event = rmw_get_zero_initialized_event();
  EXPECT_EQ(
    RMW_RET_INCORRECT_RMW_IMPLEMENTATION,
    rmw_publisher_event_init(&event, &foreign_pub, RMW_EVENT_PUBLICATION_MATCHED));
  rmw_reset_error();
}

TEST_F(EventTest, SetOnNewMessageCallbackRejectsForeignSubscription)
{
  rmw_subscription_t foreign_sub = *sub;
  foreign_sub.implementation_identifier = "rmw_bogus_cpp";
  EXPECT_EQ(
    RMW_RET_INCORRECT_RMW_IMPLEMENTATION,
    rmw_subscription_set_on_new_message_callback(&foreign_sub, nullptr, nullptr));
  rmw_reset_error();
}

TEST_F(EventTest, EventInitRejectsNullArguments)
{
  rmw_event_t event = rmw_get_zero_initialized_event();
  EXPECT_EQ(
    RMW_RET_INVALID_ARGUMENT,
    rmw_subscription_event_init(nullptr, sub, RMW_EVENT_MESSAGE_LOST));
  rmw_reset_error();
  EXPECT_EQ(
    RMW_RET_INVALID_ARGUMENT,
    rmw_subscription_event_init(&event, nullptr, RMW_EVENT_MESSAGE_LOST));
  rmw_reset_error();
  EXPECT_EQ(
    RMW_RET_INVALID_ARGUMENT,
    rmw_publisher_event_init(nullptr, pub, RMW_EVENT_PUBLICATION_MATCHED));
  rmw_reset_error();
  EXPECT_EQ(
    RMW_RET_INVALID_ARGUMENT,
    rmw_publisher_event_init(&event, nullptr, RMW_EVENT_PUBLICATION_MATCHED));
  rmw_reset_error();
}

// Like the DDS RMWs, a take always succeeds: rcl turns taken == false into
// RCL_RET_EVENT_TAKE_FAILED, which rclcpp logs as an error, and a
// MultiThreadedExecutor can race two takes of one ready event.
TEST_F(EventTest, TakeEventWithNothingNewReportsZeroChanges)
{
  rmw_event_t event = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&event, sub, RMW_EVENT_MESSAGE_LOST));

  rmw_message_lost_status_t st;
  std::memset(&st, 0xAB, sizeof(st));
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take_event(&event, &st, &taken));
  EXPECT_TRUE(taken);
  EXPECT_EQ(0u, st.total_count);
  EXPECT_EQ(0u, st.total_count_change);
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&event));
}

TEST_F(EventTest, TakeEventRejectsBadArguments)
{
  rmw_event_t event = our_event(RMW_EVENT_MESSAGE_LOST);
  uint8_t info[64] = {};
  bool taken = false;

  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_take_event(nullptr, info, &taken));
  rmw_reset_error();
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_take_event(&event, nullptr, &taken));
  rmw_reset_error();
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_take_event(&event, info, nullptr));
  rmw_reset_error();

  rmw_event_t foreign = event;
  foreign.implementation_identifier = "rmw_bogus_cpp";
  EXPECT_EQ(RMW_RET_INCORRECT_RMW_IMPLEMENTATION, rmw_take_event(&foreign, info, &taken));
  rmw_reset_error();
}

// rcl never reaches this RMW's rmw_event_fini - librmw implements it, and
// rmw_implementation does not forward it - but a direct caller's fini of a
// handle no init filled in must still be harmless.
TEST_F(EventTest, EventFiniOfAnUninitializedHandleIsANoOp)
{
  rmw_event_t zero = rmw_get_zero_initialized_event();
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&zero));
  EXPECT_FALSE(rmw_error_is_set());
}

// The original EventsExecutor crash surfaced as "failed to set the on new
// message callback for Event: error not set" - the ": error not set" half
// being this function returning a failure with no message behind it.
TEST_F(EventTest, EventSetCallbackReportsUnsupportedWithAMessage)
{
  rmw_event_t event = our_event(RMW_EVENT_LIVELINESS_CHANGED);
  EXPECT_EQ(RMW_RET_UNSUPPORTED, rmw_event_set_callback(&event, nullptr, nullptr));
  EXPECT_TRUE(rmw_error_is_set());
  rmw_reset_error();

  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_event_set_callback(nullptr, nullptr, nullptr));
  rmw_reset_error();

  // The identifier check the other event entry points already had.
  rmw_event_t foreign = our_event(RMW_EVENT_MESSAGE_LOST);
  foreign.implementation_identifier = "rmw_bogus_cpp";
  EXPECT_EQ(
    RMW_RET_INCORRECT_RMW_IMPLEMENTATION,
    rmw_event_set_callback(&foreign, nullptr, nullptr));
  rmw_reset_error();
}

// --- matched ---------------------------------------------------------------

// The fixture's publisher and subscription already share a topic, so each
// side's matched event is created into one match and reports it, as DDS does.
TEST_F(EventTest, MatchedReportsThePeerAnEndpointIsCreatedInto)
{
  rmw_event_t pub_ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&pub_ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  rmw_event_t sub_ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&sub_ev, sub, RMW_EVENT_SUBSCRIPTION_MATCHED));

  for (rmw_event_t * ev : {&pub_ev, &sub_ev}) {
    rmw_matched_status_t st{};
    bool taken = false;
    ASSERT_EQ(RMW_RET_OK, rmw_take_event(ev, &st, &taken));
    ASSERT_TRUE(taken);
    EXPECT_EQ(1u, st.current_count);
    EXPECT_EQ(1, st.current_count_change);
    EXPECT_EQ(1u, st.total_count);
    EXPECT_EQ(1u, st.total_count_change);

    // Taking resets the changes; the next take reports the same match, unchanged.
    ASSERT_EQ(RMW_RET_OK, rmw_take_event(ev, &st, &taken));
    ASSERT_TRUE(taken);
    EXPECT_EQ(1u, st.current_count);
    EXPECT_EQ(0, st.current_count_change);
    EXPECT_EQ(1u, st.total_count);
    EXPECT_EQ(0u, st.total_count_change);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&pub_ev));
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&sub_ev));
}

TEST_F(EventTest, PublicationMatchedTracksSubscriptionsJoiningAndLeaving)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  rmw_matched_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));  // the fixture's subscription
  ASSERT_TRUE(taken);

  auto sub_opts = rmw_get_default_subscription_options();
  auto * second = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, second);
  // A subscription on another topic is no peer.
  auto * other = rmw_create_subscription(node, ts, "/event_other_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, other);

  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  ASSERT_TRUE(taken);
  EXPECT_EQ(2u, st.current_count);
  EXPECT_EQ(1, st.current_count_change);
  EXPECT_EQ(2u, st.total_count);
  EXPECT_EQ(1u, st.total_count_change);

  ASSERT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, second));
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  ASSERT_TRUE(taken);
  EXPECT_EQ(1u, st.current_count);
  EXPECT_EQ(-1, st.current_count_change);
  EXPECT_EQ(2u, st.total_count);  // cumulative: leaving never lowers it
  EXPECT_EQ(0u, st.total_count_change);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, other));
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// image_pipeline's lazy nodes block in a wait holding only their publisher's
// matched event until a subscriber shows up. The wait must wake on the
// registry change, which takes the doorbell a graph guard condition uses.
TEST_F(EventTest, MatchedEventWakesABlockedWait)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  rmw_matched_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));  // the existing match
  ASSERT_TRUE(taken);

  rmw_wait_set_t * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);
  std::atomic<rmw_ret_t> wait_ret{RMW_RET_ERROR};
  std::atomic<bool> was_ready{false};
  std::thread waiter([&]() {
      void * arr[1] = {&ev};
      rmw_events_t events{1, arr};
      rmw_time_t timeout{5, 0};
      wait_ret = rmw_wait(nullptr, nullptr, nullptr, nullptr, &events, ws, &timeout);
      was_ready = arr[0] != nullptr;
    });
  OnExit join_waiter{[&]() {if (waiter.joinable()) {waiter.join();}}};
  std::this_thread::sleep_for(std::chrono::milliseconds(100));  // let it block

  auto sub_opts = rmw_get_default_subscription_options();
  auto * second = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, second);
  waiter.join();

  EXPECT_EQ(RMW_RET_OK, wait_ret.load());
  EXPECT_TRUE(was_ready.load()) << "the wait did not report the matched event";
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_TRUE(taken);
  EXPECT_EQ(2u, st.current_count);

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, second));
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// An EventsExecutor never waits: the listener thread has to hear the registry
// change and fire the callback itself.
TEST_F(EventTest, MatchedCallbackFiresWithNoThreadInWait)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  EventCounter counter;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &counter));
  OnExit clear{[&]() {(void)rmw_event_set_callback(&ev, nullptr, nullptr);}};
  // The existing match was paid out at registration, as one take credit.
  EXPECT_EQ(1u, counter.calls.load());
  EXPECT_EQ(1u, counter.events.load());
  rmw_matched_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));  // what the executor does

  auto sub_opts = rmw_get_default_subscription_options();
  auto * second = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, second);
  EXPECT_TRUE(await_calls(counter, 2)) << "no matched callback without rmw_wait";

  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_TRUE(taken);
  EXPECT_EQ(2u, st.current_count);
  EXPECT_EQ(1, st.current_count_change);

  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, second));
}

// rclcpp sets an event callback twice in a row on purpose (a stack temporary,
// then its permanent storage); the unread change must be paid out once.
TEST_F(EventTest, ReRegisteringAMatchedCallbackDoesNotRepayIt)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  EventCounter counter;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &counter));
  OnExit clear{[&]() {(void)rmw_event_set_callback(&ev, nullptr, nullptr);}};
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &counter));
  EXPECT_EQ(1u, counter.calls.load());
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// --- message_lost ----------------------------------------------------------

// Skips the publisher's sequence numbers ahead, which is what a subscriber sees
// when datagrams never arrive (dropped by the kernel or by the sender on a full
// socket) - without depending on making the kernel drop any.
static void skip_publications(rmw_publisher_t * pub, int64_t n)
{
  static_cast<rmw_uds::UdsPublisher *>(pub->data)->sequence_number.fetch_add(n);
}

// rclcpp registers an event callback twice. A registration that keeps the
// callback must still announce a change it is first to read: the listener,
// finding that registry generation already read, will not.
TEST_F(EventTest, AChangeReadByAReRegistrationIsStillAnnounced)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  EventCounter counter;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &counter));
  OnExit clear{[&]() {(void)rmw_event_set_callback(&ev, nullptr, nullptr);}};
  rmw_matched_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));  // the existing match

  auto sub_opts = rmw_get_default_subscription_options();
  auto * second = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, second);
  OnExit destroy{[&]() {(void)rmw_destroy_subscription(node, second);}};
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &counter));
  EXPECT_TRUE(await_calls(counter, 2)) << "the re-registration absorbed the new match";

  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(1, st.current_count_change);
}

// Event state is per endpoint and type, so a second handle on one endpoint
// shares the first one's callback. Its init may be first to read a change,
// and must announce it: the listener, finding that generation read, will not.
TEST_F(EventTest, AChangeReadByASecondHandlesInitIsStillAnnounced)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  EventCounter counter;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &counter));
  OnExit clear{[&]() {(void)rmw_event_set_callback(&ev, nullptr, nullptr);}};
  rmw_matched_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));  // the existing match

  auto sub_opts = rmw_get_default_subscription_options();
  auto * second = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, second);
  OnExit destroy{[&]() {(void)rmw_destroy_subscription(node, second);}};
  rmw_event_t other = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&other, pub, RMW_EVENT_PUBLICATION_MATCHED));
  EXPECT_TRUE(await_calls(counter, 2)) << "the second init absorbed the new match";
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&other));
}

TEST_F(EventTest, MessageLostCountsSequenceGaps)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));

  publish(1);
  wait_on_sub(sub);
  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  ASSERT_TRUE(taken);
  EXPECT_EQ(0u, st.total_count) << "the first message from a publisher only sets the baseline";

  skip_publications(pub, 5);
  publish(2);
  wait_on_sub(sub);
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  ASSERT_TRUE(taken);
  EXPECT_EQ(5u, st.total_count);
  EXPECT_EQ(5u, st.total_count_change);

  skip_publications(pub, 2);
  publish(3);
  wait_on_sub(sub);
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  ASSERT_TRUE(taken);
  EXPECT_EQ(7u, st.total_count);
  EXPECT_EQ(2u, st.total_count_change);

  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// rosbag2's recorder runs a waiting executor: the event must be reported by
// the same rmw_wait whose drain found the gap.
TEST_F(EventTest, MessageLostIsReportedByTheWaitThatFoundIt)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  publish(1);
  wait_on_sub(sub);  // baseline
  skip_publications(pub, 3);
  publish(2);

  rmw_wait_set_t * ws = rmw_create_wait_set(&context, 2);
  ASSERT_NE(nullptr, ws);
  void * sub_arr[1] = {sub->data};
  rmw_subscriptions_t subs{1, sub_arr};
  void * ev_arr[1] = {&ev};
  rmw_events_t events{1, ev_arr};
  rmw_time_t timeout{2, 0};
  EXPECT_EQ(RMW_RET_OK, rmw_wait(&subs, nullptr, nullptr, nullptr, &events, ws, &timeout));
  EXPECT_NE(nullptr, ev_arr[0]) << "the wait drained the gap but did not report the event";
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));

  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_TRUE(taken);
  EXPECT_EQ(3u, st.total_count_change);
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// An EventsExecutor registers a message callback, so the listener thread does
// the drain - and fires the message_lost callback from it.
TEST_F(EventTest, MessageLostCallbackFiresFromTheListenerDrain)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  EventCounter lost;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &lost));
  EventCounter messages;
  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, EventCounter::fire, &messages));
  OnExit clear{[&]() {
      (void)rmw_subscription_set_on_new_message_callback(sub, nullptr, nullptr);
      (void)rmw_event_set_callback(&ev, nullptr, nullptr);
    }};

  publish(1);
  ASSERT_TRUE(await_calls(messages, 1));  // baseline, drained by the listener
  skip_publications(pub, 4);
  publish(2);
  EXPECT_TRUE(await_calls(lost, 1)) << "no message_lost callback without rmw_wait";

  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_TRUE(taken);
  EXPECT_EQ(4u, st.total_count);

  ASSERT_EQ(RMW_RET_OK, rmw_subscription_set_on_new_message_callback(sub, nullptr, nullptr));
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// One callback per untaken batch: the executor takes once per callback, and a
// take collects every change since the last one, so a callback per change
// would leave takes with nothing new to report. A lost callback has the
// listener drain the socket, and a drain records a loss before it reports the
// message, so an arrival count says when each gap has been seen.
TEST_F(EventTest, EventCallbackFiresOncePerUntakenBatch)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  EventCounter lost;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &lost));
  EventCounter arrived;
  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(sub, EventCounter::fire, &arrived));
  OnExit clear{[&]() {
      (void)rmw_subscription_set_on_new_message_callback(sub, nullptr, nullptr);
      (void)rmw_event_set_callback(&ev, nullptr, nullptr);
    }};
  publish(1);
  ASSERT_TRUE(await_calls(arrived, 1));  // baseline

  for (int32_t i = 2; i <= 3; ++i) {  // two gaps, no take between
    skip_publications(pub, 1);
    publish(i);
    ASSERT_TRUE(await_calls(arrived, static_cast<size_t>(i)));
  }
  EXPECT_EQ(1u, lost.calls.load());

  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(2u, st.total_count_change);  // the take collects the whole batch

  skip_publications(pub, 1);
  publish(4);
  ASSERT_TRUE(await_calls(arrived, 4));
  EXPECT_EQ(2u, lost.calls.load()) << "a change after a take starts a new batch";
}

// --- review regressions -------------------------------------------------------

// The listener and every rmw_wait in the process read one doorbell socket, so
// a GraphListener's wait can consume the ring a matched callback needed.
// Whoever sees the generation move has to tell the listener.
TEST_F(EventTest, MatchedCallbackFiresWhenAnotherWaitConsumesTheRing)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  EventCounter counter;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &counter));
  OnExit clear{[&]() {(void)rmw_event_set_callback(&ev, nullptr, nullptr);}};
  rmw_matched_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));  // the existing match

  GraphWaiter graph(context, node);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto sub_opts = rmw_get_default_subscription_options();
  auto * second = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, second);
  OnExit destroy{[&]() {(void)rmw_destroy_subscription(node, second);}};
  EXPECT_TRUE(await_calls(counter, 2)) << "a wait consumed the ring and the listener never heard";
}

// The same theft between two wait sets: one waits on graph changes, the other
// on a publisher's matched event alone - the image_pipeline lazy node beside a
// GraphListener.
TEST_F(EventTest, MatchedWaitWakesWhenAnotherWaitConsumesTheRing)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  rmw_matched_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));  // the existing match

  GraphWaiter graph(context, node);
  rmw_wait_set_t * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);
  std::atomic<bool> was_ready{false};
  std::thread waiter([&]() {
      void * arr[1] = {&ev};
      rmw_events_t events{1, arr};
      rmw_time_t timeout{3, 0};
      (void)rmw_wait(nullptr, nullptr, nullptr, nullptr, &events, ws, &timeout);
      was_ready = arr[0] != nullptr;
    });
  OnExit join_waiter{[&]() {if (waiter.joinable()) {waiter.join();}}};
  std::this_thread::sleep_for(std::chrono::milliseconds(100));  // both waits block

  auto sub_opts = rmw_get_default_subscription_options();
  auto * second = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, second);
  waiter.join();
  EXPECT_TRUE(was_ready.load()) << "the matched wait slept through the change";

  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, second));
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// Two threads publishing on one publisher. Numbered before the send, outside
// any lock, their datagrams could reach a subscriber out of sequence order,
// and the gap check would count a message that arrived as lost. Real drops
// are allowed here - a burst can overflow the socket - but never more loss
// than is actually missing.
TEST_F(EventTest, ConcurrentPublishesOnOnePublisherAreNotLost)
{
  rmw_qos_profile_t deep = qos;
  deep.depth = 5000;  // so every arrival grows the queue and is counted
  auto sub_opts = rmw_get_default_subscription_options();
  auto * counted = rmw_create_subscription(node, ts, "/event_test_topic", &deep, &sub_opts);
  ASSERT_NE(nullptr, counted);
  OnExit destroy{[&]() {(void)rmw_destroy_subscription(node, counted);}};
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, counted, RMW_EVENT_MESSAGE_LOST));
  EventCounter arrived;
  ASSERT_EQ(
    RMW_RET_OK,
    rmw_subscription_set_on_new_message_callback(counted, EventCounter::fire, &arrived));
  OnExit clear{[&]() {
      (void)rmw_subscription_set_on_new_message_callback(counted, nullptr, nullptr);
    }};

  constexpr size_t per_thread = 2000;
  auto burst = [&]() {
      test_msgs::msg::BasicTypes msg;
      for (size_t i = 0; i < per_thread; ++i) {
        msg.int32_value = static_cast<int32_t>(i);
        (void)rmw_publish(pub, &msg, nullptr);
      }
    };
  std::thread a(burst);
  std::thread b(burst);
  a.join();
  b.join();
  for (int waited = 0; waited < 2000 && arrived.events.load() < 2 * per_thread; waited += 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  const size_t missing = 2 * per_thread - arrived.events.load();
  EXPECT_LE(st.total_count, missing) << "counted " << st.total_count << " lost of "
                                     << missing << " missing";
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// A subscription that ignores its own context's publications drops them on
// purpose, so a gap in that publisher's numbering is no loss to it.
TEST_F(EventTest, GapsFromAnIgnoredLocalPublisherAreNotLost)
{
  auto sub_opts = rmw_get_default_subscription_options();
  sub_opts.ignore_local_publications = true;
  auto * ignoring = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, ignoring);
  OnExit destroy{[&]() {(void)rmw_destroy_subscription(node, ignoring);}};
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, ignoring, RMW_EVENT_MESSAGE_LOST));

  publish(1);
  skip_publications(pub, 5);
  publish(2);
  rmw_wait_set_t * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);
  void * arr[1] = {ignoring->data};
  rmw_subscriptions_t subs{1, arr};
  rmw_time_t timeout{0, 200000000};  // drains both datagrams, takes nothing
  (void)rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, ws, &timeout);
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));

  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(0u, st.total_count);
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}


// An executor driven by callbacks may set only the message_lost callback. A
// loss is found by draining, so the listener must then drain the socket
// itself, with nothing waiting or taking.
TEST_F(EventTest, AMessageLostCallbackAloneWatchesTheSocket)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  EventCounter lost;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &lost));
  OnExit clear{[&]() {(void)rmw_event_set_callback(&ev, nullptr, nullptr);}};

  publish(1);
  skip_publications(pub, 3);
  publish(2);
  EXPECT_TRUE(await_calls(lost, 1)) << "nothing drained the socket for the lost callback";

  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(3u, st.total_count);
}

// rclcpp waits on a subscription and its events together, but the rmw
// contract does not require it: a wait holding only a message_lost event has
// to drain the subscription itself to find a gap - without reporting the
// subscription, which it was not asked about.
TEST_F(EventTest, AWaitHoldingOnlyAMessageLostEventFindsTheGap)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  rmw_wait_set_t * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);
  OnExit destroy{[&]() {(void)rmw_destroy_wait_set(ws);}};
  auto wait_on_event = [&](rmw_time_t timeout, bool & ready) {
      void * arr[1] = {&ev};
      rmw_events_t events{1, arr};
      const rmw_ret_t ret = rmw_wait(nullptr, nullptr, nullptr, nullptr, &events, ws, &timeout);
      ready = arr[0] != nullptr;
      return ret;
    };

  bool ready = true;
  publish(1);
  (void)wait_on_event({0, 200000000}, ready);  // drains the baseline
  EXPECT_FALSE(ready) << "a queued message is not this wait's business";

  skip_publications(pub, 4);
  publish(2);
  EXPECT_EQ(RMW_RET_OK, wait_on_event({2, 0}, ready));
  EXPECT_TRUE(ready) << "the event-only wait never drained the subscription";

  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(4u, st.total_count);
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}


// A datagram whose shm descriptor no longer resolves - the sender is gone,
// or lapped its ring - arrived but its message is gone: a loss. Here it is all
// the socket holds, so the listener's drain finds a loss and nothing else.
TEST_F(EventTest, AnUnresolvableShmDescriptorCountsAsLost)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  EventCounter lost;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &lost));
  OnExit clear{[&]() {(void)rmw_event_set_callback(&ev, nullptr, nullptr);}};

  rmw_uds::WireHeader hdr{};
  hdr.gid[0] = 0xAB;  // a publisher this subscription has never heard from
  hdr.sequence_number = 1;
  hdr.msg_type = rmw_uds::SHM_PAYLOAD_FLAG;  // a topic message staged in shm
  const rmw_uds::ShmPayloadDescriptor desc{};  // names no ring
  hdr.payload_size = sizeof(desc);
  auto * data = static_cast<rmw_uds::UdsSubscription *>(sub->data);
  ASSERT_TRUE(send_raw(data->socket_path, hdr, &desc, sizeof(desc)));
  EXPECT_TRUE(await_calls(lost, 1));

  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(1u, st.total_count);
  std::lock_guard<std::mutex> lock(data->queue_mutex);
  EXPECT_TRUE(data->message_queue.empty());
}

// A TRANSIENT_LOCAL late joiner gets the history through the creation-time
// pull, and its socket may still carry some of it: the datagrams sent between
// its registration and the pull, deduplicated against the pull's watermark.
// The loss baseline must start at that watermark, or a sequence the pull
// delivered but the socket never held counts as lost.
TEST_F(EventTest, HistoryDeliveredByTheLateJoinerPullIsNotLost)
{
  rmw_qos_profile_t tl = qos;
  tl.durability = RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL;
  auto pub_opts = rmw_get_default_publisher_options();
  auto * tl_pub = rmw_create_publisher(node, ts, "/event_test_tl", &tl, &pub_opts);
  ASSERT_NE(nullptr, tl_pub);
  OnExit destroy_pub{[&]() {(void)rmw_destroy_publisher(node, tl_pub);}};
  test_msgs::msg::BasicTypes msg;
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(RMW_RET_OK, rmw_publish(tl_pub, &msg, nullptr));
  }
  auto sub_opts = rmw_get_default_subscription_options();
  auto * tl_sub = rmw_create_subscription(node, ts, "/event_test_tl", &tl, &sub_opts);
  ASSERT_NE(nullptr, tl_sub);
  OnExit destroy_sub{[&]() {(void)rmw_destroy_subscription(node, tl_sub);}};
  auto * data = static_cast<rmw_uds::UdsSubscription *>(tl_sub->data);

  rmw_gid_t gid;
  ASSERT_EQ(RMW_RET_OK, rmw_get_gid_for_publisher(tl_pub, &gid));
  std::array<uint8_t, 16> key;
  std::memcpy(key.data(), gid.data, key.size());
  int64_t watermark = 0;
  {
    std::lock_guard<std::mutex> lock(data->queue_mutex);
    ASSERT_EQ(1u, data->replayed_watermarks.count(key)) << "no creation-time pull";
    watermark = data->replayed_watermarks.at(key);
  }
  ASSERT_EQ(3, watermark);

  // A send that raced the pull: sequence 2 reached the socket, 3 did not.
  rmw_uds::WireHeader hdr{};
  std::memcpy(hdr.gid, gid.data, sizeof(hdr.gid));
  hdr.sequence_number = watermark - 1;
  const uint8_t payload[4] = {};
  hdr.payload_size = sizeof(payload);
  ASSERT_TRUE(send_raw(data->socket_path, hdr, payload, sizeof(payload)));
  ASSERT_EQ(RMW_RET_OK, rmw_publish(tl_pub, &msg, nullptr));  // live: sequence 4
  // The replayed queue satisfies this wait at once; its non-blocking poll
  // pass still drains the socket.
  wait_on_sub(tl_sub);

  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, tl_sub, RMW_EVENT_MESSAGE_LOST));
  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(0u, st.total_count) << "sequence 3 came through the pull";
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}


// A drain on one thread can empty a socket another thread's wait is blocked
// on, and that wait's epoll then never sees the socket readable. Here a taker
// spins on the subscription while a waiter blocks on its message_lost event
// alone: the thread that records each loss has to wake the waiter.
TEST_F(EventTest, AWaitWakesForALossAnotherThreadRecorded)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  rmw_wait_set_t * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);
  std::atomic<bool> stop{false};
  std::atomic<size_t> reported{0};
  std::thread waiter([&]() {
      while (!stop.load()) {
        void * arr[1] = {&ev};
        rmw_events_t events{1, arr};
        rmw_time_t timeout{1, 0};
        (void)rmw_wait(nullptr, nullptr, nullptr, nullptr, &events, ws, &timeout);
        if (arr[0]) {
          rmw_message_lost_status_t st{};
          bool taken = false;
          (void)rmw_take_event(&ev, &st, &taken);
          reported.fetch_add(st.total_count_change);
        }
      }
    });
  std::thread taker([&]() {
      test_msgs::msg::BasicTypes msg;
      while (!stop.load()) {
        bool taken = false;
        (void)rmw_take(sub, &msg, &taken, nullptr);
      }
    });
  OnExit join{[&]() {
      stop.store(true);
      taker.join();
      waiter.join();  // within its 1 s timeout
      (void)rmw_destroy_wait_set(ws);
      (void)rmw_event_fini(&ev);
    }};

  publish(0);  // baseline
  size_t late = 0;
  for (size_t gap = 1; gap <= 20; ++gap) {
    skip_publications(pub, 1);
    publish(static_cast<int32_t>(gap));
    int waited = 0;
    for (; reported.load() < gap && waited < 1500; waited += 1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(gap, reported.load()) << "loss " << gap << " never reported";
    late += waited >= 500 ? 1 : 0;  // reported only by the wait's own timeout
  }
  EXPECT_EQ(0u, late) << "the waiter slept through a loss another thread recorded";
}

// A publish that chose its recipients before a subscription joined, and was
// numbered after a publish that reached it, would leave the newcomer a gap
// that nothing dropped. Bystander subscriptions lengthen each send loop, so
// the publishing threads queue on the send lock while one joins, and the
// order they get it in is up to the scheduler. Each round sends at most 100
// messages, few enough for the newcomer's socket, so no loss is real.
TEST_F(EventTest, ASubscriptionThatJoinsMidPublishSeesNoFalseGap)
{
  auto sub_opts = rmw_get_default_subscription_options();
  std::vector<rmw_subscription_t *> bystanders;
  OnExit destroy{[&]() {
      for (auto * b : bystanders) {(void)rmw_destroy_subscription(node, b);}
    }};
  for (int i = 0; i < 16; ++i) {
    bystanders.push_back(rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts));
    ASSERT_NE(nullptr, bystanders.back());
  }
  rmw_qos_profile_t deep = qos;
  deep.depth = 1000;

  size_t false_losses = 0;
  for (int round = 0; round < 100; ++round) {
    std::atomic<bool> stop{false};
    auto burst = [&]() {
        test_msgs::msg::BasicTypes msg;
        for (int i = 0; i < 25 && !stop.load(); ++i) {
          (void)rmw_publish(pub, &msg, nullptr);
        }
      };
    std::vector<std::thread> publishers;
    for (int t = 0; t < 4; ++t) {
      publishers.emplace_back(burst);
    }
    auto * joiner = rmw_create_subscription(node, ts, "/event_test_topic", &deep, &sub_opts);
    for (auto & t : publishers) {
      t.join();
    }
    ASSERT_NE(nullptr, joiner);
    rmw_event_t ev = rmw_get_zero_initialized_event();
    EXPECT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, joiner, RMW_EVENT_MESSAGE_LOST));
    test_msgs::msg::BasicTypes msg;
    bool got = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take(joiner, &msg, &got, nullptr));  // drains the socket first
    rmw_message_lost_status_t st{};
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
    false_losses += st.total_count;
    EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
    EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, joiner));
  }
  EXPECT_EQ(0u, false_losses);
}

// rcl's event fini never reaches an RMW (librmw implements rmw_event_fini), so
// the endpoint is the only lifetime the matched count can follow. Left
// counted, every later graph change in the process would wake every wait set.
TEST_F(EventTest, ADestroyedEndpointReleasesItsMatchedCount)
{
  auto * ctx = reinterpret_cast<rmw_uds::UdsContext *>(context.impl);
  const int before = ctx->matched_events.load();
  auto pub_opts = rmw_get_default_publisher_options();
  auto * other = rmw_create_publisher(node, ts, "/event_test_topic", &qos, &pub_opts);
  ASSERT_NE(nullptr, other);
  for (int i = 0; i < 2; ++i) {  // two handles on one endpoint count it once
    rmw_event_t ev = rmw_get_zero_initialized_event();
    ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, other, RMW_EVENT_PUBLICATION_MATCHED));
  }
  EXPECT_EQ(before + 1, ctx->matched_events.load());
  ASSERT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, other));  // no fini, as under rcl
  EXPECT_EQ(before, ctx->matched_events.load());
}

// A wait that holds a message_lost event without its subscription drains the
// subscription for it, and may so empty the socket another wait set holding
// the subscription is blocked on. It has to wake that wait for the messages
// it queued.
TEST_F(EventTest, AnEventOnlyWaitWakesTheWaitWhoseSocketItDrained)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  rmw_wait_set_t * event_ws = rmw_create_wait_set(&context, 1);
  rmw_wait_set_t * sub_ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, event_ws);
  ASSERT_NE(nullptr, sub_ws);
  std::atomic<bool> stop{false};
  std::atomic<size_t> received{0};
  std::thread drainer([&]() {
      while (!stop.load()) {
        void * arr[1] = {&ev};
        rmw_events_t events{1, arr};
        rmw_time_t timeout{0, 1000000};
        (void)rmw_wait(nullptr, nullptr, nullptr, nullptr, &events, event_ws, &timeout);
      }
    });
  std::thread reader([&]() {
      test_msgs::msg::BasicTypes msg;
      while (!stop.load()) {
        void * arr[1] = {sub->data};
        rmw_subscriptions_t subs{1, arr};
        rmw_time_t timeout{1, 0};
        (void)rmw_wait(&subs, nullptr, nullptr, nullptr, nullptr, sub_ws, &timeout);
        bool taken = true;
        while (arr[0] && taken) {
          (void)rmw_take(sub, &msg, &taken, nullptr);
          received.fetch_add(taken ? 1 : 0);
        }
      }
    });
  OnExit join{[&]() {
      stop.store(true);
      drainer.join();
      reader.join();  // within its 1 s timeout
      (void)rmw_destroy_wait_set(event_ws);
      (void)rmw_destroy_wait_set(sub_ws);
      (void)rmw_event_fini(&ev);
    }};

  size_t late = 0;
  for (size_t i = 1; i <= 20; ++i) {
    publish(static_cast<int32_t>(i));
    int waited = 0;
    for (; received.load() < i && waited < 1500; waited += 1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(i, received.load()) << "message " << i << " never taken";
    late += waited >= 500 ? 1 : 0;  // taken only after the reader's timeout
  }
  EXPECT_EQ(0u, late) << "the reader slept through messages another wait queued";
}

// An rcl user may destroy an endpoint with its matched callback still set;
// rclcpp clears it first. The destroy must then stop the listener watching
// the doorbell, as clearing the last matched callback does.
TEST_F(EventTest, DestroyingTheLastMatchedCallbacksEndpointUnwatchesTheDoorbell)
{
  auto * ctx = reinterpret_cast<rmw_uds::UdsContext *>(context.impl);
  auto doorbell_watched = [ctx]() {
      std::lock_guard<std::mutex> lock(ctx->listener_mutex);
      return ctx->listener_targets.count(ctx->doorbell_fd) == 1;
    };
  auto pub_opts = rmw_get_default_publisher_options();
  auto * other = rmw_create_publisher(node, ts, "/event_test_topic", &qos, &pub_opts);
  ASSERT_NE(nullptr, other);
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, other, RMW_EVENT_PUBLICATION_MATCHED));
  EventCounter counter;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &counter));
  EXPECT_TRUE(doorbell_watched());
  ASSERT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, other));  // callback still set
  EXPECT_FALSE(doorbell_watched());
}

// The gap check does arithmetic on a number off the wire, which any local
// process can write. Publishers number from 1: a datagram numbered 0 must not
// become a baseline that the next number overflows against (0 then INT64_MAX
// made seq - seen - 1 overflow and counted ~9.2e18 losses).
TEST_F(EventTest, ADatagramNumberedBelowOneIsNotGapChecked)
{
  auto * data = static_cast<rmw_uds::UdsSubscription *>(sub->data);
  rmw_uds::WireHeader hdr{};
  hdr.gid[0] = 0xCD;  // a publisher this subscription has never heard from
  const uint8_t payload[4] = {};
  hdr.payload_size = sizeof(payload);
  for (const int64_t seq : {int64_t{0}, std::numeric_limits<int64_t>::max()}) {
    hdr.sequence_number = seq;
    ASSERT_TRUE(send_raw(data->socket_path, hdr, payload, sizeof(payload)));
  }
  wait_on_sub(sub);  // both are queued; the wait drains them

  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(0u, st.total_count);
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// The listener holds listener_mutex across a whole drain. An rmw_wait that
// sees a graph change - here a GraphListener's, holding a matched event -
// fans it out, and must not do so through listener_mutex: it would stall
// behind the drain, and deadlock when a listener callback polls a wait set.
TEST_F(EventTest, AWaitThatSeesAGraphChangeDoesNotWaitForTheListener)
{
  auto * ctx = reinterpret_cast<rmw_uds::UdsContext *>(context.impl);
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_publisher_event_init(&ev, pub, RMW_EVENT_PUBLICATION_MATCHED));
  rmw_wait_set_t * ws = rmw_create_wait_set(&context, 1);
  ASSERT_NE(nullptr, ws);
  auto sub_opts = rmw_get_default_subscription_options();
  auto * second = rmw_create_subscription(node, ts, "/event_test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, second);  // a graph change nobody has seen yet

  std::atomic<bool> returned{false};
  std::unique_lock<std::mutex> drain(ctx->listener_mutex);  // as the listener holds it
  std::thread waiter([&]() {
      void * arr[1] = {&ev};
      rmw_events_t events{1, arr};
      rmw_time_t timeout{0, 0};
      (void)rmw_wait(nullptr, nullptr, nullptr, nullptr, &events, ws, &timeout);
      returned.store(true);
    });
  for (int waited = 0; !returned.load() && waited < 2000; waited += 5) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_TRUE(returned.load()) << "the wait blocked on listener_mutex";
  drain.unlock();
  waiter.join();
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_wait_set(ws));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_subscription(node, second));
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}

// A datagram whose descriptor no longer resolves counts as one lost message -
// but only a publish the gap check had not passed yet. One numbered at or
// below what its publisher was already counted up to is not a new loss.
TEST_F(EventTest, AnUnresolvableDescriptorAlreadyCountedIsNotCountedAgain)
{
  auto * data = static_cast<rmw_uds::UdsSubscription *>(sub->data);
  rmw_uds::WireHeader hdr{};
  hdr.gid[0] = 0xEF;  // a publisher this subscription has never heard from
  const uint8_t payload[4] = {};
  hdr.payload_size = sizeof(payload);
  hdr.sequence_number = 5;
  ASSERT_TRUE(send_raw(data->socket_path, hdr, payload, sizeof(payload)));  // baseline
  hdr.msg_type = rmw_uds::SHM_PAYLOAD_FLAG;
  const rmw_uds::ShmPayloadDescriptor desc{};  // names no ring
  hdr.payload_size = sizeof(desc);
  hdr.sequence_number = 3;
  ASSERT_TRUE(send_raw(data->socket_path, hdr, &desc, sizeof(desc)));
  wait_on_sub(sub);  // the baseline is queued; the wait drains both

  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(0u, st.total_count);
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}
