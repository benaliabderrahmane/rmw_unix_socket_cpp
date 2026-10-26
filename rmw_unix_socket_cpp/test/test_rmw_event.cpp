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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <thread>

#include "test_msgs/msg/basic_types.hpp"

#include "rmw/error_handling.h"
#include "rmw/event.h"
#include "rmw/events_statuses/events_statuses.h"
#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"

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

// rcl_event_fini skips rmw_event_fini when event->impl is NULL, which is what
// rcl_*_event_init leaves behind after an RMW_RET_UNSUPPORTED - but a handle no
// init filled in must still be harmless to finalize.
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

  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));  // clears the callback first
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
// would leave takes with nothing new to report.
TEST_F(EventTest, EventCallbackFiresOncePerUntakenBatch)
{
  rmw_event_t ev = rmw_get_zero_initialized_event();
  ASSERT_EQ(RMW_RET_OK, rmw_subscription_event_init(&ev, sub, RMW_EVENT_MESSAGE_LOST));
  EventCounter lost;
  ASSERT_EQ(RMW_RET_OK, rmw_event_set_callback(&ev, EventCounter::fire, &lost));
  publish(1);
  wait_on_sub(sub);  // baseline

  for (int32_t i = 2; i <= 3; ++i) {  // two gaps, two drains, no take between
    skip_publications(pub, 1);
    publish(i);
    wait_on_sub(sub);
  }
  EXPECT_EQ(1u, lost.calls.load());

  rmw_message_lost_status_t st{};
  bool taken = false;
  ASSERT_EQ(RMW_RET_OK, rmw_take_event(&ev, &st, &taken));
  EXPECT_EQ(2u, st.total_count_change);  // the take collects the whole batch

  skip_publications(pub, 1);
  publish(4);
  wait_on_sub(sub);
  EXPECT_EQ(2u, lost.calls.load()) << "a change after a take starts a new batch";
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ev));
}
