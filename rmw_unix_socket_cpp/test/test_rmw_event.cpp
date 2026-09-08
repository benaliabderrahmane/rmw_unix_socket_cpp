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

#include <cstdint>
#include <cstring>

#include "test_msgs/msg/basic_types.hpp"

#include "rmw/error_handling.h"
#include "rmw/event.h"
#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"

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
  // *_event_init() cannot produce it: it rejects every event type, which is
  // the behavior the tests below pin.
  rmw_event_t our_event(rmw_event_type_t type) const
  {
    rmw_event_t event = rmw_get_zero_initialized_event();
    event.implementation_identifier = uds_id();
    event.data = sub->data;
    event.event_type = type;
    return event;
  }
};

// rmw_event_type_is_supported() reports no event types as supported, so
// rclcpp expects *_event_init() to fail with RMW_RET_UNSUPPORTED for those
// event types (it only swallows initialization failures reported this way,
// via UnsupportedEventTypeException). If init instead reports success,
// rclcpp constructs a live event handler for it, and the failure only
// surfaces later - unhandled - when the executor registers its callback,
// crashing with "failed to set the on new message callback for Event".
//
// Iterating the whole enum rather than naming types keeps this honest across
// the distro matrix: rmw_event_type_t has no explicit initializers, so both
// the ordinals and RMW_EVENT_INVALID shift as upstream adds event types.
TEST_F(EventTest, SubscriptionEventInitRejectsEveryEventType)
{
  for (int i = 0; i <= RMW_EVENT_INVALID; ++i) {
    const auto type = static_cast<rmw_event_type_t>(i);
    ASSERT_FALSE(rmw_event_type_is_supported(type)) << "event type " << i;

    rmw_event_t event = rmw_get_zero_initialized_event();
    EXPECT_EQ(RMW_RET_UNSUPPORTED, rmw_subscription_event_init(&event, sub, type))
      << "event type " << i;
    // rcl logs whatever the RMW left behind, so a bare return code is a
    // dead end for anyone debugging the rejection.
    EXPECT_TRUE(rmw_error_is_set()) << "event type " << i;
    rmw_reset_error();
  }
}

TEST_F(EventTest, PublisherEventInitRejectsEveryEventType)
{
  for (int i = 0; i <= RMW_EVENT_INVALID; ++i) {
    const auto type = static_cast<rmw_event_type_t>(i);
    ASSERT_FALSE(rmw_event_type_is_supported(type)) << "event type " << i;

    rmw_event_t event = rmw_get_zero_initialized_event();
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
  ASSERT_EQ(RMW_RET_UNSUPPORTED, rmw_subscription_event_init(&event, sub, RMW_EVENT_MESSAGE_LOST));
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
    rmw_publisher_event_init(&event, &foreign_pub, RMW_EVENT_LIVELINESS_LOST));
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
    rmw_publisher_event_init(nullptr, pub, RMW_EVENT_LIVELINESS_LOST));
  rmw_reset_error();
  EXPECT_EQ(
    RMW_RET_INVALID_ARGUMENT,
    rmw_publisher_event_init(&event, nullptr, RMW_EVENT_LIVELINESS_LOST));
  rmw_reset_error();
}

// rmw_take_event() may only write into the caller's status struct when it
// reports taken. rcl reuses that buffer across calls, so scribbling in it on
// the nothing-taken path would hand the application a stale status it never
// asked for.
TEST_F(EventTest, TakeEventTakesNothingAndLeavesEventInfoUntouched)
{
  rmw_event_t event = our_event(RMW_EVENT_MESSAGE_LOST);

  uint8_t info[128];
  std::memset(info, 0xAB, sizeof(info));
  uint8_t expected[128];
  std::memset(expected, 0xAB, sizeof(expected));

  bool taken = true;
  EXPECT_EQ(RMW_RET_OK, rmw_take_event(&event, info, &taken));
  EXPECT_FALSE(taken);
  EXPECT_EQ(0, std::memcmp(info, expected, sizeof(info)));
}

TEST_F(EventTest, TakeEventRejectsBadArguments)
{
  rmw_event_t event = our_event(RMW_EVENT_MESSAGE_LOST);
  uint8_t info[16] = {};
  bool taken = false;

  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_take_event(nullptr, info, &taken));
  rmw_reset_error();
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_take_event(&event, info, nullptr));
  rmw_reset_error();

  rmw_event_t foreign = event;
  foreign.implementation_identifier = "rmw_bogus_cpp";
  EXPECT_EQ(RMW_RET_INCORRECT_RMW_IMPLEMENTATION, rmw_take_event(&foreign, info, &taken));
  rmw_reset_error();
}

// rclcpp's EventHandler destructor runs even when its constructor bailed out
// on the RMW_RET_UNSUPPORTED from *_event_init, and that leaves the handle
// zero-initialized. Finalizing it must not be reported as a caller error, or
// every swallowed UnsupportedEventTypeException produces a spurious failure
// on the way out.
TEST_F(EventTest, EventFiniAcceptsAZeroInitializedHandle)
{
  rmw_event_t event = rmw_get_zero_initialized_event();
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&event));
  EXPECT_FALSE(rmw_error_is_set());
}

TEST_F(EventTest, EventFiniValidatesItsArgument)
{
  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_event_fini(nullptr));
  rmw_reset_error();

  rmw_event_t ours = our_event(RMW_EVENT_MESSAGE_LOST);
  EXPECT_EQ(RMW_RET_OK, rmw_event_fini(&ours));

  rmw_event_t foreign = our_event(RMW_EVENT_MESSAGE_LOST);
  foreign.implementation_identifier = "rmw_bogus_cpp";
  EXPECT_EQ(RMW_RET_INCORRECT_RMW_IMPLEMENTATION, rmw_event_fini(&foreign));
  rmw_reset_error();
}

// The original EventsExecutor crash surfaced as "failed to set the on new
// message callback for Event: error not set" - the ": error not set" half
// being this function returning a failure with no message behind it.
TEST_F(EventTest, EventSetCallbackReportsUnsupportedWithAMessage)
{
  rmw_event_t event = our_event(RMW_EVENT_MESSAGE_LOST);
  EXPECT_EQ(RMW_RET_UNSUPPORTED, rmw_event_set_callback(&event, nullptr, nullptr));
  EXPECT_TRUE(rmw_error_is_set());
  rmw_reset_error();

  EXPECT_EQ(RMW_RET_INVALID_ARGUMENT, rmw_event_set_callback(nullptr, nullptr, nullptr));
  rmw_reset_error();
}
