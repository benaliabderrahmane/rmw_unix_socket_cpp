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

#include <cstring>

#include "test_msgs/msg/basic_types.hpp"

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
    RmwUdsNodeTest::TearDown();
  }
};

// rmw_event_type_is_supported() reports no event types as supported, so
// rclcpp expects *_event_init() to fail with RMW_RET_UNSUPPORTED for those
// event types (it only swallows initialization failures reported this way,
// via UnsupportedEventTypeException). If init instead reports success,
// rclcpp constructs a live event handler for it, and the failure only
// surfaces later - unhandled - when the executor registers its callback,
// crashing with "failed to set the on new message callback for Event".
TEST_F(EventTest, SubscriptionEventInitRejectsUnsupportedEventType)
{
  ASSERT_FALSE(rmw_event_type_is_supported(RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE));

  rmw_event_t event = rmw_get_zero_initialized_event();
  EXPECT_EQ(
    RMW_RET_UNSUPPORTED,
    rmw_subscription_event_init(&event, sub, RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE));
}

TEST_F(EventTest, PublisherEventInitRejectsUnsupportedEventType)
{
  ASSERT_FALSE(rmw_event_type_is_supported(RMW_EVENT_OFFERED_QOS_INCOMPATIBLE));

  rmw_event_t event = rmw_get_zero_initialized_event();
  EXPECT_EQ(
    RMW_RET_UNSUPPORTED,
    rmw_publisher_event_init(&event, pub, RMW_EVENT_OFFERED_QOS_INCOMPATIBLE));
}
