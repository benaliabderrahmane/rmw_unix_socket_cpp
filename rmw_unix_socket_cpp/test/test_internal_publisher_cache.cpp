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

// Deliberate white-box tests of the TL replay cache's pruning: the oracle is
// the publisher's private known_subscriber_paths set (a leak there has no
// public-API observable short of unbounded memory growth), so these inspect
// UdsPublisher internals on purpose. Behavioral QoS coverage lives in
// test_rmw_qos.cpp.

#include "test_base.hpp"

#include <cstring>

#include "test_msgs/msg/basic_types.hpp"

#include "rosidl_typesupport_cpp/message_type_support.hpp"

#include "../src/types.hpp"

class QosTest : public RmwUdsNodeTest
{
protected:
  const rosidl_message_type_support_t * ts = nullptr;

  void SetUp() override
  {
    RmwUdsNodeTest::SetUp();
    ts = rosidl_typesupport_cpp::get_message_type_support_handle<
      test_msgs::msg::BasicTypes>();
  }

  rmw_qos_profile_t make_qos(
    rmw_qos_reliability_policy_e rel,
    rmw_qos_durability_policy_e dur,
    size_t depth = 10)
  {
    rmw_qos_profile_t qos;
    std::memset(&qos, 0, sizeof(qos));
    qos.history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    qos.depth = depth;
    qos.reliability = rel;
    qos.durability = dur;
    return qos;
  }
};

TEST_F(QosTest, KnownSubscriberPathsPrunedOnChurn)
{
  // The publisher's known_subscriber_paths must not accumulate dead entries as
  // subscribers churn: each create/destroy bumps the registry generation, and a
  // restarted subscriber gets a brand-new unique socket path. Without pruning on
  // refresh the set is insert-only and grows by one per churned subscriber.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/churn", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Seed the cache so there is something to replay.
  test_msgs::msg::BasicTypes seed;
  seed.int32_value = 1;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &seed, nullptr));

  auto sub_opts = rmw_get_default_subscription_options();
  constexpr int kChurn = 8;
  for (int i = 0; i < kChurn; ++i) {
    // New sub bumps generation -> next publish refreshes + records this sub.
    auto * sub = rmw_create_subscription(node, ts, "/churn", &qos, &sub_opts);
    ASSERT_NE(nullptr, sub);
    test_msgs::msg::BasicTypes m;
    m.int32_value = i + 2;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));
    // Destroy bumps generation again; next publish refreshes + prunes the gone sub.
    auto _r [[maybe_unused]] = rmw_destroy_subscription(node, sub);
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &m, nullptr));
  }

  // After the loop every churned sub is destroyed, so known should be empty.
  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  size_t known_size = 0;
  {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);
    known_size = pub_data->known_subscriber_paths.size();
  }
  // With the prune: tracks only live subs (0 here). Without it: grows to kChurn.
  EXPECT_LE(known_size, 1u)
    << "known_subscriber_paths leaked dead entries: size=" << known_size;

  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}

TEST_F(QosTest, TransientLocalSerializedKnownSubscriberPathsPrunedOnChurn)
{
  // Same prune guarantee as KnownSubscriberPathsPrunedOnChurn, but driven
  // through rmw_publish_serialized_message, which carries its own copy of the
  // prune-on-refresh logic. Guards against that copy silently diverging: without
  // the prune the serialized path's known_subscriber_paths grows by one per
  // churned subscriber.
  auto qos = make_qos(
    RMW_QOS_POLICY_RELIABILITY_RELIABLE,
    RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL, 5);

  auto pub_opts = rmw_get_default_publisher_options();
  auto * pub = rmw_create_publisher(node, ts, "/churn_serialized", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  uint8_t bytes[] = {1, 2, 3, 4, 5, 6, 7, 8};
  rmw_serialized_message_t msg;
  msg.buffer = bytes;
  msg.buffer_length = sizeof(bytes);
  msg.buffer_capacity = sizeof(bytes);
  msg.allocator = rcutils_get_default_allocator();

  // Seed the cache so there is something to replay.
  EXPECT_EQ(RMW_RET_OK, rmw_publish_serialized_message(pub, &msg, nullptr));

  auto sub_opts = rmw_get_default_subscription_options();
  constexpr int kChurn = 8;
  for (int i = 0; i < kChurn; ++i) {
    auto * sub = rmw_create_subscription(node, ts, "/churn_serialized", &qos, &sub_opts);
    ASSERT_NE(nullptr, sub);
    EXPECT_EQ(RMW_RET_OK, rmw_publish_serialized_message(pub, &msg, nullptr));
    auto _r [[maybe_unused]] = rmw_destroy_subscription(node, sub);
    EXPECT_EQ(RMW_RET_OK, rmw_publish_serialized_message(pub, &msg, nullptr));
  }

  auto * pub_data = static_cast<rmw_uds::UdsPublisher *>(pub->data);
  size_t known_size = 0;
  {
    std::lock_guard<std::mutex> lock(pub_data->cache_mutex);
    known_size = pub_data->known_subscriber_paths.size();
  }
  EXPECT_LE(known_size, 1u)
    << "serialized-path known_subscriber_paths leaked dead entries: size=" << known_size;

  auto _r2 [[maybe_unused]] = rmw_destroy_publisher(node, pub);
}
