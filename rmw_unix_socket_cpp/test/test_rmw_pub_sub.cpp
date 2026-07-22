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
#include "test_msgs/msg/empty.hpp"
#include "test_msgs/msg/strings.hpp"
#include "test_msgs/msg/unbounded_sequences.hpp"

#include "rmw/qos_profiles.h"
#include "rosidl_typesupport_cpp/message_type_support.hpp"

#include <chrono>
#include <thread>

class PubSubTest : public RmwUdsNodeTest
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
  }

  void TearDown() override
  {
    if (sub) { auto _r [[maybe_unused]] = rmw_destroy_subscription(node, sub); }
    if (pub) { auto _r [[maybe_unused]] = rmw_destroy_publisher(node, pub); }
    RmwUdsNodeTest::TearDown();
  }
};

TEST_F(PubSubTest, CreateDestroyPublisher)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/test_topic", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);
  EXPECT_STREQ("/test_topic", pub->topic_name);
  EXPECT_EQ(uds_id(), pub->implementation_identifier);
  EXPECT_FALSE(pub->can_loan_messages);
}

TEST_F(PubSubTest, CreateDestroySubscription)
{
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/test_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);
  EXPECT_STREQ("/test_topic", sub->topic_name);
  EXPECT_EQ(uds_id(), sub->implementation_identifier);
}

TEST_F(PubSubTest, PublishAndTake)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/roundtrip", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/roundtrip", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  // Publish
  test_msgs::msg::BasicTypes send_msg;
  send_msg.int32_value = 42;
  send_msg.float64_value = 3.14;
  send_msg.bool_value = true;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  // Take
  test_msgs::msg::BasicTypes recv_msg;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(42, recv_msg.int32_value);
  EXPECT_DOUBLE_EQ(3.14, recv_msg.float64_value);
  EXPECT_TRUE(recv_msg.bool_value);
}

TEST_F(PubSubTest, TakeWithInfo)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/info_topic", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/info_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::BasicTypes send_msg;
  send_msg.int32_value = 99;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  test_msgs::msg::BasicTypes recv_msg;
  bool taken = false;
  rmw_message_info_t info = rmw_get_zero_initialized_message_info();
  EXPECT_EQ(RMW_RET_OK, rmw_take_with_info(sub, &recv_msg, &taken, &info, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(99, recv_msg.int32_value);
  EXPECT_GT(info.source_timestamp, 0);
  EXPECT_GT(info.received_timestamp, 0);
  EXPECT_GE(info.publication_sequence_number, 1u);
}

TEST_F(PubSubTest, TakeEmptyReturnsFalse)
{
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/empty_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::BasicTypes recv_msg;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  EXPECT_FALSE(taken);
}

TEST_F(PubSubTest, MultipleMessages)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/multi", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/multi", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  constexpr int N = 5;
  for (int i = 0; i < N; ++i) {
    test_msgs::msg::BasicTypes msg;
    msg.int32_value = i;
    EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &msg, nullptr));
  }

  for (int i = 0; i < N; ++i) {
    test_msgs::msg::BasicTypes recv;
    bool taken = false;
    EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv, &taken, nullptr));
    ASSERT_TRUE(taken);
    EXPECT_EQ(i, recv.int32_value);
  }
}

TEST_F(PubSubTest, PublisherCountMatchedSubscriptions)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/count_test", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  size_t count = 0;
  EXPECT_EQ(RMW_RET_OK, rmw_publisher_count_matched_subscriptions(pub, &count));
  EXPECT_EQ(0u, count);

  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/count_test", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  EXPECT_EQ(RMW_RET_OK, rmw_publisher_count_matched_subscriptions(pub, &count));
  EXPECT_EQ(1u, count);
}

TEST_F(PubSubTest, PublisherGetGid)
{
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/gid_test", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  rmw_gid_t gid;
  EXPECT_EQ(RMW_RET_OK, rmw_get_gid_for_publisher(pub, &gid));
  EXPECT_EQ(uds_id(), gid.implementation_identifier);

  // GID should not be all zeros
  bool all_zero = true;
  for (size_t i = 0; i < RMW_GID_STORAGE_SIZE; ++i) {
    if (gid.data[i] != 0) { all_zero = false; break; }
  }
  EXPECT_FALSE(all_zero);
}

TEST_F(PubSubTest, LargeMessageRoundtrip)
{
  // A payload well above SHM_PAYLOAD_THRESHOLD travels through the
  // publisher's /dev/shm ring: the datagram carries only a descriptor and
  // the subscriber copies the bytes out of the ring. This exercises the
  // full path — stage on publish, descriptor resolve on drain, deserialize
  // on take — through the public API.
  auto seq_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::UnboundedSequences>();
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, seq_ts, "/large_topic", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, seq_ts, "/large_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::UnboundedSequences send_msg;
  send_msg.uint8_values.resize(300 * 1024);  // ~300 KB serialized
  for (size_t i = 0; i < send_msg.uint8_values.size(); ++i) {
    send_msg.uint8_values[i] = static_cast<uint8_t>((i * 13 + 5) & 0xFF);
  }
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  test_msgs::msg::UnboundedSequences recv_msg;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ(send_msg.uint8_values, recv_msg.uint8_values);
}

TEST_F(PubSubTest, StringMessages)
{
  auto str_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::Strings>();
  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, str_ts, "/str_topic", &qos, &pub_opts);
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, str_ts, "/str_topic", &qos, &sub_opts);
  ASSERT_NE(nullptr, pub);
  ASSERT_NE(nullptr, sub);

  test_msgs::msg::Strings send_msg;
  send_msg.string_value = "Hello from Unix sockets!";
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &send_msg, nullptr));

  test_msgs::msg::Strings recv_msg;
  bool taken = false;
  EXPECT_EQ(RMW_RET_OK, rmw_take(sub, &recv_msg, &taken, nullptr));
  ASSERT_TRUE(taken);
  EXPECT_EQ("Hello from Unix sockets!", recv_msg.string_value);
}

// Regression for the rmw_take_sequence cursor bug: a deserialize failure in the
// middle of a batch must not leave an uninitialized hole or miscount size — each
// success is written at the *taken cursor, not the loop index. We produce
// [good, corrupt, good] entirely through the public API: the corrupt element
// comes from a second publisher on the same topic with a smaller message type
// (Empty), whose CDR payload is too short to deserialize as BasicTypes. Both
// publishers share the node's send socket, so a single SOCK_DGRAM sender
// preserves order and the mismatched datagram lands between the two good ones.
// Before the fix the second good message was written at data[2] and lost, while
// size == *taken == 2 exposed data[1] (the hole from the failed deserialize) to
// the consumer as a valid message.
TEST_F(PubSubTest, TakeSequenceSkipsMidBatchCorruptContiguously)
{
  auto sub_opts = rmw_get_default_subscription_options();
  sub = rmw_create_subscription(node, ts, "/take_seq_corrupt", &qos, &sub_opts);
  ASSERT_NE(nullptr, sub);

  auto pub_opts = rmw_get_default_publisher_options();
  pub = rmw_create_publisher(node, ts, "/take_seq_corrupt", &qos, &pub_opts);
  ASSERT_NE(nullptr, pub);

  // Same topic, smaller type: fan-out matches by topic only, so its datagram
  // reaches the subscriber but fails CDR deserialization as BasicTypes.
  auto empty_ts = rosidl_typesupport_cpp::get_message_type_support_handle<
    test_msgs::msg::Empty>();
  rmw_publisher_t * corrupt_pub =
    rmw_create_publisher(node, empty_ts, "/take_seq_corrupt", &qos, &pub_opts);
  ASSERT_NE(nullptr, corrupt_pub);

  test_msgs::msg::BasicTypes good;
  good.int32_value = 4242;
  good.bool_value = true;
  test_msgs::msg::Empty mismatched;
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &good, nullptr));
  EXPECT_EQ(RMW_RET_OK, rmw_publish(corrupt_pub, &mismatched, nullptr));
  EXPECT_EQ(RMW_RET_OK, rmw_publish(pub, &good, nullptr));
  EXPECT_EQ(RMW_RET_OK, rmw_destroy_publisher(node, corrupt_pub));

  std::this_thread::sleep_for(std::chrono::milliseconds(50));  // defensive; datagrams already buffered

  test_msgs::msg::BasicTypes out[3];
  void * out_ptrs[3] = {&out[0], &out[1], &out[2]};
  rmw_message_sequence_t seq;
  seq.data = out_ptrs;
  seq.size = 0;
  seq.capacity = 3;
  seq.allocator = nullptr;

  rmw_message_info_t infos[3];
  std::memset(infos, 0, sizeof(infos));
  rmw_message_info_sequence_t info_seq;
  info_seq.data = infos;
  info_seq.size = 0;
  info_seq.capacity = 3;
  info_seq.allocator = nullptr;

  size_t taken = 99;
  EXPECT_EQ(RMW_RET_OK, rmw_take_sequence(sub, 3, &seq, &info_seq, &taken, nullptr));

  EXPECT_EQ(2u, taken);
  EXPECT_EQ(2u, seq.size);
  EXPECT_EQ(2u, info_seq.size);
  EXPECT_EQ(4242, out[0].int32_value);
  EXPECT_EQ(4242, out[1].int32_value);  // the bug left this a hole and lost it
}
