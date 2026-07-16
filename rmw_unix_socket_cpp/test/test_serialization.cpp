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

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "../src/serialization.hpp"

#include "test_msgs/msg/basic_types.hpp"
#include "test_msgs/msg/strings.hpp"
#include "test_msgs/msg/arrays.hpp"
#include "test_msgs/msg/unbounded_sequences.hpp"
#include "test_msgs/msg/nested.hpp"
#include "test_msgs/msg/multi_nested.hpp"

#include "rosidl_typesupport_cpp/message_type_support.hpp"

// Helper: get fastrtps callbacks for a message type
template<typename T>
const message_type_support_callbacks_t * get_test_callbacks()
{
  auto * ts = rosidl_typesupport_cpp::get_message_type_support_handle<T>();
  return rmw_uds::get_callbacks(ts);
}

// Helper: serialize -> deserialize round-trip
template<typename T>
void round_trip(const T & input, T & output)
{
  auto * cb = get_test_callbacks<T>();
  ASSERT_NE(nullptr, cb);

  std::vector<uint8_t> buffer;
  ASSERT_TRUE(rmw_uds::serialize(&input, cb, buffer));
  EXPECT_GT(buffer.size(), 0u);

  ASSERT_TRUE(rmw_uds::deserialize(buffer.data(), buffer.size(), cb, &output));
}

TEST(SerializationTest, BasicTypes)
{
  test_msgs::msg::BasicTypes input;
  input.bool_value = true;
  input.byte_value = 42;
  input.char_value = 'A';
  input.float32_value = 3.14f;
  input.float64_value = 2.71828;
  input.int8_value = -10;
  input.uint8_value = 200;
  input.int16_value = -3000;
  input.uint16_value = 60000;
  input.int32_value = -100000;
  input.uint32_value = 3000000;
  input.int64_value = -9000000000LL;
  input.uint64_value = 18000000000ULL;

  test_msgs::msg::BasicTypes output;
  ASSERT_NO_FATAL_FAILURE(round_trip(input, output));

  EXPECT_EQ(input.bool_value, output.bool_value);
  EXPECT_EQ(input.byte_value, output.byte_value);
  EXPECT_EQ(input.char_value, output.char_value);
  EXPECT_FLOAT_EQ(input.float32_value, output.float32_value);
  EXPECT_DOUBLE_EQ(input.float64_value, output.float64_value);
  EXPECT_EQ(input.int8_value, output.int8_value);
  EXPECT_EQ(input.uint8_value, output.uint8_value);
  EXPECT_EQ(input.int16_value, output.int16_value);
  EXPECT_EQ(input.uint16_value, output.uint16_value);
  EXPECT_EQ(input.int32_value, output.int32_value);
  EXPECT_EQ(input.uint32_value, output.uint32_value);
  EXPECT_EQ(input.int64_value, output.int64_value);
  EXPECT_EQ(input.uint64_value, output.uint64_value);
}

TEST(SerializationTest, Strings)
{
  test_msgs::msg::Strings input;
  input.string_value = "Hello, ROS 2!";
  input.bounded_string_value = "bounded";

  test_msgs::msg::Strings output;
  ASSERT_NO_FATAL_FAILURE(round_trip(input, output));

  EXPECT_EQ(input.string_value, output.string_value);
  EXPECT_EQ(input.bounded_string_value, output.bounded_string_value);
}

TEST(SerializationTest, EmptyStrings)
{
  test_msgs::msg::Strings input;
  input.string_value = "";
  input.bounded_string_value = "";

  test_msgs::msg::Strings output;
  ASSERT_NO_FATAL_FAILURE(round_trip(input, output));

  EXPECT_EQ("", output.string_value);
  EXPECT_EQ("", output.bounded_string_value);
}

TEST(SerializationTest, NestedMessage)
{
  test_msgs::msg::Nested input;
  input.basic_types_value.bool_value = true;
  input.basic_types_value.int32_value = 42;
  input.basic_types_value.float64_value = 1.234;

  test_msgs::msg::Nested output;
  ASSERT_NO_FATAL_FAILURE(round_trip(input, output));

  EXPECT_EQ(input.basic_types_value.bool_value, output.basic_types_value.bool_value);
  EXPECT_EQ(input.basic_types_value.int32_value, output.basic_types_value.int32_value);
  EXPECT_DOUBLE_EQ(input.basic_types_value.float64_value, output.basic_types_value.float64_value);
}

TEST(SerializationTest, UnboundedSequences)
{
  test_msgs::msg::UnboundedSequences input;
  input.int32_values = {1, 2, 3, 4, 5};
  input.string_values = {"hello", "world", "test"};
  input.bool_values = {true, false, true};
  input.float64_values = {1.1, 2.2, 3.3};

  test_msgs::msg::UnboundedSequences output;
  ASSERT_NO_FATAL_FAILURE(round_trip(input, output));

  EXPECT_EQ(input.int32_values, output.int32_values);
  EXPECT_EQ(input.string_values, output.string_values);
  EXPECT_EQ(input.bool_values, output.bool_values);
  EXPECT_EQ(input.float64_values, output.float64_values);
}

TEST(SerializationTest, EmptySequences)
{
  test_msgs::msg::UnboundedSequences input;

  test_msgs::msg::UnboundedSequences output;
  ASSERT_NO_FATAL_FAILURE(round_trip(input, output));

  EXPECT_TRUE(output.int32_values.empty());
  EXPECT_TRUE(output.string_values.empty());
}

TEST(SerializationTest, Arrays)
{
  test_msgs::msg::Arrays input;
  input.bool_values = {true, false, true};
  input.int32_values = {100, 200, 300};
  input.float64_values = {1.1, 2.2, 3.3};
  input.string_values = {"one", "two", "three"};

  test_msgs::msg::Arrays output;
  ASSERT_NO_FATAL_FAILURE(round_trip(input, output));

  EXPECT_EQ(input.bool_values, output.bool_values);
  EXPECT_EQ(input.int32_values, output.int32_values);
  EXPECT_EQ(input.float64_values, output.float64_values);
  EXPECT_EQ(input.string_values, output.string_values);
}

TEST(SerializationTest, LargeString)
{
  test_msgs::msg::Strings input;
  input.string_value = std::string(10000, 'X');

  test_msgs::msg::Strings output;
  ASSERT_NO_FATAL_FAILURE(round_trip(input, output));

  EXPECT_EQ(input.string_value, output.string_value);
}

TEST(SerializationTest, MultiNested)
{
  test_msgs::msg::MultiNested input;

  // Add elements to unbounded sequence of unbounded sequences
  test_msgs::msg::UnboundedSequences elem;
  elem.int32_values = {10, 20, 30};
  elem.string_values = {"nested", "strings"};
  elem.bool_values = {true, false};
  input.unbounded_sequence_of_unbounded_sequences.push_back(elem);

  elem.int32_values = {40, 50};
  elem.string_values = {"more"};
  input.unbounded_sequence_of_unbounded_sequences.push_back(elem);

  test_msgs::msg::MultiNested output;
  ASSERT_NO_FATAL_FAILURE(round_trip(input, output));

  ASSERT_EQ(2u, output.unbounded_sequence_of_unbounded_sequences.size());
  EXPECT_EQ(
    input.unbounded_sequence_of_unbounded_sequences[0].int32_values,
    output.unbounded_sequence_of_unbounded_sequences[0].int32_values);
  EXPECT_EQ(
    input.unbounded_sequence_of_unbounded_sequences[0].string_values,
    output.unbounded_sequence_of_unbounded_sequences[0].string_values);
  EXPECT_EQ(
    input.unbounded_sequence_of_unbounded_sequences[1].string_values,
    output.unbounded_sequence_of_unbounded_sequences[1].string_values);
}

// --- serialize_into: the direct-into-ring variant of serialize() ---

// serialize_into must be wire-equivalent to serialize(): same length, and the
// bytes decode to an equal message. Not byte-identical — fastCDR skips CDR
// alignment padding rather than writing it, so padding bytes carry whatever
// the destination held (zeros in serialize()'s fresh vector, stale ring bytes
// in a reused record). Decoders skip those same bytes, so receivers cannot
// tell which path a message took. The 0xCD poison makes any NON-padding byte
// the writer failed to produce show up as a decode mismatch.
template<typename T>
void expect_into_equivalent(const T & input)
{
  auto * cb = get_test_callbacks<T>();
  ASSERT_NE(nullptr, cb);

  std::vector<uint8_t> reference;
  ASSERT_TRUE(rmw_uds::serialize(&input, cb, reference));

  size_t est = 0;
  ASSERT_TRUE(rmw_uds::serialized_size(&input, cb, est));
  ASSERT_GE(est, reference.size()) << "size walk must be exact or an upper bound";

  std::vector<uint8_t> direct(est, 0xCD);  // poison: unwritten bytes are visible
  size_t actual = 0;
  ASSERT_TRUE(rmw_uds::serialize_into(&input, cb, direct.data(), est, actual));
  ASSERT_EQ(reference.size(), actual);

  T decoded{};
  ASSERT_TRUE(rmw_uds::deserialize(direct.data(), actual, cb, &decoded));
  EXPECT_EQ(input, decoded);
}

TEST(SerializationTest, SerializeIntoMatchesSerializeBasicTypes)
{
  test_msgs::msg::BasicTypes msg;
  msg.int32_value = 1234;
  msg.float64_value = 3.14;
  expect_into_equivalent(msg);
}

TEST(SerializationTest, SerializeIntoMatchesSerializeLargeSequence)
{
  test_msgs::msg::UnboundedSequences msg;
  msg.uint8_values.resize(5 * 1024 * 1024);
  for (size_t i = 0; i < msg.uint8_values.size(); ++i) {
    msg.uint8_values[i] = static_cast<uint8_t>((i * 131 + 7) & 0xFF);
  }
  expect_into_equivalent(msg);
}

TEST(SerializationTest, SerializeIntoMatchesSerializeMultiNested)
{
  test_msgs::msg::MultiNested msg;  // field-heavy type, exercises alignment
  expect_into_equivalent(msg);
}

TEST(SerializationTest, SerializeIntoRejectsTooSmallCapacity)
{
  test_msgs::msg::UnboundedSequences msg;
  msg.uint8_values.resize(64 * 1024);
  auto * cb = get_test_callbacks<test_msgs::msg::UnboundedSequences>();
  ASSERT_NE(nullptr, cb);

  std::vector<uint8_t> buf(1024);  // far too small
  size_t actual = 0;
  EXPECT_FALSE(rmw_uds::serialize_into(&msg, cb, buf.data(), buf.size(), actual))
    << "overflowing the capacity must fail, never write out of bounds";
}
