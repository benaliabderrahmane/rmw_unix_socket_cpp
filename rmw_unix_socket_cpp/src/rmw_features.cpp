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

#include "identifier.hpp"

#include <cstdio>

#include "rmw/error_handling.h"
#include "rmw/features.h"
#include "rmw/qos_profiles.h"
#include "rmw/rmw.h"

extern "C"
{

const char * rmw_get_implementation_identifier(void)
{
  return rmw_uds::identifier;
}

const char * rmw_get_serialization_format(void)
{
  return rmw_uds::serialization_format;
}

bool rmw_feature_supported(rmw_feature_t feature)
{
  switch (feature) {
    case RMW_FEATURE_MESSAGE_INFO_PUBLICATION_SEQUENCE_NUMBER:
      return true;
    case RMW_FEATURE_MESSAGE_INFO_RECEPTION_SEQUENCE_NUMBER:
      return true;
    default:
      return false;
  }
}

rmw_ret_t rmw_set_log_severity(rmw_log_severity_t severity)
{
  (void)severity;
  return RMW_RET_OK;
}

rmw_ret_t rmw_qos_profile_check_compatible(
  const rmw_qos_profile_t publisher_profile,
  const rmw_qos_profile_t subscription_profile,
  rmw_qos_compatibility_type_t * compatibility,
  char * reason,
  size_t reason_size)
{
  RMW_CHECK_ARGUMENT_FOR_NULL(compatibility, RMW_RET_INVALID_ARGUMENT);

  *compatibility = RMW_QOS_COMPATIBILITY_OK;
  if (reason && reason_size > 0) {
    reason[0] = '\0';
  }

  // Appends msg to reason (with a "; " separator) so multiple mismatches are
  // all reported instead of the last one overwriting the rest.
  size_t reason_len = 0;
  auto set_reason = [&](const char * msg) {
      if (!reason || reason_size == 0 || reason_len >= reason_size - 1) {
        return;
      }
      const char * sep = (reason_len > 0) ? "; " : "";
      int n = std::snprintf(
        reason + reason_len, reason_size - reason_len, "%s%s", sep, msg);
      // snprintf returns the length it would have written; clamp to the
      // space actually consumed so reason_len never overruns the buffer.
      if (n > 0) {
        size_t avail = reason_size - reason_len - 1;
        reason_len += (static_cast<size_t>(n) < avail) ? static_cast<size_t>(n) : avail;
      }
    };

  // Reliability: BEST_EFFORT pub + RELIABLE sub = warning
  auto pub_rel = publisher_profile.reliability;
  auto sub_rel = subscription_profile.reliability;
  if (pub_rel == RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT &&
    sub_rel == RMW_QOS_POLICY_RELIABILITY_RELIABLE)
  {
    *compatibility = RMW_QOS_COMPATIBILITY_WARNING;
    set_reason("best effort publisher with reliable subscriber");
  }

  // Durability: VOLATILE pub + TRANSIENT_LOCAL sub = warning
  auto pub_dur = publisher_profile.durability;
  auto sub_dur = subscription_profile.durability;
  if (pub_dur == RMW_QOS_POLICY_DURABILITY_VOLATILE &&
    sub_dur == RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL)
  {
    *compatibility = RMW_QOS_COMPATIBILITY_WARNING;
    set_reason("volatile publisher with transient local subscriber");
  }

  return RMW_RET_OK;
}

}  // extern "C"
