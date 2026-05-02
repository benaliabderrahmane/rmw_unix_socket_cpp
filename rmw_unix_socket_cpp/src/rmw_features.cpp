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

  auto set_reason = [&](const char * msg) {
      if (reason && reason_size > 0) {
        std::snprintf(reason, reason_size, "%s", msg);
      }
    };

  // Reliability: BEST_EFFORT pub + RELIABLE sub = incompatible
  auto pub_rel = publisher_profile.reliability;
  auto sub_rel = subscription_profile.reliability;
  if (pub_rel == RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT &&
    sub_rel == RMW_QOS_POLICY_RELIABILITY_RELIABLE)
  {
    *compatibility = RMW_QOS_COMPATIBILITY_WARNING;
    set_reason("best effort publisher with reliable subscriber");
  }

  // Durability: VOLATILE pub + TRANSIENT_LOCAL sub = incompatible
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
