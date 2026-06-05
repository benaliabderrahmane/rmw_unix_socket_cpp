#ifndef RMW_UNIX_SOCKET_CPP__REGISTRY_HPP_
#define RMW_UNIX_SOCKET_CPP__REGISTRY_HPP_

#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include <pthread.h>
#include <sys/types.h>

#include "rmw/types.h"

namespace rmw_uds
{

static constexpr uint32_t REGISTRY_MAX_ENTRIES = 32768;
// Layout v3: per-slot seqlock + atomic state. No global mutex on hot paths.
static constexpr uint32_t REGISTRY_VERSION = 3;

enum RegistryEntryType : uint8_t
{
  ENTRY_EMPTY = 0,
  ENTRY_NODE,
  ENTRY_PUBLISHER,
  ENTRY_SUBSCRIPTION,
  ENTRY_SERVICE,
  ENTRY_CLIENT,
  // Transient claim state: a writer won the slot but has not yet committed its
  // payload. Readers treat it like ENTRY_EMPTY so they never observe a slot
  // before its payload is published. Never stored in a RegistryEntry; lives
  // only in the slot's atomic state field, so it costs no struct layout.
  ENTRY_RESERVED = 0xFF,
};

// User-facing struct, passed to registry_add. Plain data, no atomics.
struct RegistryEntry
{
  RegistryEntryType type;
  uint8_t pad[3];
  pid_t pid;
  uint8_t gid[16];
  char node_name[256];
  char node_namespace[256];
  char topic_name[256];
  char type_name[256];
  char socket_path[108];
  uint8_t qos_reliability;
  uint8_t qos_durability;
  uint8_t qos_history;
  uint8_t pad2;
  uint32_t qos_depth;
};

// Internal slot layout as stored in shared memory. Implements a per-slot
// seqlock + atomic state machine so readers never need a global mutex.
//
// Protocol (writer):
//   1. CAS state EMPTY -> RESERVED       (claims the slot; readers treat as EMPTY)
//   2. seq.fetch_add(1, acq_rel)         -> seq becomes odd (write in progress)
//   3. memcpy payload fields
//   4. seq.fetch_add(1, acq_rel)         -> seq becomes even (snapshot stable)
//   5. state.store(target_type, release) -> publishes the committed payload
//   6. generation.fetch_add(1, acq_rel)  -> wakes cached readers
//
// Protocol (reader):
//   1. s1 = seq.load(acquire); if (s1 & 1) skip slot (writer in progress)
//   2. st = state.load(acquire); if (st == EMPTY) skip
//   3. memcpy payload into local snapshot
//   4. s2 = seq.load(acquire); if (s1 != s2) retry (bounded retries, then skip)
//
// Protocol (remove):
//   1. CAS state <current> -> EMPTY      (claims removal; loses race => abort)
//   2. seq.fetch_add(1)                  -> begin payload teardown
//   3. unlink(socket_path); memset payload
//   4. seq.fetch_add(1)                  -> end teardown
//   5. generation.fetch_add(1)
//
// All atomics are lock-free + address-free (std::atomic<u8/u32>::is_always_lock_free
// is true on every Linux target we support), so they work across process
// boundaries via the shared mmap.
struct RegistryEntrySlot
{
  std::atomic<uint32_t> seq;     // even = stable, odd = mid-write
  std::atomic<uint8_t> state;    // RegistryEntryType
  uint8_t pad[3];
  pid_t pid;
  uint8_t gid[16];
  char node_name[256];
  char node_namespace[256];
  char topic_name[256];
  char type_name[256];
  char socket_path[108];
  uint8_t qos_reliability;
  uint8_t qos_durability;
  uint8_t qos_history;
  uint8_t pad2;
  uint32_t qos_depth;
};

struct RegistryHeader
{
  std::atomic<uint32_t> version;
  uint32_t max_entries;
  std::atomic<uint64_t> generation;
  // Robust mutex retained only for the one-time init handshake on shm creation.
  // Hot paths (add / remove / query / cleanup) never take it.
  pthread_mutex_t init_mutex;
};

// Open or create the shared memory registry for the given domain_id.
// Returns fd >= 0 on success, sets *out_ptr and *out_size.
int registry_open(
  size_t domain_id, void ** out_ptr, size_t * out_size);

// Close the shared memory registry.
void registry_close(int fd, void * ptr, size_t size);

// Add an entry. Lock-free. Returns slot index or -1 if registry is full
// (after attempting to reclaim stale slots).
int32_t registry_add(RegistryHeader * header, const RegistryEntry & entry);

// Remove an entry by index. Lock-free. No-op if index is out of range or
// the slot is already empty.
void registry_remove(RegistryHeader * header, int32_t index);

// Sweep the registry, removing slots whose owning PID is no longer alive
// (per /proc/<pid>). Lock-free; safe to call concurrently with add/remove.
void registry_cleanup_stale(RegistryHeader * header);

// Current generation counter. Bumped on every successful add/remove.
uint64_t registry_generation(RegistryHeader * header);

// Query helpers — return matching entries.
struct RegistryQueryResult
{
  std::string node_name;
  std::string node_namespace;
  std::string topic_name;
  std::string type_name;
  std::string socket_path;
  uint8_t gid[16];
  RegistryEntryType type;
  rmw_qos_profile_t qos;
};

// Iterate all slots and return entries matching all non-null filters.
// type_filter == ENTRY_EMPTY matches any type. Lock-free; readers may run
// concurrently with writers without external synchronization.
std::vector<RegistryQueryResult> registry_query(
  RegistryHeader * header,
  RegistryEntryType type_filter,
  const char * topic_filter,
  const char * node_name_filter,
  const char * node_namespace_filter);

inline RegistryHeader * registry_header(void * ptr)
{
  return static_cast<RegistryHeader *>(ptr);
}

// Internal: pointer to the slot array immediately after the header in shm.
// Exposed for tests; production code should use the registry_* functions.
inline RegistryEntrySlot * registry_slots(RegistryHeader * header)
{
  return reinterpret_cast<RegistryEntrySlot *>(
    reinterpret_cast<uint8_t *>(header) + sizeof(RegistryHeader));
}

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__REGISTRY_HPP_
