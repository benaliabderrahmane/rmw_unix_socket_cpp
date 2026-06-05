#include "registry.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <sched.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "logging.hpp"

namespace rmw_uds
{

// Lock-free atomics in shared memory require both lock-freedom AND
// address-freedom. On every Linux target we support these hold; assert at
// compile time so we fail loud on exotic platforms.
static_assert(
  std::atomic<uint8_t>::is_always_lock_free,
  "std::atomic<uint8_t> must be always lock-free for cross-process shm use");
static_assert(
  std::atomic<uint32_t>::is_always_lock_free,
  "std::atomic<uint32_t> must be always lock-free for cross-process shm use");
static_assert(
  std::atomic<uint64_t>::is_always_lock_free,
  "std::atomic<uint64_t> must be always lock-free for cross-process shm use");

// Bounded reader retries before we give up and treat the slot as "unstable
// right now, skip it for this query". A persistent writer would otherwise
// spin readers forever.
static constexpr int MAX_READ_RETRIES = 16;

static size_t compute_registry_size()
{
  return sizeof(RegistryHeader) + REGISTRY_MAX_ENTRIES * sizeof(RegistryEntrySlot);
}

// Initialize a freshly-created shm region. Called by the unique creator.
static void initialize_header(RegistryHeader * header, size_t total_size)
{
  // Zero the entire shm region. Cast to void* to suppress -Wclass-memaccess:
  // the header has non-trivial members (atomics, pthread_mutex_t) that we
  // (re-)initialize explicitly below; the slot array's atomics work fine when
  // backed by zeroed memory.
  std::memset(static_cast<void *>(header), 0, total_size);
  header->max_entries = REGISTRY_MAX_ENTRIES;
  header->generation.store(0, std::memory_order_relaxed);

  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
  pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
  pthread_mutex_init(&header->init_mutex, &attr);
  pthread_mutexattr_destroy(&attr);

  // Publish version LAST with release ordering. Other processes spin on this
  // field and only proceed once they see REGISTRY_VERSION.
  header->version.store(REGISTRY_VERSION, std::memory_order_release);
}

int registry_open(size_t domain_id, void ** out_ptr, size_t * out_size)
{
  char name[64];
  std::snprintf(name, sizeof(name), "/ros2_uds_%zu", domain_id);

  size_t total_size = compute_registry_size();

  // We may need to retry once if we detect a stale shm file from an
  // incompatible layout version (different REGISTRY_VERSION or different
  // size). One retry is enough: after the unlink, the next O_CREAT|O_EXCL
  // creates a fresh file at the right size.
  for (int attempt = 0; attempt < 2; ++attempt) {
    bool we_created = false;
    int fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0666);
    if (fd >= 0) {
      we_created = true;
      if (ftruncate(fd, static_cast<off_t>(total_size)) != 0) {
        close(fd);
        shm_unlink(name);
        return -1;
      }
    } else if (errno == EEXIST) {
      fd = shm_open(name, O_RDWR, 0666);
      if (fd < 0) {
        return -1;
      }
      // Verify the existing file is sized for our layout. If not, it was
      // created by an incompatible build of this RMW (different version or
      // different REGISTRY_MAX_ENTRIES). mmap'ing past the actual file size
      // would SIGBUS on first slot-array access, so we must reclaim the file.
      struct stat st;
      if (fstat(fd, &st) != 0) {
        close(fd);
        return -1;
      }
      if (static_cast<size_t>(st.st_size) != total_size) {
        close(fd);
        shm_unlink(name);
        continue;  // retry create-exclusive
      }
    } else {
      return -1;
    }

    void * ptr = mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
      close(fd);
      return -1;
    }

    auto * header = static_cast<RegistryHeader *>(ptr);

    if (we_created) {
      initialize_header(header, total_size);
    } else {
      // Wait for the creator to finish initializing. Bounded spin so we
      // don't hang if the creator died mid-init.
      bool initialized = false;
      for (int i = 0; i < 10000; ++i) {
        if (header->version.load(std::memory_order_acquire) == REGISTRY_VERSION) {
          initialized = true;
          break;
        }
        sched_yield();
      }
      if (!initialized) {
        uint32_t v = header->version.load(std::memory_order_acquire);
        if (v != 0 && v != REGISTRY_VERSION) {
          // File exists with the right size but a different layout-version
          // tag (very rare: same struct sizes by coincidence, different
          // semantics). Treat as stale, unlink, and retry.
          munmap(ptr, total_size);
          close(fd);
          shm_unlink(name);
          continue;
        }
        // Best-effort init: creator crashed in the ~ms window during init.
        initialize_header(header, total_size);
      }
    }

    *out_ptr = ptr;
    *out_size = total_size;
    return fd;
  }
  return -1;
}

void registry_close(int fd, void * ptr, size_t size)
{
  if (ptr && ptr != MAP_FAILED) {
    munmap(ptr, size);
  }
  if (fd >= 0) {
    close(fd);
  }
}

// Snapshot one slot under the seqlock protocol. Returns true on a successful
// snapshot of a non-empty slot, false if the slot is empty / we gave up.
// On success, *out_seq (if non-null) receives the consistent even seq the
// snapshot was taken at, so callers can detect a later remove+re-add (ABA).
static bool snapshot_slot(RegistryEntrySlot * slot, RegistryEntrySlot * out, uint32_t * out_seq = nullptr)
{
  for (int retry = 0; retry < MAX_READ_RETRIES; ++retry) {
    uint32_t s1 = slot->seq.load(std::memory_order_acquire);
    if (s1 & 1u) {
      sched_yield();
      continue;
    }
    uint8_t st = slot->state.load(std::memory_order_acquire);
    if (st == ENTRY_EMPTY || st == ENTRY_RESERVED) {
      // RESERVED = a writer claimed the slot but has not yet published its
      // payload. Treat it as empty so we never snapshot a half-built slot.
      return false;
    }

    // Read payload into the output buffer. The non-atomic fields can race
    // with a concurrent writer; the seq re-read below detects torn snapshots.
    out->state.store(st, std::memory_order_relaxed);
    out->pid = slot->pid;
    std::memcpy(out->gid, slot->gid, sizeof(out->gid));
    std::memcpy(out->node_name, slot->node_name, sizeof(out->node_name));
    std::memcpy(out->node_namespace, slot->node_namespace, sizeof(out->node_namespace));
    std::memcpy(out->topic_name, slot->topic_name, sizeof(out->topic_name));
    std::memcpy(out->type_name, slot->type_name, sizeof(out->type_name));
    std::memcpy(out->socket_path, slot->socket_path, sizeof(out->socket_path));
    out->qos_reliability = slot->qos_reliability;
    out->qos_durability = slot->qos_durability;
    out->qos_history = slot->qos_history;
    out->qos_depth = slot->qos_depth;

    uint32_t s2 = slot->seq.load(std::memory_order_acquire);
    if (s1 == s2) {
      if (out_seq) {
        *out_seq = s1;
      }
      return true;
    }
    // Writer touched the slot — retry.
  }
  return false;  // gave up after MAX_READ_RETRIES
}

// Write the entry's payload into the slot. Caller must already own the slot
// (via successful CAS on state).
static void write_slot_payload(RegistryEntrySlot * slot, const RegistryEntry & entry)
{
  // Begin write: seq -> odd.
  slot->seq.fetch_add(1, std::memory_order_acq_rel);

  slot->pid = entry.pid;
  std::memcpy(slot->gid, entry.gid, sizeof(slot->gid));
  std::memcpy(slot->node_name, entry.node_name, sizeof(slot->node_name));
  std::memcpy(slot->node_namespace, entry.node_namespace, sizeof(slot->node_namespace));
  std::memcpy(slot->topic_name, entry.topic_name, sizeof(slot->topic_name));
  std::memcpy(slot->type_name, entry.type_name, sizeof(slot->type_name));
  std::memcpy(slot->socket_path, entry.socket_path, sizeof(slot->socket_path));
  slot->qos_reliability = entry.qos_reliability;
  slot->qos_durability = entry.qos_durability;
  slot->qos_history = entry.qos_history;
  slot->qos_depth = entry.qos_depth;

  // End write: seq -> even.
  slot->seq.fetch_add(1, std::memory_order_acq_rel);
}

// One pass: try to claim an empty slot and store the entry. Returns slot
// index on success, -1 if no empty slot was found.
static int32_t try_add_once(RegistryHeader * header, const RegistryEntry & entry)
{
  auto * slots = registry_slots(header);
  uint32_t max = header->max_entries;

  for (uint32_t i = 0; i < max; ++i) {
    uint8_t expected = ENTRY_EMPTY;
    // CAS state EMPTY -> RESERVED. Winner owns the slot, but readers still see
    // it as empty until we publish the real type below — so no reader can
    // snapshot the slot before its payload is committed under the seqlock.
    if (slots[i].state.compare_exchange_strong(
        expected, static_cast<uint8_t>(ENTRY_RESERVED),
        std::memory_order_acq_rel, std::memory_order_relaxed))
    {
      write_slot_payload(&slots[i], entry);
      // Payload is committed (seq even). Publish the real type with release
      // ordering so any reader that now sees it also sees the payload.
      slots[i].state.store(static_cast<uint8_t>(entry.type), std::memory_order_release);
      header->generation.fetch_add(1, std::memory_order_acq_rel);
      return static_cast<int32_t>(i);
    }
  }
  return -1;
}

int32_t registry_add(RegistryHeader * header, const RegistryEntry & entry)
{
  int32_t idx = try_add_once(header, entry);
  if (idx >= 0) {
    return idx;
  }
  // No empty slots. Reclaim any whose owner PID is dead, then retry once.
  registry_cleanup_stale(header);
  return try_add_once(header, entry);
}

// Best-effort tear-down of a slot we already claimed (state already EMPTY).
// Unlink the socket file and zero out the payload so the next reuse starts
// from a clean state. Wrapped in a seqlock so any reader still in the middle
// of a snapshot retries.
static void teardown_slot(RegistryEntrySlot * slot)
{
  slot->seq.fetch_add(1, std::memory_order_acq_rel);

  char path_copy[sizeof(slot->socket_path)];
  std::memcpy(path_copy, slot->socket_path, sizeof(path_copy));

  slot->pid = 0;
  std::memset(slot->gid, 0, sizeof(slot->gid));
  std::memset(slot->node_name, 0, sizeof(slot->node_name));
  std::memset(slot->node_namespace, 0, sizeof(slot->node_namespace));
  std::memset(slot->topic_name, 0, sizeof(slot->topic_name));
  std::memset(slot->type_name, 0, sizeof(slot->type_name));
  std::memset(slot->socket_path, 0, sizeof(slot->socket_path));
  slot->qos_reliability = 0;
  slot->qos_durability = 0;
  slot->qos_history = 0;
  slot->qos_depth = 0;

  slot->seq.fetch_add(1, std::memory_order_acq_rel);

  // Unlink outside the seqlock — filesystem op, doesn't need to be observable
  // by other readers atomically.
  if (path_copy[0] != '\0') {
    unlink(path_copy);
  }
}

void registry_remove(RegistryHeader * header, int32_t index)
{
  if (index < 0 || static_cast<uint32_t>(index) >= header->max_entries) {
    return;
  }
  auto * slot = &registry_slots(header)[index];

  uint8_t st = slot->state.load(std::memory_order_acquire);
  if (st == ENTRY_EMPTY) {
    return;
  }
  // CAS state -> EMPTY. Winner owns the teardown.
  if (!slot->state.compare_exchange_strong(
      st, static_cast<uint8_t>(ENTRY_EMPTY),
      std::memory_order_acq_rel, std::memory_order_relaxed))
  {
    return;  // someone else removed it
  }
  teardown_slot(slot);
  header->generation.fetch_add(1, std::memory_order_acq_rel);
}

// Best-effort: stat /proc/<pid>. ENOENT means the PID is not in our
// namespace (dead or in another container) and we treat it as reclaimable.
static bool pid_is_alive_or_unreachable(pid_t pid)
{
  if (pid <= 0) {
    return false;
  }
  char proc_path[32];
  std::snprintf(proc_path, sizeof(proc_path), "/proc/%d", pid);
  struct stat st;
  if (stat(proc_path, &st) == 0) {
    return true;
  }
  return errno != ENOENT;  // Other errors -> be conservative, treat as alive
}

// Human-readable name for a slot's type — used only in log lines.
static const char * entry_type_name(uint8_t t)
{
  switch (t) {
    case ENTRY_NODE: return "node";
    case ENTRY_PUBLISHER: return "publisher";
    case ENTRY_SUBSCRIPTION: return "subscription";
    case ENTRY_SERVICE: return "service";
    case ENTRY_CLIENT: return "client";
    default: return "?";
  }
}

void registry_cleanup_stale(RegistryHeader * header)
{
  auto * slots = registry_slots(header);
  uint32_t max = header->max_entries;

  for (uint32_t i = 0; i < max; ++i) {
    // Fast-skip empty slots without snapshotting, mirroring registry_query.
    // snapshot_slot would also bail on EMPTY, but only after loading seq; this
    // drops that load for the empty slots in the 32768-slot scan. A slot that
    // turns non-empty just after the skip is reclaimed on the next sweep.
    if (slots[i].state.load(std::memory_order_acquire) == ENTRY_EMPTY) {
      continue;
    }
    RegistryEntrySlot snap;
    uint32_t snap_seq;
    if (!snapshot_slot(&slots[i], &snap, &snap_seq)) {
      continue;
    }
    if (snap.pid == 0) {
      // pid==0 is never a real entry — only a transient snapshot of a slot
      // whose payload is not yet written. Never reclaim it.
      continue;
    }
    if (pid_is_alive_or_unreachable(snap.pid)) {
      continue;
    }
    // ABA guard: a remove + live re-add of the same type leaves the state
    // value unchanged but advances seq. Confirm the slot has not been touched
    // since the snapshot, otherwise we would tear down a live entry. The
    // acquire load is ordered before the acq_rel CAS by the control dependency.
    if (slots[i].seq.load(std::memory_order_acquire) != snap_seq) {
      continue;  // slot was rewritten since snapshot — not the entry we vetted
    }
    // Try to claim the slot for removal. The CAS-expected value is the state
    // we observed in the snapshot.
    uint8_t expected = snap.state.load(std::memory_order_relaxed);
    if (!slots[i].state.compare_exchange_strong(
        expected, static_cast<uint8_t>(ENTRY_EMPTY),
        std::memory_order_acq_rel, std::memory_order_relaxed))
    {
      continue;  // raced with another remover / writer
    }

    // Surface the reclaim so a crashed-node incident leaves a breadcrumb
    // in the ROS log. WARN level — this is an ungraceful exit, not routine.
    RMW_UDS_LOG_WARN(
      "reclaimed stale registry slot %u: %s pid=%d node=%s%s topic=%s — "
      "owning process is gone (ungraceful exit)",
      i, entry_type_name(expected),
      static_cast<int>(snap.pid),
      snap.node_namespace[0] ? snap.node_namespace : "",
      snap.node_name,
      snap.topic_name[0] ? snap.topic_name : "(none)");

    teardown_slot(&slots[i]);
    header->generation.fetch_add(1, std::memory_order_acq_rel);
  }
}

uint64_t registry_generation(RegistryHeader * header)
{
  return header->generation.load(std::memory_order_acquire);
}

static rmw_qos_profile_t snapshot_to_qos(const RegistryEntrySlot & s)
{
  rmw_qos_profile_t qos;
  std::memset(&qos, 0, sizeof(qos));
  qos.reliability = static_cast<rmw_qos_reliability_policy_e>(s.qos_reliability);
  qos.durability = static_cast<rmw_qos_durability_policy_e>(s.qos_durability);
  qos.history = static_cast<rmw_qos_history_policy_e>(s.qos_history);
  qos.depth = s.qos_depth;
  return qos;
}

std::vector<RegistryQueryResult> registry_query(
  RegistryHeader * header,
  RegistryEntryType type_filter,
  const char * topic_filter,
  const char * node_name_filter,
  const char * node_namespace_filter)
{
  std::vector<RegistryQueryResult> results;
  auto * slots = registry_slots(header);
  uint32_t max = header->max_entries;

  for (uint32_t i = 0; i < max; ++i) {
    // Fast pre-check: skip clearly-empty slots without snapshotting.
    uint8_t pre_state = slots[i].state.load(std::memory_order_acquire);
    if (pre_state == ENTRY_EMPTY) {
      continue;
    }
    // Type pre-filter: skip the full snapshot for slots whose current type
    // can't match. pre_state is only an early-out — a slot rewritten to a
    // different type between this load and the snapshot is still rejected by
    // the authoritative post-snapshot type re-check below, so results stay
    // seqlock-consistent. (RESERVED/EMPTY never equal a real type_filter.)
    if (type_filter != ENTRY_EMPTY && pre_state != static_cast<uint8_t>(type_filter)) {
      continue;
    }

    RegistryEntrySlot snap;
    if (!snapshot_slot(&slots[i], &snap)) {
      continue;
    }

    uint8_t st = snap.state.load(std::memory_order_relaxed);
    if (st == ENTRY_EMPTY) {
      continue;
    }
    if (type_filter != ENTRY_EMPTY && st != static_cast<uint8_t>(type_filter)) {
      continue;
    }
    if (topic_filter && std::strcmp(snap.topic_name, topic_filter) != 0) {
      continue;
    }
    if (node_name_filter && std::strcmp(snap.node_name, node_name_filter) != 0) {
      continue;
    }
    if (node_namespace_filter &&
      std::strcmp(snap.node_namespace, node_namespace_filter) != 0)
    {
      continue;
    }

    RegistryQueryResult r;
    r.node_name = snap.node_name;
    r.node_namespace = snap.node_namespace;
    r.topic_name = snap.topic_name;
    r.type_name = snap.type_name;
    r.socket_path = snap.socket_path;
    std::memcpy(r.gid, snap.gid, sizeof(r.gid));
    r.type = static_cast<RegistryEntryType>(st);
    r.qos = snapshot_to_qos(snap);
    results.push_back(r);
  }

  return results;
}

}  // namespace rmw_uds
