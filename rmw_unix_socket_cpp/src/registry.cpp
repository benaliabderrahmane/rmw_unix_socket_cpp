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

#include "registry.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <sched.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "logging.hpp"

namespace rmw_uds
{

// Defined below; called after every generation bump (see its comment).
static void ring_doorbells(RegistryHeader * header);

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

// A seqlock reader copies a slot's payload while a writer may be storing it.
// With plain memcpy on both sides that overlap is a data race - undefined
// behaviour, even though the seq re-check discards a torn copy - so every
// access to a published slot's payload goes through these relaxed atomic
// copies, word by word where aligned. The seq counter and the fences around
// these calls give the ordering (Boehm, "Can seqlocks get along with
// programming language memory models?", MSPC 2012). C++17 has no atomic_ref,
// hence the builtins; may_alias lets them read the char arrays as words.
using ShmWord = uint64_t __attribute__((may_alias));
static_assert(__atomic_always_lock_free(sizeof(ShmWord), 0), "registry needs lock-free words");

static void shm_load_relaxed(void * dst, const void * shared, size_t n)
{
  auto * d = static_cast<unsigned char *>(dst);
  auto * s = static_cast<const unsigned char *>(shared);
  for (; n > 0 && reinterpret_cast<uintptr_t>(s) % sizeof(ShmWord) != 0; --n) {
    *d++ = __atomic_load_n(s++, __ATOMIC_RELAXED);
  }
  for (; n >= sizeof(ShmWord); n -= sizeof(ShmWord), s += sizeof(ShmWord), d += sizeof(ShmWord)) {
    const ShmWord w = __atomic_load_n(reinterpret_cast<const ShmWord *>(s), __ATOMIC_RELAXED);
    std::memcpy(d, &w, sizeof(w));
  }
  for (; n > 0; --n) {
    *d++ = __atomic_load_n(s++, __ATOMIC_RELAXED);
  }
}

// Copy a NUL-padded string field and stop after the word that holds its
// terminator. Registry names are a few dozen bytes in 108- to 256-byte fields,
// so a snapshot reads a few words per name instead of all of them. dst past the
// terminator is left as it was: every reader of a snapshot uses these fields as
// C strings. A torn copy can miss the terminator: n bounds it, and the seq
// re-check rejects it like any other torn copy.
static void shm_load_cstr_relaxed(char * dst, const char * shared, size_t n)
{
  size_t i = 0;
  bool done = false;
  for (; !done && i < n && reinterpret_cast<uintptr_t>(shared + i) % sizeof(ShmWord) != 0; ++i) {
    dst[i] = __atomic_load_n(shared + i, __ATOMIC_RELAXED);
    done = dst[i] == '\0';
  }
  for (; !done && i + sizeof(ShmWord) <= n; i += sizeof(ShmWord)) {
    const ShmWord w =
      __atomic_load_n(reinterpret_cast<const ShmWord *>(shared + i), __ATOMIC_RELAXED);
    std::memcpy(dst + i, &w, sizeof(w));
    done = ((w - 0x0101010101010101ull) & ~w & 0x8080808080808080ull) != 0;  // has a zero byte
  }
  for (; !done && i < n; ++i) {
    dst[i] = __atomic_load_n(shared + i, __ATOMIC_RELAXED);
    done = dst[i] == '\0';
  }
}

// The payload (pid .. qos_depth) is one contiguous span, laid out identically
// in a slot and in a RegistryEntry, so it is copied in a single call: aligned
// words end to end, with no per-field head and tail.
static constexpr size_t PAYLOAD_SIZE =
  sizeof(RegistryEntrySlot) - offsetof(RegistryEntrySlot, pid);
static_assert(
  PAYLOAD_SIZE == sizeof(RegistryEntry) - offsetof(RegistryEntry, pid),
  "slot and entry payload sizes differ");
static_assert(
  offsetof(RegistryEntrySlot, pid) % sizeof(ShmWord) == 0 &&
  sizeof(RegistryEntrySlot) % sizeof(ShmWord) == 0,
  "slot payloads must start on a word boundary in the slot array");
#define RMW_UDS_SAME_PAYLOAD_OFFSET(f) \
  static_assert( \
    offsetof(RegistryEntrySlot, f) - offsetof(RegistryEntrySlot, pid) == \
    offsetof(RegistryEntry, f) - offsetof(RegistryEntry, pid), \
    "slot and entry payloads diverge at " #f)
RMW_UDS_SAME_PAYLOAD_OFFSET(gid);
RMW_UDS_SAME_PAYLOAD_OFFSET(node_name);
RMW_UDS_SAME_PAYLOAD_OFFSET(node_namespace);
RMW_UDS_SAME_PAYLOAD_OFFSET(topic_name);
RMW_UDS_SAME_PAYLOAD_OFFSET(type_name);
RMW_UDS_SAME_PAYLOAD_OFFSET(socket_path);
RMW_UDS_SAME_PAYLOAD_OFFSET(qos_reliability);
RMW_UDS_SAME_PAYLOAD_OFFSET(qos_durability);
RMW_UDS_SAME_PAYLOAD_OFFSET(qos_history);
RMW_UDS_SAME_PAYLOAD_OFFSET(qos_depth);
#undef RMW_UDS_SAME_PAYLOAD_OFFSET

// src == nullptr stores zeros.
static void shm_store_relaxed(void * shared, const void * src, size_t n)
{
  auto * d = static_cast<unsigned char *>(shared);
  auto * s = static_cast<const unsigned char *>(src);
  auto next_byte = [&s]() -> unsigned char {return s ? *s++ : 0;};
  for (; n > 0 && reinterpret_cast<uintptr_t>(d) % sizeof(ShmWord) != 0; --n) {
    __atomic_store_n(d++, next_byte(), __ATOMIC_RELAXED);
  }
  for (; n >= sizeof(ShmWord); n -= sizeof(ShmWord), d += sizeof(ShmWord)) {
    ShmWord w = 0;
    if (s) {
      std::memcpy(&w, s, sizeof(w));
      s += sizeof(w);
    }
    __atomic_store_n(reinterpret_cast<ShmWord *>(d), w, __ATOMIC_RELAXED);
  }
  for (; n > 0; --n) {
    __atomic_store_n(d++, next_byte(), __ATOMIC_RELAXED);
  }
}

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
  header->high_water_slot.store(0, std::memory_order_relaxed);

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

    // Read payload into the output buffer. A concurrent writer can tear the
    // copy; the seq re-read below detects that. The copies are atomic so the
    // overlap is not a data race (see shm_load_relaxed).
    out->state.store(st, std::memory_order_relaxed);
    shm_load_relaxed(
      &out->pid, &slot->pid, offsetof(RegistryEntrySlot, node_name) - offsetof(RegistryEntrySlot, pid));
    shm_load_cstr_relaxed(out->node_name, slot->node_name, sizeof(out->node_name));
    shm_load_cstr_relaxed(out->node_namespace, slot->node_namespace, sizeof(out->node_namespace));
    shm_load_cstr_relaxed(out->topic_name, slot->topic_name, sizeof(out->topic_name));
    shm_load_cstr_relaxed(out->type_name, slot->type_name, sizeof(out->type_name));
    shm_load_cstr_relaxed(out->socket_path, slot->socket_path, sizeof(out->socket_path));
    shm_load_relaxed(
      &out->qos_reliability, &slot->qos_reliability,
      sizeof(RegistryEntrySlot) - offsetof(RegistryEntrySlot, qos_reliability));

    // Pairs with the writer's release fence: if any load above saw a byte the
    // writer stored, the re-read below sees its odd seq and the copy retries.
    std::atomic_thread_fence(std::memory_order_acquire);
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
  // Begin write: seq -> odd. The release fence keeps the payload stores
  // below from becoming visible to a reader before the odd seq does.
  slot->seq.fetch_add(1, std::memory_order_acq_rel);
  std::atomic_thread_fence(std::memory_order_release);

  shm_store_relaxed(&slot->pid, &entry.pid, PAYLOAD_SIZE);

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
      // Widen the high-water bound to max(current, i+1) BEFORE bumping the
      // generation. Readers gate their re-query on a generation acquire-load and
      // then read high_water; if this store were sequenced AFTER the release
      // generation bump, a reader could observe the new generation but a stale
      // high_water, scan [0, stale) and miss this just-added slot. Done first,
      // the relaxed store is sequenced-before the release fetch_add, so any
      // reader that synchronizes on the generation also observes high_water.
      uint32_t want = i + 1;
      uint32_t cur = header->high_water_slot.load(std::memory_order_relaxed);
      while (cur < want &&
             !header->high_water_slot.compare_exchange_weak(
               cur, want, std::memory_order_relaxed, std::memory_order_relaxed)) {}
      header->generation.fetch_add(1, std::memory_order_acq_rel);
      // Strictly after the bump — see ring_doorbells. Doorbell registration
      // itself rings nobody: the slot is invisible to queries, no consumer
      // exists for the wake, and ringing here would let K graph-waiting
      // processes registering at launch produce a K^2/2 datagram mini-storm.
      if (entry.type != ENTRY_DOORBELL) {
        ring_doorbells(header);
      }
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

// Best-effort tear-down of a slot we already claimed (state RESERVED). Zero out
// the payload so the next reuse starts from a clean state, handing the path back
// for the caller to unlink once the slot is released. Wrapped in a seqlock so any
// reader still in the middle of a snapshot retries. The caller stores EMPTY only
// afterwards: freeing the slot first would let a concurrent add claim it and have
// its fresh entry wiped here.
static void teardown_slot(
  RegistryEntrySlot * slot,
  char (& path_copy)[sizeof(RegistryEntrySlot::socket_path)])
{
  slot->seq.fetch_add(1, std::memory_order_acq_rel);
  std::atomic_thread_fence(std::memory_order_release);  // as in write_slot_payload

  // Plain read: this thread owns the slot (RESERVED), so no writer can race it.
  std::memcpy(path_copy, slot->socket_path, sizeof(path_copy));

  shm_store_relaxed(&slot->pid, nullptr, PAYLOAD_SIZE);

  slot->seq.fetch_add(1, std::memory_order_acq_rel);
}

static void unlink_torn_down_path(const char * path_copy)
{
  // Unlink outside the seqlock — filesystem op, doesn't need to be observable
  // by other readers atomically. A TRANSIENT_LOCAL publisher slot names its
  // latched-cache shm segment here (not a socket file), so a stale-PID reap
  // reclaims the dead publisher's cache promptly instead of leaking it until
  // the next rmw_init sweep.
  if (path_copy[0] != '\0') {
    if (std::strncmp(path_copy, "/ros2_uds_tl_", 13) == 0) {
      shm_unlink(path_copy);
    } else {
      unlink(path_copy);
    }
  }
}

void registry_remove(RegistryHeader * header, int32_t index)
{
  if (index < 0 || static_cast<uint32_t>(index) >= header->max_entries) {
    return;
  }
  auto * slot = &registry_slots(header)[index];

  uint8_t st = slot->state.load(std::memory_order_acquire);
  if (st == ENTRY_EMPTY || st == ENTRY_RESERVED) {
    return;  // already free, or another thread is mid-add/mid-teardown
  }
  // CAS state -> RESERVED. Winner owns the teardown; the slot stays unclaimable
  // until the payload is zeroed.
  if (!slot->state.compare_exchange_strong(
      st, static_cast<uint8_t>(ENTRY_RESERVED),
      std::memory_order_acq_rel, std::memory_order_relaxed))
  {
    return;  // someone else removed it
  }
  char path[sizeof(RegistryEntrySlot::socket_path)];
  teardown_slot(slot, path);
  slot->state.store(static_cast<uint8_t>(ENTRY_EMPTY), std::memory_order_release);
  header->generation.fetch_add(1, std::memory_order_acq_rel);
  unlink_torn_down_path(path);
  if (st != ENTRY_DOORBELL) {  // doorbell slots have no wake consumers
    ring_doorbells(header);    // strictly after the bump — see ring_doorbells
  }
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
    case ENTRY_DOORBELL: return "doorbell";
    default: return "?";
  }
}

// Ring every registered doorbell (one octet, best-effort) so processes blocked
// in rmw_wait re-check the registry. Called strictly AFTER a generation bump:
// paired with rmw_wait draining its doorbell strictly BEFORE reading the
// generation, every mutation either lands in the pre-block generation read or
// leaves a queued datagram on a level-triggered fd — no lost wakeup. EAGAIN
// means the peer already has a wakeup queued; other send errors mean a dead
// peer whose slot will be reclaimed. Scans slots directly (not registry_query,
// which calls back into cleanup and would recurse).
static void ring_doorbells(RegistryHeader * header)
{
  // One ring socket per mutating thread, closed at thread exit. AF_UNIX
  // datagrams stay charged to the SENDER's buffer until the receiver consumes
  // them, so a peer that is slow to drain could exhaust this fd's budget and
  // make sendto fail for every OTHER peer too; the recreate-on-EAGAIN below
  // resets that budget. On a fresh fd, EAGAIN can only mean the destination's
  // own queue is full — a wakeup is already pending there, so the drop is safe.
  struct RingFd
  {
    int fd = -1;
    ~RingFd() {if (fd >= 0) {close(fd);}}
  };
  static thread_local RingFd ring;
  if (ring.fd < 0) {
    ring.fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (ring.fd < 0) {
      return;
    }
  }
  auto * slots = registry_slots(header);
  const uint32_t hi = header->high_water_slot.load(std::memory_order_acquire);
  for (uint32_t i = 0; i < hi; ++i) {
    if (slots[i].state.load(std::memory_order_acquire) !=
      static_cast<uint8_t>(ENTRY_DOORBELL))
    {
      continue;
    }
    // Seqlock snapshot of the socket path, re-validating the type INSIDE the
    // seq window: without it, a remove + re-claim of this slot by a data
    // endpoint between the fast-skip above and the copy could land the wake
    // octet on a real data socket. A rewrite after this re-check still bumps
    // seq, so the s1 comparison below rejects the torn copy.
    char path[sizeof(slots[i].socket_path)];
    const uint32_t s1 = slots[i].seq.load(std::memory_order_acquire);
    if (s1 & 1) {
      continue;
    }
    if (slots[i].state.load(std::memory_order_acquire) !=
      static_cast<uint8_t>(ENTRY_DOORBELL))
    {
      continue;
    }
    shm_load_cstr_relaxed(path, slots[i].socket_path, sizeof(path));
    std::atomic_thread_fence(std::memory_order_acquire);  // as in snapshot_slot
    if (slots[i].seq.load(std::memory_order_acquire) != s1 || path[0] == '\0') {
      continue;
    }
    struct sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    // memcpy of the measured length: addr is zeroed, so termination is free
    // and -Wstringop-truncation stays quiet (path may fill all 108 bytes).
    std::memcpy(addr.sun_path, path, strnlen(path, sizeof(addr.sun_path) - 1));
    const uint8_t octet = 1;
    ssize_t sent = sendto(
      ring.fd, &octet, 1, MSG_DONTWAIT | MSG_NOSIGNAL,
      reinterpret_cast<const struct sockaddr *>(&addr), sizeof(addr));
    if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // Sender-side budget exhausted (some peer is slow to drain): reset the
      // budget and retry once, so one undrained doorbell cannot mute rings to
      // healthy peers. EAGAIN again on the fresh fd is the benign case.
      close(ring.fd);
      ring.fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
      if (ring.fd < 0) {
        return;
      }
      (void)sendto(
        ring.fd, &octet, 1, MSG_DONTWAIT | MSG_NOSIGNAL,
        reinterpret_cast<const struct sockaddr *>(&addr), sizeof(addr));
    }
  }
}

void registry_cleanup_stale(RegistryHeader * header)
{
  auto * slots = registry_slots(header);
  bool reclaimed = false;
  // Scan only [0, high_water): over-scan is safe, under-scan is impossible.
  uint32_t hw = header->high_water_slot.load(std::memory_order_acquire);
  uint32_t max = header->max_entries;
  if (hw < max) { max = hw; }

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
    // we observed in the snapshot. RESERVED (not EMPTY) keeps adders out until
    // teardown_slot has zeroed it; see teardown_slot.
    uint8_t expected = snap.state.load(std::memory_order_relaxed);
    if (!slots[i].state.compare_exchange_strong(
        expected, static_cast<uint8_t>(ENTRY_RESERVED),
        std::memory_order_acq_rel, std::memory_order_relaxed))
    {
      continue;  // raced with another remover / writer
    }

    char path[sizeof(RegistryEntrySlot::socket_path)];
    teardown_slot(&slots[i], path);
    slots[i].state.store(static_cast<uint8_t>(ENTRY_EMPTY), std::memory_order_release);
    header->generation.fetch_add(1, std::memory_order_acq_rel);
    unlink_torn_down_path(path);

    // Surface the reclaim so a crashed-node incident leaves a breadcrumb
    // in the ROS log. WARN level — this is an ungraceful exit, not routine.
    // Logged from the local snapshot after the slot is released, so
    // formatting it never holds the slot unclaimable.
    RMW_UDS_LOG_WARN(
      "reclaimed stale registry slot %u: %s pid=%d node=%s%s topic=%s — "
      "owning process is gone (ungraceful exit)",
      i, entry_type_name(expected),
      static_cast<int>(snap.pid),
      snap.node_namespace[0] ? snap.node_namespace : "",
      snap.node_name,
      snap.topic_name[0] ? snap.topic_name : "(none)");
    if (expected != static_cast<uint8_t>(ENTRY_DOORBELL)) {
      reclaimed = true;  // doorbell-only reclaims ring nobody
    }
  }
  if (reclaimed) {
    ring_doorbells(header);  // strictly after the bump(s) — see ring_doorbells
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
  // Scan only [0, high_water): over-scan is safe, under-scan is impossible.
  uint32_t hw = header->high_water_slot.load(std::memory_order_acquire);
  uint32_t max = header->max_entries;
  if (hw < max) { max = hw; }

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
