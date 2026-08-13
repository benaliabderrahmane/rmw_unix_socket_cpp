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

#ifndef RMW_UNIX_SOCKET_CPP__SHM_TRANSPORT_HPP_
#define RMW_UNIX_SOCKET_CPP__SHM_TRANSPORT_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <sys/types.h>

namespace rmw_uds
{

// Defined in types.hpp (which includes this header); only referenced here.
struct WireHeader;

// Shared-memory payload path for large messages — topic payloads, service
// requests, and service responses alike.
//
// A payload at or above SHM_PAYLOAD_THRESHOLD does not travel inside the UDS
// datagram. The sender (publisher, client, or service) writes the serialized
// bytes once into its own /dev/shm ring and fans out a fixed-size
// ShmPayloadDescriptor instead (the WireHeader msg_type gains the
// SHM_PAYLOAD_FLAG bit). Receivers map the ring on first use and copy the
// payload out under a per-record seqlock — the same odd/even protocol the
// discovery registry uses — so a sender that laps the ring is detected as a
// clean drop, never delivered corrupt. This removes the two kernel copies
// (and the net.core.wmem_max datagram cap) from the large-message path; the
// datagram itself stays tiny.

// Payloads >= this many bytes are staged in shared memory. Below it the
// inline datagram is both simpler and faster (no page faults, no seqlock).
static constexpr size_t SHM_PAYLOAD_THRESHOLD = 64 * 1024;

// Smallest ring a publisher (or service/client) creates. A ring is always
// sized to hold at least four records of the largest payload staged so far
// (see create_segment: max(this, 4*record)), so a subscriber that is one
// wait-wakeup behind still finds the record intact regardless of this floor;
// this only sets the minimum for small large-payloads. posix_fallocate commits
// the whole ring's RAM up front, and every large-message sender pays it, so the
// floor is kept modest: 1 MiB still holds 15 records at the 64 KiB threshold
// (each record is align_up(8 + 65536, 64) = 65600 bytes).
static constexpr size_t SHM_RING_MIN_BYTES = 1 * 1024 * 1024;

static constexpr uint32_t SHM_RING_MAGIC = 0x52534455;  // "UDSR"
static constexpr uint32_t SHM_RING_VERSION = 1;

// WireHeader::msg_type high bit: the datagram payload is a
// ShmPayloadDescriptor, not CDR bytes. The low bits keep their meaning
// (0 = topic message, 1 = request, 2 = response — the flag combines with
// all three; receivers mask it off before dispatching on the type).
static constexpr uint8_t SHM_PAYLOAD_FLAG = 0x80;

// Datagram payload when SHM_PAYLOAD_FLAG is set. Blitted onto the wire, so
// the packed layout is a cross-process contract like WireHeader.
struct __attribute__((packed)) ShmPayloadDescriptor
{
  uint64_t segment_id;    // ring segment generation (part of the shm name)
  uint64_t offset;        // byte offset of the record inside the ring area
  uint32_t seq;           // expected (even) seqlock value of the record
  uint32_t payload_size;  // CDR payload length
  int32_t owner_pid;      // with owner_id: names the publisher's segment
  uint32_t owner_id;      // per-process unique ring id
};

static_assert(sizeof(ShmPayloadDescriptor) == 32, "descriptor wire layout changed");
static_assert(offsetof(ShmPayloadDescriptor, segment_id) == 0, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, offset) == 8, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, seq) == 16, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, payload_size) == 20, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, owner_pid) == 24, "descriptor layout changed");
static_assert(offsetof(ShmPayloadDescriptor, owner_id) == 28, "descriptor layout changed");

// First bytes of every ring segment. Readers validate it before trusting any
// offset out of a descriptor.
struct ShmRingHeader
{
  uint32_t magic;
  uint32_t version;
  uint64_t ring_bytes;  // capacity of the record area that follows
};

static_assert(sizeof(ShmRingHeader) == 16, "ring header shm layout changed");

// One record in the ring: header immediately followed by the payload bytes.
// Records are 64-byte aligned. seq carries an absolute value from the
// writer's monotonic counter (2*n while record n is stable, 2*n-1 while it
// is being written), so a reader comparing against the descriptor's expected
// value detects both "still being written" and "already overwritten".
struct ShmRecordHeader
{
  std::atomic<uint32_t> seq;
  uint32_t payload_len;
};

static_assert(sizeof(ShmRecordHeader) == 8, "record header shm layout changed");

// Publisher-side ring state (process-local; the mapped segment is the shared
// part). Guarded by UdsPublisher::shm_mutex — only the owning process writes.
struct ShmRingWriter
{
  int fd = -1;
  uint8_t * base = nullptr;   // mapping of ShmRingHeader + record area
  size_t ring_bytes = 0;      // record-area capacity
  uint64_t segment_id = 0;    // bumped when the ring is recreated bigger
  uint64_t next_offset = 0;   // ring cursor into the record area
  uint32_t next_index = 1;    // monotonic record counter (drives seq values)
  int32_t owner_pid = -1;
  uint32_t owner_id = 0;      // assigned from a process-global counter
  std::string shm_name;
  size_t reserved_cap = 0;    // capacity of the reserved-but-uncommitted record
};

// Subscriber-side cache of mapped publisher rings, keyed by the segment's
// (owner_pid, owner_id, segment_id) — a 16-byte POD, so the receive hot path
// does an integer-keyed lookup instead of rebuilding and hashing the segment
// path string per message. domain_id is constant per cache instance, so those
// three descriptor fields uniquely identify a segment. One entry per (ring,
// generation) seen; internally locked because rmw_wait and rmw_take may drain
// the same subscription concurrently.
struct ShmReaderCache
{
  struct Key
  {
    int32_t owner_pid;
    uint32_t owner_id;
    uint64_t segment_id;
    bool operator==(const Key & o) const
    {
      return owner_pid == o.owner_pid && owner_id == o.owner_id &&
             segment_id == o.segment_id;
    }
  };
  struct KeyHash
  {
    size_t operator()(const Key & k) const
    {
      size_t h = std::hash<uint64_t>()(k.segment_id);
      h ^= std::hash<uint32_t>()(k.owner_id) + 0x9e3779b9 + (h << 6) + (h >> 2);
      h ^= std::hash<int32_t>()(k.owner_pid) + 0x9e3779b9 + (h << 6) + (h >> 2);
      return h;
    }
  };
  struct Mapping
  {
    uint8_t * base = nullptr;
    size_t size = 0;
    // Ring capacity as validated at map time. Bounds checks use this
    // snapshot, never the live (owner-writable) header in the mapping.
    size_t ring_bytes = 0;
    // Segment path, built once when mapped; reused by the liveness-probe sweep
    // so a hit never rebuilds it.
    std::string shm_name;
  };
  std::mutex mutex;
  std::unordered_map<Key, Mapping, KeyHash> mappings;
};

// A dedicated shm segment holding exactly one immutable payload record, for
// TRANSIENT_LOCAL cached messages. Unlike the cycling ShmRingWriter, this
// segment is written once and never reused, so a late-joining subscriber can
// map and replay it at any time until the cache entry is evicted. Owns its
// mapping and shm name; the destructor unmaps and unlinks. Non-copyable, and
// (with a user-declared destructor and no move ops) never moved directly — it
// is always held through a std::unique_ptr in the owning CachedMessage, and
// that unique_ptr is what lets the cache entry move through the replay deque
// without double-freeing or leaking the segment.
struct DurableShmSegment
{
  uint8_t * base = nullptr;
  size_t map_size = 0;
  std::string shm_name;

  DurableShmSegment() = default;
  ~DurableShmSegment();
  DurableShmSegment(const DurableShmSegment &) = delete;
  DurableShmSegment & operator=(const DurableShmSegment &) = delete;
};

// Stage a payload into the publisher's ring, creating the ring lazily and
// recreating it larger (new segment_id, old segment unlinked) when the
// payload outgrows it. Fills desc_out on success. Returns false when shared
// memory is unavailable (shm_open/ftruncate/mmap failure) — the caller falls
// back to sending the payload inline. Caller holds UdsPublisher::shm_mutex.
// Implemented as reserve + memcpy + commit (below).
bool shm_stage_payload(
  ShmRingWriter & ring,
  size_t domain_id,
  const uint8_t * payload,
  size_t payload_size,
  ShmPayloadDescriptor & desc_out);

// Two-phase staging, so a serializer can write CDR bytes directly into the
// ring record instead of through an intermediate heap buffer. The caller
// holds the entity's shm_mutex across the whole reserve..commit/abort span.
//
//   uint8_t * dst = shm_stage_reserve(ring, domain, max_size);
//   ...write up to max_size bytes into dst...
//   shm_stage_commit(ring, actual_size, desc);   // or shm_stage_abort(ring)
//
// reserve marks the record's seqlock odd and returns its payload area (null
// when shared memory is unavailable); the cursor does not advance. commit
// publishes the record (seqlock even, cursor advanced by the ACTUAL size, so
// a size-walk overestimate wastes nothing). abort leaves the seqlock odd and
// the cursor unchanged: the half-written record is never observable — any
// stale descriptor pointing at that offset fails its seqlock check — and the
// next reserve reuses the same slot.
uint8_t * shm_stage_reserve(
  ShmRingWriter & ring,
  size_t domain_id,
  size_t max_payload_size);

bool shm_stage_commit(
  ShmRingWriter & ring,
  size_t payload_size,
  ShmPayloadDescriptor & desc_out);

void shm_stage_abort(ShmRingWriter & ring);

// Stage a payload into a fresh, dedicated, immutable segment and fill desc_out.
// The returned segment stays valid (its record never overwritten) until the
// handle is destroyed, so a TRANSIENT_LOCAL publisher can cache the descriptor
// and replay it to late joiners. Returns nullptr when shared memory is
// unavailable — the caller falls back to caching the payload inline. The
// resulting descriptor is read by the same shm_fetch_payload path as ring
// payloads; readers cannot tell the two apart.
std::unique_ptr<DurableShmSegment> shm_stage_durable(
  size_t domain_id,
  const uint8_t * payload,
  size_t payload_size,
  ShmPayloadDescriptor & desc_out);

// Replace payload_io (a ShmPayloadDescriptor as received off the wire) with
// the payload bytes it describes, copied out under the record seqlock.
// Returns false when the descriptor is malformed, the segment cannot be
// mapped (publisher gone), or the record was overwritten (publisher lapped
// the ring) — all of which the caller treats as a dropped message.
bool shm_fetch_payload(
  ShmReaderCache & cache,
  size_t domain_id,
  std::vector<uint8_t> & payload_io);

// ---------------------------------------------------------------------------
// TRANSIENT_LOCAL latched cache: a per-publisher shm segment holding the last
// qos.depth latched samples, written at publish time and READ BY THE LATE
// JOINER ITSELF at rmw_create_subscription. The publisher process is never
// woken for replay — durability is pull-based, like discovery itself. The
// segment's name is published in the publisher's registry slot (socket_path,
// unused for publishers until now), so a subscriber locates it from the slot
// it already queried; the name carries a per-incarnation unique id, so a
// recycled PID can never alias a dead publisher's cache.
// ---------------------------------------------------------------------------

// Latched payloads at or below this many bytes are embedded in the slot;
// larger ones are staged once via shm_stage_durable and the slot carries the
// 32-byte descriptor (resolved by the puller through shm_fetch_payload).
// Small keeps the fixed slot stride — and with it the ring's RAM commit —
// modest for chatty latched topics like /rosout (depth 1000).
static constexpr size_t TL_EMBED_CAP = 1024;

// Hard byte ceiling for one publisher's latched ring (record area). A
// misconfigured depth cannot fallocate tens of MB: slots are clamped to
// whatever fits. Replay may then hold fewer than qos.depth samples — a
// stated, accepted limit (DESIGN, latched cache). 2 MiB admits the stock
// rosout profile (TRANSIENT_LOCAL, KEEP_LAST 1000: 1000 x 1088 B stride)
// without clamping; the file is sparse, so the ceiling costs nothing until
// slots are actually latched.
static constexpr size_t TL_RING_MAX_BYTES = 2 * 1024 * 1024;

static constexpr uint32_t TL_RING_MAGIC = 0x4C544455;  // "UDTL"
static constexpr uint32_t TL_RING_VERSION = 1;

// First bytes of a latched-cache segment. Written once at creation, before
// the publisher's registry slot exists; immutable afterwards, so readers
// need no synchronization to validate it. The creator's full 16-byte GID is
// embedded so a puller can verify the segment belongs to the slot it derived
// the name from (stale-segment defense in depth; the unique name is the
// primary defense).
struct TlRingHeader
{
  uint32_t magic;
  uint32_t version;
  uint8_t gid[16];
  uint32_t slots;       // slot count (qos.depth clamped by TL_RING_MAX_BYTES)
  uint32_t slot_bytes;  // stride of one slot
};

static_assert(sizeof(TlRingHeader) == 32, "TL ring header shm layout changed");

// One latched slot: ShmRecordHeader (seq + payload_len) followed by the
// sample's WireHeader (37 packed bytes) and the payload area (TL_EMBED_CAP
// bytes — inline CDR, or a 32-byte ShmPayloadDescriptor when the WireHeader
// carries SHM_PAYLOAD_FLAG). Slot i holds record n where n % slots == i; the
// record seq is the absolute 2n-1 (writing) / 2n (stable) protocol shared
// with the payload ring, so a reader detects both in-flight and lapped slots
// per record — a writer preempted or killed mid-write poisons exactly one
// slot, never the history.
static constexpr size_t TL_SLOT_BYTES_UNALIGNED =
  sizeof(ShmRecordHeader) + 37 /* sizeof(WireHeader) */ + TL_EMBED_CAP;

// Publisher-side latched-cache state (process-local; the mapped segment is
// the shared part). Guarded by UdsPublisher::cache_mutex; the sequence
// number is assigned under the same lock so slot commit order equals
// sequence order — the property that makes the puller's dedup watermark
// sound. durable_segs parallels the slots: it owns the DurableShmSegment a
// slot's descriptor points at, destroyed when that slot is overwritten.
struct TlRingWriter
{
  int fd = -1;
  uint8_t * base = nullptr;
  size_t map_size = 0;
  uint32_t slots = 0;
  uint32_t slot_bytes = 0;
  uint64_t next_index = 1;  // absolute record counter (drives per-slot seq)
  std::string shm_name;
  std::vector<std::unique_ptr<DurableShmSegment>> durable_segs;
};

// One sample pulled out of a latched cache.
struct TlPulledRecord
{
  int64_t sequence_number;
  uint8_t wire_header[37];      // verbatim WireHeader bytes
  std::vector<uint8_t> payload; // inline CDR or a descriptor (per msg_type)
};

// Create the latched-cache segment for a TRANSIENT_LOCAL publisher. Called
// BEFORE the publisher's registry_add, so a visible slot always names a
// mappable, validated segment. The file is sized sparsely (ftruncate);
// per-record ranges are committed by posix_fallocate at latch time, so an
// idle publisher's ring costs an inode and a page, not depth x slot_bytes of
// RAM. Returns false on failure — the publisher then runs latch-less (no
// replay) and its slot publishes an empty name.
bool tl_ring_create(
  TlRingWriter & ring,
  size_t domain_id,
  const uint8_t * gid16,
  size_t depth);

// Latch one sample under the per-record seqlock. Caller holds the
// publisher's cache_mutex and has already assigned hdr.sequence_number under
// it. Payloads above TL_EMBED_CAP must be pre-staged BY THE CALLER (outside
// the lock — staging is shm_open/fallocate/mmap, up to milliseconds) via
// shm_stage_durable; pass its descriptor and segment here and the slot then
// carries the 32-byte descriptor with SHM_PAYLOAD_FLAG. On success the ring
// owns staged_seg (evicted with the slot); the previously-latched segment of
// the overwritten slot is returned in evicted_out so the caller can destroy
// it (munmap + shm_unlink) after releasing the lock. Returns false — and
// leaves staged_seg destroyed, nothing latched, live sends unaffected — when
// the ring is absent or the slot's pages cannot be committed (ENOSPC).
bool tl_ring_latch(
  TlRingWriter & ring,
  const WireHeader & hdr,
  const uint8_t * payload,
  size_t payload_size,
  const ShmPayloadDescriptor * staged_desc,
  std::unique_ptr<DurableShmSegment> staged_seg,
  std::unique_ptr<DurableShmSegment> & evicted_out);

// Map, validate, and snapshot a publisher's latched cache. expected_gid16 is
// the slot GID the name came from; a mismatched or malformed segment is
// skipped silently (stale incarnation). Records are returned sorted by
// sequence number, at most max_records newest. max_seq_out is the highest
// sequence number observed among stable records — the subscriber's dedup
// watermark. Per-slot seqlock reads are bounded (skip, never spin), so a
// publisher killed mid-write cannot hang subscription creation.
// *overlapped_out is set true when a writer latched DURING the scan (any
// pulled slot's seq moved by the end): the scan is then not a point-in-time
// snapshot and a sequence gap in the result may hide a sample the scan
// missed — the caller must not extend its dedup watermark across such a gap
// (the missed sample's datagram is in flight and must not be dropped).
bool tl_ring_pull(
  const std::string & shm_name,
  const uint8_t * expected_gid16,
  size_t max_records,
  std::vector<TlPulledRecord> & records_out,
  int64_t & max_seq_out,
  bool * overlapped_out);

// Unmap + unlink the latched cache (publisher destruction / failed create).
void tl_ring_close(TlRingWriter & ring);

// Unmap + unlink the publisher's current segment (publisher destruction).
void shm_writer_close(ShmRingWriter & ring);

// Unmap every cached segment (subscription destruction).
void shm_reader_close(ShmReaderCache & cache);

// Scan /dev/shm for ring segments of this domain whose owner PID (encoded in
// the name) is no longer alive in our PID namespace, and unlink them. Same
// contract and reasoning as cleanup_orphan_socket_files.
void shm_cleanup_orphan_segments(size_t domain_id);

}  // namespace rmw_uds

#endif  // RMW_UNIX_SOCKET_CPP__SHM_TRANSPORT_HPP_
