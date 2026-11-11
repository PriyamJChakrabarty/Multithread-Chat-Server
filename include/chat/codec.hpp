#pragma once
// Wire formats. Pure functions/classes with no socket dependency so they can be
// unit tested in isolation. See docs/protocol.md.
#include <cstddef>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <string_view>

namespace chat {

// ---------------------------------------------------------------- TCP framing
// Frame = 4-byte big-endian payload length + payload.
constexpr std::size_t kFrameHeaderSize = 4;

std::string encode_frame(std::string_view payload);

// Incremental decoder: feed arbitrary byte chunks (partial frames, several
// frames glued together) and pull complete payloads out in order.
class FrameDecoder {
 public:
  explicit FrameDecoder(std::size_t max_payload) : max_payload_(max_payload) {}

  void feed(const char* data, std::size_t len) { buf_.append(data, len); }
  // Next complete payload, or nullopt if more bytes are needed or the stream
  // is corrupt (check failed()).
  std::optional<std::string> next();
  // True once a header announced a payload larger than max_payload. The stream
  // cannot be resynchronised, so the connection must be closed.
  bool failed() const { return failed_; }
  std::size_t buffered() const { return buf_.size() - pos_; }

 private:
  std::string buf_;
  std::size_t pos_ = 0;
  std::size_t max_payload_;
  bool failed_ = false;
};

// ------------------------------------------------------------ UDP datagrams
// 16-byte RTP-like header, big endian:
//   u16 magic | u8 type | u8 flags | u32 seq | u64 timestamp_us
constexpr std::size_t kDatagramHeaderSize = 16;
constexpr std::uint16_t kDatagramMagic = 0xC47A;

enum class DatagramType : std::uint8_t { Ping = 1, Pong = 2, Message = 3 };

struct DatagramHeader {
  DatagramType type = DatagramType::Ping;
  std::uint8_t flags = 0;
  std::uint32_t seq = 0;
  std::uint64_t timestamp_us = 0;
};

struct Datagram {
  DatagramHeader header;
  std::string_view payload;  // points into the buffer passed to decode_datagram
};

std::string encode_datagram(const DatagramHeader& h, std::string_view payload = {});
std::optional<Datagram> decode_datagram(const char* data, std::size_t len);

// Detects loss, reordering and duplication from sequence numbers (wrap-safe).
class SequenceTracker {
 public:
  struct Stats {
    std::uint64_t received = 0;
    std::uint64_t lost = 0;        // gaps never filled (so far)
    std::uint64_t reordered = 0;   // arrived late but did fill a gap
    std::uint64_t duplicates = 0;
  };

  void on_packet(std::uint32_t seq);
  Stats stats() const;

 private:
  static constexpr std::size_t kMaxTrackedGaps = 1024;
  bool started_ = false;
  std::uint32_t highest_ = 0;
  std::set<std::uint32_t> missing_;  // seqs skipped over and not yet seen
  std::uint64_t expired_ = 0;        // gaps evicted from missing_ => counted lost
  Stats s_;
};

}  // namespace chat
