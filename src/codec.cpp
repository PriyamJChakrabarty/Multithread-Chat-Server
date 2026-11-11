#include "chat/codec.hpp"

namespace chat {

namespace {
void put_be(std::string& out, std::uint64_t v, int bytes) {
  for (int i = bytes - 1; i >= 0; --i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
std::uint64_t get_be(const char* p, int bytes) {
  std::uint64_t v = 0;
  for (int i = 0; i < bytes; ++i) v = (v << 8) | static_cast<unsigned char>(p[i]);
  return v;
}
}  // namespace

std::string encode_frame(std::string_view payload) {
  std::string out;
  out.reserve(kFrameHeaderSize + payload.size());
  put_be(out, payload.size(), 4);
  out.append(payload);
  return out;
}

std::optional<std::string> FrameDecoder::next() {
  if (failed_) return std::nullopt;
  const std::size_t avail = buf_.size() - pos_;
  if (avail < kFrameHeaderSize) return std::nullopt;

  const std::uint64_t len = get_be(buf_.data() + pos_, 4);
  if (len > max_payload_) {
    failed_ = true;
    return std::nullopt;
  }
  if (avail < kFrameHeaderSize + len) return std::nullopt;

  std::string payload = buf_.substr(pos_ + kFrameHeaderSize, static_cast<std::size_t>(len));
  pos_ += kFrameHeaderSize + static_cast<std::size_t>(len);
  // Compact lazily so repeated small frames don't memmove every time.
  if (pos_ == buf_.size()) {
    buf_.clear();
    pos_ = 0;
  } else if (pos_ > 4096 && pos_ * 2 > buf_.size()) {
    buf_.erase(0, pos_);
    pos_ = 0;
  }
  return payload;
}

// ------------------------------------------------------------------- UDP
std::string encode_datagram(const DatagramHeader& h, std::string_view payload) {
  std::string out;
  out.reserve(kDatagramHeaderSize + payload.size());
  put_be(out, kDatagramMagic, 2);
  out.push_back(static_cast<char>(h.type));
  out.push_back(static_cast<char>(h.flags));
  put_be(out, h.seq, 4);
  put_be(out, h.timestamp_us, 8);
  out.append(payload);
  return out;
}

std::optional<Datagram> decode_datagram(const char* data, std::size_t len) {
  if (len < kDatagramHeaderSize) return std::nullopt;
  if (get_be(data, 2) != kDatagramMagic) return std::nullopt;
  auto type = static_cast<std::uint8_t>(data[2]);
  if (type < 1 || type > 3) return std::nullopt;
  Datagram d;
  d.header.type = static_cast<DatagramType>(type);
  d.header.flags = static_cast<std::uint8_t>(data[3]);
  d.header.seq = static_cast<std::uint32_t>(get_be(data + 4, 4));
  d.header.timestamp_us = get_be(data + 8, 8);
  d.payload = std::string_view(data + kDatagramHeaderSize, len - kDatagramHeaderSize);
  return d;
}

void SequenceTracker::on_packet(std::uint32_t seq) {
  ++s_.received;
  if (!started_) {
    started_ = true;
    highest_ = seq;
    return;
  }
  // Signed distance handles 32-bit wraparound.
  const auto diff = static_cast<std::int32_t>(seq - highest_);
  if (diff > 0) {
    const std::uint32_t gap = static_cast<std::uint32_t>(diff) - 1;
    // Remember at most kMaxTrackedGaps outstanding holes; older ones are
    // written off as lost.
    const std::uint32_t tracked = gap < kMaxTrackedGaps ? gap : kMaxTrackedGaps;
    expired_ += gap - tracked;  // oldest holes of a huge gap: written off at once
    for (std::uint32_t i = 1; i <= tracked; ++i) missing_.insert(seq - i);
    while (missing_.size() > kMaxTrackedGaps) {
      missing_.erase(missing_.begin());
      ++expired_;
    }
    highest_ = seq;
  } else {
    if (missing_.erase(seq) > 0) ++s_.reordered;
    else ++s_.duplicates;
  }
}

SequenceTracker::Stats SequenceTracker::stats() const {
  Stats s = s_;
  s.lost = missing_.size() + expired_;
  return s;
}

}  // namespace chat
