#include <gtest/gtest.h>

#include "chat/codec.hpp"

using namespace chat;

TEST(Frame, RoundTrip) {
  FrameDecoder d(1024);
  std::string f = encode_frame("hello");
  ASSERT_EQ(f.size(), 9u);
  EXPECT_EQ(f.substr(0, 4), std::string("\0\0\0\5", 4));  // big endian length
  d.feed(f.data(), f.size());
  EXPECT_EQ(d.next(), "hello");
  EXPECT_EQ(d.next(), std::nullopt);
  EXPECT_EQ(d.buffered(), 0u);
}

TEST(Frame, ByteAtATime) {  // message split across many reads
  FrameDecoder d(1024);
  std::string f = encode_frame("split me up");
  for (std::size_t i = 0; i + 1 < f.size(); ++i) {
    d.feed(&f[i], 1);
    EXPECT_EQ(d.next(), std::nullopt) << "complete too early at byte " << i;
  }
  d.feed(&f.back(), 1);
  EXPECT_EQ(d.next(), "split me up");
}

TEST(Frame, ManyFramesInOneRead) {  // messages packed together
  FrameDecoder d(1024);
  std::string blob;
  for (int i = 0; i < 100; ++i) blob += encode_frame("msg" + std::to_string(i));
  d.feed(blob.data(), blob.size());
  for (int i = 0; i < 100; ++i) EXPECT_EQ(d.next(), "msg" + std::to_string(i));
  EXPECT_EQ(d.next(), std::nullopt);
}

TEST(Frame, FrameStraddlingChunks) {
  FrameDecoder d(1024);
  std::string blob = encode_frame("first") + encode_frame("second");
  d.feed(blob.data(), 7);  // header + 3 bytes of "first"
  EXPECT_EQ(d.next(), std::nullopt);
  d.feed(blob.data() + 7, blob.size() - 7);
  EXPECT_EQ(d.next(), "first");
  EXPECT_EQ(d.next(), "second");
}

TEST(Frame, EmptyPayload) {
  FrameDecoder d(16);
  std::string f = encode_frame("");
  d.feed(f.data(), f.size());
  EXPECT_EQ(d.next(), "");
}

TEST(Frame, OversizeFails) {
  FrameDecoder d(8);
  std::string f = encode_frame("123456789");  // 9 > 8
  d.feed(f.data(), f.size());
  EXPECT_EQ(d.next(), std::nullopt);
  EXPECT_TRUE(d.failed());
  d.feed(f.data(), f.size());
  EXPECT_EQ(d.next(), std::nullopt);  // stays failed
}

TEST(Frame, HugeLengthHeaderDoesNotAllocate) {
  FrameDecoder d(1024);
  const char hdr[4] = {char(0xFF), char(0xFF), char(0xFF), char(0xFF)};
  d.feed(hdr, 4);
  EXPECT_EQ(d.next(), std::nullopt);
  EXPECT_TRUE(d.failed());
}

TEST(Frame, ExactlyMaxIsAccepted) {
  FrameDecoder d(5);
  std::string f = encode_frame("12345");
  d.feed(f.data(), f.size());
  EXPECT_EQ(d.next(), "12345");
  EXPECT_FALSE(d.failed());
}

TEST(Datagram, RoundTrip) {
  DatagramHeader h{DatagramType::Message, 7, 0xDEADBEEF, 0x0123456789ABCDEFull};
  std::string wire = encode_datagram(h, "payload");
  ASSERT_EQ(wire.size(), kDatagramHeaderSize + 7);
  auto d = decode_datagram(wire.data(), wire.size());
  ASSERT_TRUE(d);
  EXPECT_EQ(d->header.type, DatagramType::Message);
  EXPECT_EQ(d->header.flags, 7);
  EXPECT_EQ(d->header.seq, 0xDEADBEEFu);
  EXPECT_EQ(d->header.timestamp_us, 0x0123456789ABCDEFull);
  EXPECT_EQ(d->payload, "payload");
}

TEST(Datagram, RejectsGarbage) {
  std::string wire = encode_datagram({DatagramType::Ping, 0, 1, 2});
  EXPECT_FALSE(decode_datagram(wire.data(), 15));  // truncated header
  std::string bad = wire;
  bad[0] = 0;  // wrong magic
  EXPECT_FALSE(decode_datagram(bad.data(), bad.size()));
  bad = wire;
  bad[2] = 9;  // unknown type
  EXPECT_FALSE(decode_datagram(bad.data(), bad.size()));
  EXPECT_TRUE(decode_datagram(wire.data(), wire.size()));
}

TEST(SequenceTracker, InOrder) {
  SequenceTracker t;
  for (std::uint32_t i = 0; i < 10; ++i) t.on_packet(i);
  auto s = t.stats();
  EXPECT_EQ(s.received, 10u);
  EXPECT_EQ(s.lost, 0u);
  EXPECT_EQ(s.reordered, 0u);
  EXPECT_EQ(s.duplicates, 0u);
}

TEST(SequenceTracker, DetectsLoss) {
  SequenceTracker t;
  for (std::uint32_t s : {0u, 1u, 2u, 6u, 7u}) t.on_packet(s);  // 3,4,5 missing
  EXPECT_EQ(t.stats().lost, 3u);
}

TEST(SequenceTracker, ReorderedPacketIsNotLost) {
  SequenceTracker t;
  for (std::uint32_t s : {0u, 1u, 3u, 2u, 4u}) t.on_packet(s);
  auto s = t.stats();
  EXPECT_EQ(s.lost, 0u);
  EXPECT_EQ(s.reordered, 1u);
}

TEST(SequenceTracker, Duplicates) {
  SequenceTracker t;
  for (std::uint32_t s : {0u, 1u, 1u, 2u, 0u}) t.on_packet(s);
  EXPECT_EQ(t.stats().duplicates, 2u);
}

TEST(SequenceTracker, WrapAround) {
  SequenceTracker t;
  for (std::uint32_t s : {0xFFFFFFFEu, 0xFFFFFFFFu, 0u, 1u}) t.on_packet(s);
  EXPECT_EQ(t.stats().lost, 0u);
  t.on_packet(4);  // skipped 2,3 across the wrap
  EXPECT_EQ(t.stats().lost, 2u);
}

TEST(SequenceTracker, HugeGapIsBounded) {
  SequenceTracker t;
  t.on_packet(0);
  t.on_packet(2'000'000'000u);  // must not loop 2 billion times
  EXPECT_EQ(t.stats().lost, 1'999'999'999u);
}
