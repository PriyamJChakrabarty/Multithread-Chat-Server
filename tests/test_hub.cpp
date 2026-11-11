#include <gtest/gtest.h>

#include <algorithm>
#include <mutex>
#include <string>
#include <vector>

#include "chat/chat_hub.hpp"

using namespace chat;

namespace {
struct Inbox {
  std::mutex mu;
  std::vector<std::string> lines;
  SendFn sender() {
    return [this](std::string_view l) {
      std::lock_guard<std::mutex> lk(mu);
      lines.emplace_back(l);
    };
  }
  bool has(const std::string& sub) {
    std::lock_guard<std::mutex> lk(mu);
    return std::any_of(lines.begin(), lines.end(),
                       [&](const std::string& l) { return l.find(sub) != std::string::npos; });
  }
  std::string last() {
    std::lock_guard<std::mutex> lk(mu);
    return lines.empty() ? "" : lines.back();
  }
};
}  // namespace

TEST(Hub, WelcomeAndGuestNick) {
  ChatHub hub;
  Inbox a;
  auto s = hub.add_session(a.sender());
  EXPECT_TRUE(a.has("welcome guest1"));
  EXPECT_EQ(hub.session_count(), 1u);
  hub.remove_session(s);
  EXPECT_EQ(hub.session_count(), 0u);
}

TEST(Hub, BroadcastReachesRoomIncludingSender) {
  ChatHub hub;
  Inbox a, b;
  auto sa = hub.add_session(a.sender());
  auto sb = hub.add_session(b.sender());
  hub.handle_line(sa, "/nick alice");
  hub.handle_line(sb, "/nick bob");
  hub.handle_line(sa, "hi all");
  EXPECT_EQ(b.last(), "[lobby] alice: hi all");
  EXPECT_EQ(a.last(), "[lobby] alice: hi all");
}

TEST(Hub, NickValidationAndUniqueness) {
  ChatHub hub;
  Inbox a, b;
  auto sa = hub.add_session(a.sender());
  auto sb = hub.add_session(b.sender());
  hub.handle_line(sa, "/nick alice");
  hub.handle_line(sb, "/nick alice");
  EXPECT_TRUE(b.has("taken"));
  hub.handle_line(sb, "/nick bad nick!");
  EXPECT_TRUE(b.has("must be"));
  hub.handle_line(sb, "/nick");
  EXPECT_TRUE(b.has("must be"));
  hub.handle_line(sa, "/nick carol");  // frees "alice"
  hub.handle_line(sb, "/nick alice");
  EXPECT_TRUE(a.has("guest2 is now known as alice"));
}

TEST(Hub, RoomsIsolateTraffic) {
  ChatHub hub;
  Inbox a, b, c;
  auto sa = hub.add_session(a.sender());
  auto sb = hub.add_session(b.sender());
  auto sc = hub.add_session(c.sender());
  hub.handle_line(sa, "/nick a");
  hub.handle_line(sb, "/nick b");
  hub.handle_line(sc, "/nick c");
  hub.handle_line(sa, "/join dev");
  hub.handle_line(sb, "/join dev");
  hub.handle_line(sa, "secret");
  EXPECT_TRUE(b.has("[dev] a: secret"));
  EXPECT_FALSE(c.has("secret"));
  EXPECT_EQ(hub.room_count(), 2u);  // lobby (c) + dev
  hub.handle_line(sc, "/who");
  EXPECT_TRUE(c.has("in #lobby (1): c"));
  hub.handle_line(sa, "/who");
  EXPECT_TRUE(a.has("in #dev (2): a b"));
}

TEST(Hub, EmptyRoomsAreRemoved) {
  ChatHub hub;
  Inbox a;
  auto sa = hub.add_session(a.sender());
  hub.handle_line(sa, "/join temp");
  EXPECT_EQ(hub.room_count(), 1u);  // lobby was emptied and dropped
  hub.remove_session(sa);
  EXPECT_EQ(hub.room_count(), 0u);
}

TEST(Hub, PrivateMessage) {
  ChatHub hub;
  Inbox a, b, c;
  auto sa = hub.add_session(a.sender());
  auto sb = hub.add_session(b.sender());
  auto sc = hub.add_session(c.sender());
  hub.handle_line(sa, "/nick a");
  hub.handle_line(sb, "/nick b");
  hub.handle_line(sc, "/nick c");
  hub.handle_line(sa, "/msg b psst");
  EXPECT_EQ(b.last(), "[pm from a] psst");
  EXPECT_EQ(a.last(), "[pm to b] psst");
  EXPECT_FALSE(c.has("psst"));
  hub.handle_line(sa, "/msg nobody hi");
  EXPECT_TRUE(a.has("no such user"));
  hub.handle_line(sa, "/msg b");
  EXPECT_TRUE(a.has("usage"));
}

TEST(Hub, QuitAndLeaveAnnouncement) {
  ChatHub hub;
  Inbox a, b;
  auto sa = hub.add_session(a.sender());
  auto sb = hub.add_session(b.sender());
  hub.handle_line(sa, "/nick a");
  EXPECT_EQ(hub.handle_line(sa, "/quit"), ChatHub::Result::Quit);
  hub.remove_session(sa);
  EXPECT_TRUE(b.has("a left lobby"));
  // Late lines from a removed session are ignored.
  EXPECT_EQ(hub.handle_line(sa, "ghost"), ChatHub::Result::Quit);
  EXPECT_FALSE(b.has("ghost"));
  hub.remove_session(sa);  // idempotent
}

TEST(Hub, UnknownCommandAndBlankLines) {
  ChatHub hub;
  Inbox a;
  auto sa = hub.add_session(a.sender());
  hub.handle_line(sa, "/bogus");
  EXPECT_TRUE(a.has("unknown command"));
  auto before = a.lines.size();
  hub.handle_line(sa, "   \r\n");
  EXPECT_EQ(a.lines.size(), before);
}

TEST(Hub, UdpDeliveryByNick) {
  ChatHub hub;
  Inbox a, b;
  auto sa = hub.add_session(a.sender());
  auto sb = hub.add_session(b.sender());
  hub.handle_line(sa, "/nick a");
  EXPECT_TRUE(hub.deliver_udp("a", "ping!"));
  EXPECT_TRUE(b.has("(udp) a: ping!"));
  EXPECT_FALSE(hub.deliver_udp("ghost", "x"));
}
