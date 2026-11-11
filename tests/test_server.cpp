// Integration tests: a real ChatServer on an ephemeral port, real sockets.
// Every test runs in both edge- and level-triggered mode.
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <sys/socket.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <vector>
#include <thread>

#include "chat/codec.hpp"
#include "chat/epoll_server.hpp"
#include "chat/logger.hpp"
#include "chat/net.hpp"
#include "chat/threaded_server.hpp"

using namespace chat;
using namespace std::chrono_literals;

namespace {

// Blocking test client speaking the framed protocol.
class TestClient {
 public:
  explicit TestClient(uint16_t port) : fd_(net::connect_tcp("127.0.0.1", port)), dec_(1 << 20) {}
  int fd() const { return fd_.get(); }

  bool send(const std::string& line) {
    std::string f = encode_frame(line);
    return net::write_all(fd_.get(), f.data(), f.size());
  }
  // Next frame within `timeout`, or nullopt on timeout/EOF.
  std::optional<std::string> recv(std::chrono::milliseconds timeout = 3000ms) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
      if (auto m = dec_.next()) return m;
      auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
          deadline - std::chrono::steady_clock::now());
      pollfd p{fd_.get(), POLLIN, 0};  // always poll at least once (timeout 0 = non-blocking peek)
      if (::poll(&p, 1, left.count() > 0 ? static_cast<int>(left.count()) : 0) <= 0)
        return std::nullopt;
      char buf[8192];
      ssize_t n = ::recv(fd_.get(), buf, sizeof buf, 0);
      if (n <= 0) return std::nullopt;
      dec_.feed(buf, static_cast<std::size_t>(n));
    }
  }
  // Read until a line containing `sub` shows up.
  bool expect(const std::string& sub, std::chrono::milliseconds timeout = 3000ms) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      auto m = recv(std::chrono::duration_cast<std::chrono::milliseconds>(
          deadline - std::chrono::steady_clock::now()));
      if (!m) return false;
      if (m->find(sub) != std::string::npos) return true;
    }
    return false;
  }
  // True if the server closed the connection (EOF/RST) within the timeout.
  bool closed_by_peer(std::chrono::milliseconds timeout = 4000ms) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      pollfd p{fd_.get(), POLLIN, 0};
      if (::poll(&p, 1, 100) <= 0) continue;
      char buf[4096];
      ssize_t n = ::recv(fd_.get(), buf, sizeof buf, 0);
      if (n <= 0) return true;
    }
    return false;
  }

 private:
  FileDescriptor fd_;
  FrameDecoder dec_;
};

class ServerHarness {
 public:
  explicit ServerHarness(ServerConfig cfg) : server_(std::make_unique<ChatServer>(std::move(cfg))) {
    server_->start();
    thread_ = std::thread([this] { server_->run(); });
  }
  ~ServerHarness() { stop(); }
  void stop() {
    if (!thread_.joinable()) return;
    server_->request_stop();
    thread_.join();
  }
  ChatServer& get() { return *server_; }
  uint16_t port() const { return server_->port(); }

 private:
  std::unique_ptr<ChatServer> server_;
  std::thread thread_;
};

ServerConfig base_config(bool et) {
  ServerConfig c;
  c.host = "127.0.0.1";
  c.port = 0;
  c.workers = 3;
  c.edge_triggered = et;
  c.idle_timeout_s = 0;
  return c;
}

bool eventually(const std::function<bool()>& cond, std::chrono::milliseconds t = 3000ms) {
  auto end = std::chrono::steady_clock::now() + t;
  while (std::chrono::steady_clock::now() < end) {
    if (cond()) return true;
    std::this_thread::sleep_for(10ms);
  }
  return cond();
}

}  // namespace

class ServerTest : public ::testing::TestWithParam<bool> {
 protected:
  static void SetUpTestSuite() {
    std::signal(SIGPIPE, SIG_IGN);
    LogLevel lvl = LogLevel::Error;  // CHAT_TEST_LOG=debug to see server logs
    if (const char* e = std::getenv("CHAT_TEST_LOG")) Logger::parse_level(e, lvl);
    Logger::instance().set_level(lvl);
  }
};
INSTANTIATE_TEST_SUITE_P(Triggering, ServerTest, ::testing::Values(true, false),
                         [](const auto& i) { return i.param ? "EdgeTriggered" : "LevelTriggered"; });

TEST_P(ServerTest, TwoClientsChat) {
  ServerHarness h(base_config(GetParam()));
  TestClient a(h.port()), b(h.port());
  ASSERT_TRUE(a.expect("welcome"));
  ASSERT_TRUE(b.expect("welcome"));
  a.send("/nick alice");
  b.send("/nick bob");
  ASSERT_TRUE(b.expect("now known as bob"));
  a.send("hello bob");
  EXPECT_TRUE(b.expect("[lobby] alice: hello bob"));
  EXPECT_TRUE(a.expect("[lobby] alice: hello bob"));
}

TEST_P(ServerTest, PrivateMessagesAndRooms) {
  ServerHarness h(base_config(GetParam()));
  TestClient a(h.port()), b(h.port()), c(h.port());
  a.send("/nick a");
  b.send("/nick b");
  c.send("/nick c");
  ASSERT_TRUE(c.expect("now known as c"));
  a.send("/msg b secret");
  EXPECT_TRUE(b.expect("[pm from a] secret"));
  b.send("/join dev");
  ASSERT_TRUE(b.expect("joined dev"));
  a.send("lobby only");
  EXPECT_TRUE(c.expect("lobby only"));
  b.send("/who");
  EXPECT_TRUE(b.expect("in #dev (1): b"));
}

TEST_P(ServerTest, QuitClosesConnectionAndAnnounces) {
  ServerHarness h(base_config(GetParam()));
  TestClient a(h.port()), b(h.port());
  a.send("/nick a");
  b.send("/nick b");
  ASSERT_TRUE(b.expect("now known as b"));
  a.send("/quit");
  EXPECT_TRUE(a.expect("bye"));
  EXPECT_TRUE(a.closed_by_peer());
  EXPECT_TRUE(b.expect("a left lobby"));
  EXPECT_TRUE(eventually([&] { return h.get().stats().connections == 1; }));
}

TEST_P(ServerTest, MessageOrderIsPreservedPerClient) {
  ServerHarness h(base_config(GetParam()));
  TestClient a(h.port()), b(h.port());
  a.send("/nick a");
  b.send("/nick b");
  ASSERT_TRUE(b.expect("now known as b"));
  constexpr int N = 500;
  for (int i = 0; i < N; ++i) a.send("n" + std::to_string(i));  // many workers, one connection
  for (int i = 0; i < N; ++i) {
    auto m = b.recv();
    ASSERT_TRUE(m) << "timed out at " << i;
    ASSERT_EQ(*m, "[lobby] a: n" + std::to_string(i));
  }
}

TEST_P(ServerTest, PartialAndCoalescedFramesFromClient) {
  ServerHarness h(base_config(GetParam()));
  TestClient a(h.port()), b(h.port());
  a.send("/nick a");
  b.send("/nick b");
  ASSERT_TRUE(b.expect("now known as b"));
  // Dribble one frame a byte at a time...
  std::string f = encode_frame("slow one");
  for (char ch : f) {
    ASSERT_EQ(::send(a.fd(), &ch, 1, MSG_NOSIGNAL), 1);
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(b.expect("a: slow one"));
  // ...then pack many frames into one write.
  std::string blob;
  for (int i = 0; i < 20; ++i) blob += encode_frame("packed" + std::to_string(i));
  ASSERT_TRUE(net::write_all(a.fd(), blob.data(), blob.size()));
  for (int i = 0; i < 20; ++i) EXPECT_TRUE(b.expect("a: packed" + std::to_string(i)));
}

TEST_P(ServerTest, OversizedFrameDropsClient) {
  auto cfg = base_config(GetParam());
  cfg.max_frame = 128;
  ServerHarness h(cfg);
  TestClient a(h.port()), b(h.port());
  ASSERT_TRUE(a.expect("welcome"));
  a.send(std::string(1000, 'x'));
  EXPECT_TRUE(a.closed_by_peer());
  EXPECT_TRUE(eventually([&] { return h.get().stats().closed_protocol == 1; }));
  // the server is still healthy
  b.send("still alive");
  EXPECT_TRUE(b.expect("still alive"));
}

TEST_P(ServerTest, MaxConnectionsEnforced) {
  auto cfg = base_config(GetParam());
  cfg.max_connections = 3;
  ServerHarness h(cfg);
  std::vector<std::unique_ptr<TestClient>> ok;
  for (int i = 0; i < 3; ++i) {
    ok.push_back(std::make_unique<TestClient>(h.port()));
    ASSERT_TRUE(ok.back()->expect("welcome"));
  }
  TestClient extra(h.port());
  EXPECT_TRUE(extra.closed_by_peer());
  EXPECT_EQ(h.get().stats().rejected, 1u);
  ok.pop_back();  // free a slot
  EXPECT_TRUE(eventually([&] { return h.get().stats().connections == 2; }));
  TestClient again(h.port());
  EXPECT_TRUE(again.expect("welcome"));
}

TEST_P(ServerTest, IdleClientsAreDisconnected) {
  auto cfg = base_config(GetParam());
  cfg.idle_timeout_s = 1;
  ServerHarness h(cfg);
  TestClient idle(h.port());
  TestClient chatty(h.port());
  ASSERT_TRUE(idle.expect("welcome"));
  for (int i = 0; i < 6; ++i) {  // keep one alive past the timeout
    chatty.send("/who");
    std::this_thread::sleep_for(400ms);
  }
  EXPECT_TRUE(idle.closed_by_peer(3000ms));
  EXPECT_EQ(h.get().stats().closed_idle, 1u);
  EXPECT_TRUE(chatty.expect("in #lobby"));
}

TEST_P(ServerTest, SlowConsumerIsCutOffByBackpressure) {
  auto cfg = base_config(GetParam());
  cfg.max_out_bytes = 128 * 1024;
  cfg.max_frame = 16 * 1024;
  ServerHarness h(cfg);

  TestClient slow(h.port());
  int tiny = 4096;
  ::setsockopt(slow.fd(), SOL_SOCKET, SO_RCVBUF, &tiny, sizeof tiny);  // and never read
  TestClient sender(h.port());
  ASSERT_TRUE(sender.expect("welcome"));

  // Lock-step: wait for our own echo before sending the next, so only `slow`
  // (which never reads) can fall behind.
  const std::string big(8000, 'z');
  for (int i = 0; i < 3000 && h.get().stats().closed_overflow == 0; ++i) {
    ASSERT_TRUE(sender.send(big));
    ASSERT_TRUE(sender.recv(2000ms)) << "sender lost at message " << i;
  }
  EXPECT_TRUE(eventually([&] { return h.get().stats().closed_overflow >= 1; }, 5000ms));
  // The sender (a healthy client) was not collateral damage.
  sender.send("/who");
  EXPECT_TRUE(sender.expect("in #lobby"));
}

TEST_P(ServerTest, DisconnectMidWriteDoesNotCrash) {  // EPIPE / SIGPIPE path
  ServerHarness h(base_config(GetParam()));
  TestClient sender(h.port());
  for (int round = 0; round < 20; ++round) {
    {
      TestClient victim(h.port());
      ASSERT_TRUE(victim.expect("welcome"));
      linger lg{1, 0};  // close => RST, the nastiest disconnect
      ::setsockopt(victim.fd(), SOL_SOCKET, SO_LINGER, &lg, sizeof lg);
      for (int i = 0; i < 20; ++i) sender.send(std::string(2000, 'q'));
    }
    while (sender.recv(0ms)) {}
  }
  sender.send("/who");
  EXPECT_TRUE(sender.expect("in #lobby"));
}

TEST_P(ServerTest, ConnectDisconnectStress) {
  auto cfg = base_config(GetParam());
  cfg.workers = 4;
  ServerHarness h(cfg);
  constexpr int kThreads = 8, kIters = 150;
  std::atomic<int> failures{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t)
    ts.emplace_back([&, t] {
      for (int i = 0; i < kIters; ++i) {
        try {
          TestClient c(h.port());
          c.send("/nick u" + std::to_string(t) + "_" + std::to_string(i));
          c.send("/join r" + std::to_string(i % 5));
          c.send("hello from " + std::to_string(t));
          if (i % 3 == 0) c.send("/quit");
          if (i % 7 == 0) c.recv(5ms);
          // abrupt close for the rest
        } catch (const std::exception&) {
          ++failures;
        }
      }
    });
  for (auto& t : ts) t.join();
  EXPECT_EQ(failures, 0);
  EXPECT_TRUE(eventually([&] { return h.get().stats().connections == 0; }, 30000ms));
  EXPECT_TRUE(eventually([&] { return h.get().hub().session_count() == 0; }, 30000ms));
  EXPECT_EQ(h.get().hub().room_count(), 0u);
  EXPECT_EQ(h.get().stats().accepted, static_cast<std::uint64_t>(kThreads * kIters));
}

TEST_P(ServerTest, ManyConcurrentClientsAllReceiveBroadcast) {
  ServerHarness h(base_config(GetParam()));
  constexpr int N = 200;
  std::vector<std::unique_ptr<TestClient>> cs;
  for (int i = 0; i < N; ++i) cs.push_back(std::make_unique<TestClient>(h.port()));
  cs[0]->send("/nick speaker");
  ASSERT_TRUE(cs[0]->expect("now known as speaker"));
  cs[0]->send("everyone listen");
  for (int i = 0; i < N; ++i) EXPECT_TRUE(cs[i]->expect("speaker: everyone listen")) << i;
}

TEST_P(ServerTest, UdpPingPong) {
  ServerHarness h(base_config(GetParam()));
  FileDescriptor u(::socket(AF_INET, SOCK_DGRAM, 0));
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(h.port());
  inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
  ASSERT_EQ(::connect(u.get(), reinterpret_cast<sockaddr*>(&sa), sizeof sa), 0);

  std::string d = encode_datagram({DatagramType::Ping, 0, 41, 123456}, "hi");
  ASSERT_EQ(::send(u.get(), d.data(), d.size(), 0), static_cast<ssize_t>(d.size()));
  pollfd p{u.get(), POLLIN, 0};
  ASSERT_EQ(::poll(&p, 1, 2000), 1);
  char buf[512];
  ssize_t n = ::recv(u.get(), buf, sizeof buf, 0);
  ASSERT_GT(n, 0);
  auto r = decode_datagram(buf, static_cast<std::size_t>(n));
  ASSERT_TRUE(r);
  EXPECT_EQ(r->header.type, DatagramType::Pong);
  EXPECT_EQ(r->header.seq, 41u);
  EXPECT_EQ(r->header.timestamp_us, 123456u);
  EXPECT_EQ(r->payload, "hi");
}

TEST_P(ServerTest, UdpMessageReachesTcpRoomAndLossIsCounted) {
  ServerHarness h(base_config(GetParam()));
  TestClient tcp(h.port());
  tcp.send("/nick dave");
  ASSERT_TRUE(tcp.expect("now known as dave"));

  FileDescriptor u(::socket(AF_INET, SOCK_DGRAM, 0));
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(h.port());
  inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
  ASSERT_EQ(::connect(u.get(), reinterpret_cast<sockaddr*>(&sa), sizeof sa), 0);

  // seq 0,1,(2,3 dropped),4 and a late 2 => 1 lost, 1 reordered
  for (std::uint32_t seq : {0u, 1u, 4u, 2u}) {
    std::string d = encode_datagram({DatagramType::Message, 0, seq, 1}, "dave udp says hi");
    ASSERT_GT(::send(u.get(), d.data(), d.size(), 0), 0);
  }
  EXPECT_TRUE(tcp.expect("(udp) dave: udp says hi"));
  ASSERT_TRUE(eventually([&] { return h.get().stats().udp_received == 4; }));
  auto s = h.get().stats();
  EXPECT_EQ(s.udp_lost, 1u);  // seq 3
  EXPECT_EQ(s.udp_reordered, 1u);
  // garbage is counted, not crashed on
  ::send(u.get(), "junk", 4, 0);
  EXPECT_TRUE(eventually([&] { return h.get().stats().udp_invalid == 1; }));
}

TEST_P(ServerTest, GracefulStopClosesClients) {
  ServerHarness h(base_config(GetParam()));
  TestClient a(h.port());
  ASSERT_TRUE(a.expect("welcome"));
  h.stop();  // joins run(): must return promptly
  EXPECT_TRUE(a.closed_by_peer());
}

// ---- thread-per-client baseline: same protocol, smoke-tested -----------------
TEST(ThreadedBaseline, ChatsAndStops) {
  std::signal(SIGPIPE, SIG_IGN);
  Logger::instance().set_level(LogLevel::Error);
  ServerConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 0;
  cfg.idle_timeout_s = 0;
  ThreadedServer s(cfg);
  s.start();
  std::thread t([&] { s.run(); });
  {
    TestClient a(s.port()), b(s.port());
    ASSERT_TRUE(a.expect("welcome"));
    a.send("/nick a");
    b.send("/nick b");
    ASSERT_TRUE(b.expect("now known as b"));
    a.send("hi");
    EXPECT_TRUE(b.expect("[lobby] a: hi"));
    a.send("/quit");
    EXPECT_TRUE(b.expect("a left lobby"));
  }
  s.request_stop();
  t.join();
}

// ---- multiple I/O loops (SO_REUSEPORT listeners) -------------------------------
TEST(MultiLoop, BroadcastCrossesLoopsAndStressIsClean) {
  std::signal(SIGPIPE, SIG_IGN);
  Logger::instance().set_level(LogLevel::Error);
  auto cfg = base_config(true);
  cfg.io_threads = 4;
  cfg.workers = 4;
  ServerHarness h(cfg);

  constexpr int N = 120;  // enough that the kernel spreads them over all 4 listeners
  std::vector<std::unique_ptr<TestClient>> cs;
  for (int i = 0; i < N; ++i) {
    cs.push_back(std::make_unique<TestClient>(h.port()));
    ASSERT_TRUE(cs.back()->expect("welcome")) << i;  // i.e. accepted and registered
  }
  cs[0]->send("/nick speaker");
  ASSERT_TRUE(cs[0]->expect("now known as speaker"));
  cs[0]->send("to all loops");
  for (int i = 0; i < N; ++i) EXPECT_TRUE(cs[i]->expect("speaker: to all loops")) << i;
  EXPECT_EQ(h.get().stats().connections, static_cast<std::size_t>(N));
  cs.clear();
  EXPECT_TRUE(eventually([&] { return h.get().stats().connections == 0; }));

  std::atomic<int> failures{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < 6; ++t)
    ts.emplace_back([&, t] {
      for (int i = 0; i < 100; ++i) {
        try {
          TestClient c(h.port());
          c.send("/join r" + std::to_string(i % 4));
          c.send("hi " + std::to_string(t));
          if (i % 2) c.send("/quit");
        } catch (const std::exception&) {
          ++failures;
        }
      }
    });
  for (auto& t : ts) t.join();
  EXPECT_EQ(failures, 0);
  EXPECT_TRUE(eventually([&] { return h.get().stats().connections == 0; }, 10000ms));
  EXPECT_TRUE(eventually([&] { return h.get().hub().session_count() == 0; }, 10000ms));
  EXPECT_EQ(h.get().hub().room_count(), 0u);
  h.stop();  // all loops must exit
}
