// chat_client: interactive CLI client.
//   chat_client [--host H] [--port P] [--nick NAME]
// Type lines to chat. Extra client-side commands:
//   /ping        send a UDP ping to the server and print the round-trip time
//   /udp <text>  send <text> to your room over UDP (needs /nick registered via TCP)
//   /quit        leave
#include <poll.h>
#include <sys/socket.h>

#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>

#include "chat/cli.hpp"
#include "chat/codec.hpp"
#include "chat/net.hpp"

namespace {
std::uint64_t now_us() {
  using namespace std::chrono;
  return duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
}
}  // namespace

int main(int argc, char** argv) {
  chat::Args args(argc, argv);
  const std::string host = args.get("host", "127.0.0.1");
  const auto port = static_cast<std::uint16_t>(args.get_int("port", 9000));
  std::string nick = args.get("nick", "");

  try {
    auto tcp = chat::net::connect_tcp(host, port);
    chat::net::set_tcp_nodelay(tcp.get());

    // UDP socket "connected" to the server so we can send/recv without addresses.
    chat::FileDescriptor udp(::socket(AF_INET, SOCK_DGRAM, 0));
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &sa.sin_addr);
    ::connect(udp.get(), reinterpret_cast<sockaddr*>(&sa), sizeof sa);
    std::uint32_t seq = 0;

    auto send_line = [&](const std::string& s) {
      std::string f = chat::encode_frame(s);
      return chat::net::write_all(tcp.get(), f.data(), f.size());
    };
    if (!nick.empty()) send_line("/nick " + nick);

    chat::FrameDecoder dec(1 << 20);
    pollfd fds[3] = {{0, POLLIN, 0}, {tcp.get(), POLLIN, 0}, {udp.get(), POLLIN, 0}};
    bool stdin_open = true;
    for (;;) {
      fds[0].fd = stdin_open ? 0 : -1;
      if (::poll(fds, 3, -1) < 0) continue;

      if (fds[0].revents & (POLLIN | POLLHUP)) {
        std::string line;
        if (!std::getline(std::cin, line)) {
          stdin_open = false;
          send_line("/quit");
        } else if (line == "/ping") {
          chat::DatagramHeader h{chat::DatagramType::Ping, 0, seq++, now_us()};
          std::string d = chat::encode_datagram(h);
          ::send(udp.get(), d.data(), d.size(), 0);
        } else if (line.rfind("/udp ", 0) == 0) {
          chat::DatagramHeader h{chat::DatagramType::Message, 0, seq++, now_us()};
          std::string d = chat::encode_datagram(h, nick + " " + line.substr(5));
          ::send(udp.get(), d.data(), d.size(), 0);
        } else {
          if (line.rfind("/nick ", 0) == 0) nick = line.substr(6);
          if (!send_line(line)) break;
        }
      }
      if (fds[1].revents & (POLLIN | POLLHUP | POLLERR)) {
        char buf[4096];
        ssize_t n = ::recv(tcp.get(), buf, sizeof buf, 0);
        if (n <= 0) break;
        dec.feed(buf, static_cast<std::size_t>(n));
        while (auto m = dec.next()) std::cout << *m << std::endl;
        if (dec.failed()) break;
      }
      if (fds[2].revents & POLLIN) {
        char buf[2048];
        ssize_t n = ::recv(udp.get(), buf, sizeof buf, 0);
        if (n > 0) {
          auto d = chat::decode_datagram(buf, static_cast<std::size_t>(n));
          if (d && d->header.type == chat::DatagramType::Pong)
            std::cout << "* udp pong seq=" << d->header.seq << " rtt="
                      << (now_us() - d->header.timestamp_us) << "us" << std::endl;
        }
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
