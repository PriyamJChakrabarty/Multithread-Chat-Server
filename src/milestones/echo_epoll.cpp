// Milestone 3: single-threaded epoll echo server.
//   --lt  level-triggered (default): one read per readiness notification is fine.
//   --et  edge-triggered: MUST accept/read until EAGAIN or events are lost.
// Partial writes are buffered per client and flushed on EPOLLOUT.
#include <sys/epoll.h>
#include <sys/socket.h>

#include <cerrno>
#include <csignal>
#include <iostream>
#include <string>
#include <unordered_map>

#include "chat/cli.hpp"
#include "chat/net.hpp"

namespace {

struct Client {
  chat::FileDescriptor fd;
  std::string out;  // bytes accepted from the peer but not yet written back
  bool want_out = false;
};

}  // namespace

int main(int argc, char** argv) {
  chat::Args args(argc, argv);
  auto port = static_cast<uint16_t>(args.get_int("port", 9001));
  const bool et = args.has("et");
  std::signal(SIGPIPE, SIG_IGN);

  try {
    auto listener = chat::net::listen_tcp("0.0.0.0", port, 1024, /*nonblocking=*/true);
    chat::FileDescriptor ep(::epoll_create1(EPOLL_CLOEXEC));
    if (!ep) throw std::system_error(errno, std::generic_category(), "epoll_create1");
    const uint32_t mode = et ? static_cast<uint32_t>(EPOLLET) : 0u;

    auto ctl = [&](int op, int fd, uint32_t ev) {
      epoll_event e{};
      e.events = ev;
      e.data.fd = fd;
      return ::epoll_ctl(ep.get(), op, fd, &e);
    };
    ctl(EPOLL_CTL_ADD, listener.get(), EPOLLIN | mode);
    std::cerr << "echo_epoll (" << (et ? "edge" : "level") << "-triggered) listening on "
              << chat::net::local_port(listener.get()) << "\n";

    std::unordered_map<int, Client> clients;
    epoll_event events[256];

    auto drop = [&](int fd) { clients.erase(fd); };  // closes fd; epoll forgets it

    // Returns false if the client should be dropped.
    auto flush = [&](int fd, Client& c) {
      while (!c.out.empty()) {
        ssize_t n = ::send(fd, c.out.data(), c.out.size(), MSG_NOSIGNAL);
        if (n < 0) {
          if (errno == EINTR) continue;
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (!c.want_out) {  // partial write: wait for the socket to drain
              c.want_out = true;
              ctl(EPOLL_CTL_MOD, fd, EPOLLIN | EPOLLOUT | mode);
            }
            return true;
          }
          return false;  // EPIPE / ECONNRESET
        }
        c.out.erase(0, static_cast<size_t>(n));
      }
      if (c.want_out) {
        c.want_out = false;
        ctl(EPOLL_CTL_MOD, fd, EPOLLIN | mode);
      }
      return true;
    };

    for (;;) {
      int n = ::epoll_wait(ep.get(), events, 256, -1);
      if (n < 0) {
        if (errno == EINTR) continue;
        throw std::system_error(errno, std::generic_category(), "epoll_wait");
      }
      for (int i = 0; i < n; ++i) {
        int fd = events[i].data.fd;
        uint32_t ev = events[i].events;

        if (fd == listener.get()) {
          // Edge-triggered: loop until EAGAIN. Level-triggered would also work
          // with a single accept, but looping is harmless.
          for (;;) {
            int cfd = ::accept4(fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (cfd < 0) {
              if (errno == EINTR) continue;
              break;  // EAGAIN, or EMFILE (handled properly in the chat server)
            }
            chat::net::set_tcp_nodelay(cfd);
            clients[cfd].fd.reset(cfd);
            ctl(EPOLL_CTL_ADD, cfd, EPOLLIN | EPOLLRDHUP | mode);
          }
          continue;
        }

        auto it = clients.find(fd);
        if (it == clients.end()) continue;
        Client& c = it->second;
        bool alive = true;

        if (ev & (EPOLLERR | EPOLLHUP)) alive = false;

        if (alive && (ev & EPOLLIN)) {
          char buf[4096];
          for (;;) {
            ssize_t r = ::recv(fd, buf, sizeof buf, 0);
            if (r > 0) {
              c.out.append(buf, static_cast<size_t>(r));
              if (!et) break;  // level-triggered: epoll will tell us again
              continue;
            }
            if (r == 0) { alive = false; break; }        // orderly shutdown
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) alive = false;
            break;                                        // EAGAIN: drained
          }
          if (alive) alive = flush(fd, c);
        }
        if (alive && (ev & EPOLLOUT)) alive = flush(fd, c);
        if (alive && (ev & EPOLLRDHUP) && c.out.empty()) alive = false;
        if (!alive) drop(fd);
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "\n";
    return 1;
  }
}
