// Milestone 1: blocking TCP echo server, one client at a time.
//   socket -> bind -> listen -> accept -> read/write loop.
#include <sys/socket.h>

#include <cerrno>
#include <csignal>
#include <iostream>

#include "chat/cli.hpp"
#include "chat/net.hpp"

int main(int argc, char** argv) {
  chat::Args args(argc, argv);
  auto port = static_cast<uint16_t>(args.get_int("port", 9001));
  std::signal(SIGPIPE, SIG_IGN);

  try {
    auto listener = chat::net::listen_tcp("0.0.0.0", port, 16, /*nonblocking=*/false);
    std::cerr << "echo_blocking listening on " << chat::net::local_port(listener.get()) << "\n";
    for (;;) {
      chat::FileDescriptor client(::accept4(listener.get(), nullptr, nullptr, SOCK_CLOEXEC));
      if (!client) {
        if (errno == EINTR) continue;
        throw std::system_error(errno, std::generic_category(), "accept");
      }
      // While this client is connected no one else is served: the weakness
      // that milestone 2 fixes.
      char buf[4096];
      for (;;) {
        ssize_t n = ::recv(client.get(), buf, sizeof buf, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        if (!chat::net::write_all(client.get(), buf, static_cast<size_t>(n))) break;
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "\n";
    return 1;
  }
}
