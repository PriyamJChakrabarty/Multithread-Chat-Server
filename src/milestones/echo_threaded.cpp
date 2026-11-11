// Milestone 2: thread-per-client TCP echo server (the benchmark baseline model).
#include <sys/socket.h>

#include <cerrno>
#include <csignal>
#include <iostream>
#include <thread>

#include "chat/cli.hpp"
#include "chat/net.hpp"

namespace {
void serve(chat::FileDescriptor client) {
  char buf[4096];
  for (;;) {
    ssize_t n = ::recv(client.get(), buf, sizeof buf, 0);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;
    if (!chat::net::write_all(client.get(), buf, static_cast<size_t>(n))) break;
  }
}  // FileDescriptor destructor closes the socket
}  // namespace

int main(int argc, char** argv) {
  chat::Args args(argc, argv);
  auto port = static_cast<uint16_t>(args.get_int("port", 9001));
  std::signal(SIGPIPE, SIG_IGN);

  try {
    auto listener = chat::net::listen_tcp("0.0.0.0", port, 1024, false);
    std::cerr << "echo_threaded listening on " << chat::net::local_port(listener.get()) << "\n";
    for (;;) {
      chat::FileDescriptor client(::accept4(listener.get(), nullptr, nullptr, SOCK_CLOEXEC));
      if (!client) {
        if (errno == EINTR || errno == ECONNABORTED) continue;
        throw std::system_error(errno, std::generic_category(), "accept");
      }
      // One OS thread per connection: simple, but memory and scheduling cost
      // grow linearly with the number of clients.
      std::thread(serve, std::move(client)).detach();
    }
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "\n";
    return 1;
  }
}
