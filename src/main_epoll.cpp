// chat_server: epoll reactor + thread pool (the main deliverable).
#include <csignal>
#include <iostream>
#include <thread>

#include "chat/cli.hpp"
#include "chat/epoll_server.hpp"
#include "chat/logger.hpp"

int main(int argc, char** argv) {
  chat::Args a(argc, argv);
  if (a.has("help")) {
    std::cout << "usage: chat_server [--port N] [--host IP] [--workers N] [--io-threads N] [--max-conns N]\n"
                 "                   [--idle SECONDS] [--max-frame BYTES] [--max-out BYTES]\n"
                 "                   [--lt] [--no-udp] [--log-level debug|info|warn|error]\n";
    return 0;
  }
  chat::LogLevel lvl;
  if (chat::Logger::parse_level(a.get("log-level", "info"), lvl)) chat::Logger::instance().set_level(lvl);

  chat::ServerConfig cfg;
  cfg.host = a.get("host", cfg.host);
  cfg.port = static_cast<std::uint16_t>(a.get_int("port", 9000));
  unsigned hw = std::thread::hardware_concurrency();
  cfg.workers = static_cast<std::size_t>(a.get_int("workers", hw ? hw : 4));
  cfg.max_connections = static_cast<std::size_t>(a.get_int("max-conns", 20000));
  cfg.idle_timeout_s = static_cast<unsigned>(a.get_int("idle", 300));
  cfg.max_frame = static_cast<std::size_t>(a.get_int("max-frame", 64 * 1024));
  cfg.max_out_bytes = static_cast<std::size_t>(a.get_int("max-out", 1 << 20));
  cfg.io_threads = static_cast<std::size_t>(a.get_int("io-threads", 1));
  cfg.edge_triggered = !a.has("lt");
  cfg.enable_udp = !a.has("no-udp");
  cfg.handle_signals = true;

  std::signal(SIGPIPE, SIG_IGN);  // a dead peer must yield EPIPE, not kill us
  try {
    chat::ChatServer server(cfg);
    server.start();
    server.run();
  } catch (const std::exception& e) {
    LOG_ERROR("fatal: " << e.what());
    return 1;
  }
  return 0;
}
