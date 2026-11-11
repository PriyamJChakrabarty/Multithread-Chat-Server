#pragma once
// Thread-safe leveled logger. One mutex around the write; messages are
// formatted by the caller before the lock is taken.
#include <atomic>
#include <sstream>
#include <string>
#include <string_view>

namespace chat {

enum class LogLevel { Debug = 0, Info = 1, Warn = 2, Error = 3, Off = 4 };

class Logger {
 public:
  static Logger& instance();
  void set_level(LogLevel l) { level_.store(l, std::memory_order_relaxed); }
  LogLevel level() const { return level_.load(std::memory_order_relaxed); }
  bool enabled(LogLevel l) const { return l >= level(); }
  void log(LogLevel l, std::string_view msg);
  static bool parse_level(std::string_view s, LogLevel& out);

 private:
  std::atomic<LogLevel> level_{LogLevel::Info};
};

}  // namespace chat

#define CHAT_LOG(lvl, expr)                                    \
  do {                                                         \
    if (::chat::Logger::instance().enabled(lvl)) {             \
      std::ostringstream chat_log_os__;                        \
      chat_log_os__ << expr;                                   \
      ::chat::Logger::instance().log(lvl, chat_log_os__.str()); \
    }                                                          \
  } while (0)

#define LOG_DEBUG(expr) CHAT_LOG(::chat::LogLevel::Debug, expr)
#define LOG_INFO(expr) CHAT_LOG(::chat::LogLevel::Info, expr)
#define LOG_WARN(expr) CHAT_LOG(::chat::LogLevel::Warn, expr)
#define LOG_ERROR(expr) CHAT_LOG(::chat::LogLevel::Error, expr)
