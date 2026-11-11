#pragma once
// ChatHub: client registry, rooms and command routing, independent of how bytes
// reach the clients. The epoll server and the thread-per-client baseline both
// plug into it through a per-session `send` callback.
//
// Locking: one mutex (mu_) guards the registry, nicks, rooms and each
// session's nick/room. LOCK RULE: mu_ is never held while calling a session's
// `send` (it may block or take the transport's own locks). Handlers copy the
// recipient list under mu_, release it, then deliver. Lock order is therefore
// hub mutex -> (released) -> transport out-queue mutex; they never nest.
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace chat {

// Delivers one text line to a client. Must be thread-safe and non-throwing.
using SendFn = std::function<void(std::string_view line)>;

struct Session {
  Session(std::uint64_t i, SendFn s) : id(i), send(std::move(s)) {}
  const std::uint64_t id;
  const SendFn send;
  std::atomic<bool> closed{false};
  // Guarded by ChatHub::mu_:
  std::string nick;
  std::string room;
};
using SessionPtr = std::shared_ptr<Session>;

class ChatHub {
 public:
  enum class Result { Continue, Quit };

  static constexpr const char* kDefaultRoom = "lobby";
  static constexpr std::size_t kMaxNickLen = 16;
  static constexpr std::size_t kMaxRoomLen = 24;

  // Registers a client: assigns a guest nick, joins the lobby, greets it.
  SessionPtr add_session(SendFn send);
  // Handle one line of input (plain text or a /command).
  Result handle_line(const SessionPtr& s, std::string_view line);
  // Unregister (idempotent): leaves the room and tells the others.
  void remove_session(const SessionPtr& s);
  // A chat message that arrived over UDP on behalf of `nick`. Returns false if
  // no such user is connected.
  bool deliver_udp(const std::string& nick, std::string_view text);

  std::size_t session_count() const;
  std::size_t room_count() const;

 private:
  using Recipients = std::vector<SessionPtr>;

  Recipients members_locked(const std::string& room, std::uint64_t except_id = 0) const;
  void leave_room_locked(const SessionPtr& s);
  static void deliver(const Recipients& to, const std::string& line);

  void cmd_nick(const SessionPtr& s, std::string_view arg);
  void cmd_join(const SessionPtr& s, std::string_view arg);
  void cmd_who(const SessionPtr& s);
  void cmd_rooms(const SessionPtr& s);
  void cmd_msg(const SessionPtr& s, std::string_view arg);
  void say(const SessionPtr& s, std::string_view text);

  mutable std::mutex mu_;
  std::uint64_t next_id_ = 1;
  std::unordered_map<std::uint64_t, SessionPtr> sessions_;
  std::unordered_map<std::string, std::uint64_t> nicks_;
  std::unordered_map<std::string, std::unordered_set<std::uint64_t>> rooms_;
};

}  // namespace chat
