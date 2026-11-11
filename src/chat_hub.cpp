#include "chat/chat_hub.hpp"

#include <algorithm>
#include <cctype>

namespace chat {

namespace {

bool valid_name(std::string_view s, std::size_t max_len) {
  if (s.empty() || s.size() > max_len) return false;
  return std::all_of(s.begin(), s.end(), [](unsigned char c) {
    return std::isalnum(c) || c == '_' || c == '-';
  });
}

std::string_view trim(std::string_view s) {
  while (!s.empty() && (s.back() == '\n' || s.back() == '\r' || s.back() == ' ' || s.back() == '\t'))
    s.remove_suffix(1);
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
  return s;
}

// Split "word rest of line" at the first run of spaces.
std::pair<std::string_view, std::string_view> split_word(std::string_view s) {
  auto sp = s.find(' ');
  if (sp == std::string_view::npos) return {s, {}};
  return {s.substr(0, sp), trim(s.substr(sp + 1))};
}

void sys(const SessionPtr& s, const std::string& text) { s->send("* " + text); }

}  // namespace

// ------------------------------------------------------------------ helpers
ChatHub::Recipients ChatHub::members_locked(const std::string& room, std::uint64_t except_id) const {
  Recipients out;
  auto it = rooms_.find(room);
  if (it == rooms_.end()) return out;
  out.reserve(it->second.size());
  for (auto id : it->second) {
    if (id == except_id) continue;
    auto sit = sessions_.find(id);
    if (sit != sessions_.end()) out.push_back(sit->second);
  }
  return out;
}

void ChatHub::deliver(const Recipients& to, const std::string& line) {
  for (const auto& r : to) r->send(line);
}

void ChatHub::leave_room_locked(const SessionPtr& s) {
  auto it = rooms_.find(s->room);
  if (it == rooms_.end()) return;
  it->second.erase(s->id);
  if (it->second.empty()) rooms_.erase(it);
}

// ------------------------------------------------------------- session mgmt
SessionPtr ChatHub::add_session(SendFn send) {
  SessionPtr s;
  Recipients others;
  std::string nick, announce;
  {
    std::lock_guard<std::mutex> lk(mu_);
    s = std::make_shared<Session>(next_id_++, std::move(send));
    nick = "guest" + std::to_string(s->id);
    while (nicks_.count(nick)) nick += "_";
    s->nick = nick;
    s->room = kDefaultRoom;
    sessions_[s->id] = s;
    nicks_[nick] = s->id;
    rooms_[s->room].insert(s->id);
    others = members_locked(s->room, s->id);
    announce = "* " + nick + " joined " + std::string(kDefaultRoom);
  }
  sys(s, "welcome " + nick + "! you are in #" + kDefaultRoom + ". /help for commands");
  deliver(others, announce);
  return s;
}

void ChatHub::remove_session(const SessionPtr& s) {
  Recipients others;
  std::string announce;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (s->closed.exchange(true)) return;
    if (sessions_.erase(s->id) == 0) return;
    nicks_.erase(s->nick);
    others = members_locked(s->room, s->id);
    announce = "* " + s->nick + " left " + s->room;
    leave_room_locked(s);
  }
  deliver(others, announce);
}

std::size_t ChatHub::session_count() const {
  std::lock_guard<std::mutex> lk(mu_);
  return sessions_.size();
}

std::size_t ChatHub::room_count() const {
  std::lock_guard<std::mutex> lk(mu_);
  return rooms_.size();
}

// ----------------------------------------------------------------- commands
ChatHub::Result ChatHub::handle_line(const SessionPtr& s, std::string_view raw) {
  if (s->closed.load()) return Result::Quit;
  std::string_view line = trim(raw);
  if (line.empty()) return Result::Continue;

  if (line.front() != '/') {
    say(s, line);
    return Result::Continue;
  }
  auto [cmd, arg] = split_word(line);
  if (cmd == "/nick") cmd_nick(s, arg);
  else if (cmd == "/join") cmd_join(s, arg);
  else if (cmd == "/who") cmd_who(s);
  else if (cmd == "/rooms") cmd_rooms(s);
  else if (cmd == "/msg" || cmd == "/w") cmd_msg(s, arg);
  else if (cmd == "/quit") {
    sys(s, "bye");
    return Result::Quit;
  } else if (cmd == "/help") {
    sys(s, "commands: /nick <name>  /join <room>  /who  /rooms  /msg <nick> <text>  /quit");
  } else {
    sys(s, "unknown command " + std::string(cmd) + " (try /help)");
  }
  return Result::Continue;
}

void ChatHub::say(const SessionPtr& s, std::string_view text) {
  Recipients to;
  std::string line;
  {
    std::lock_guard<std::mutex> lk(mu_);
    to = members_locked(s->room);  // includes the sender: acts as an ack/echo
    line = "[" + s->room + "] " + s->nick + ": " + std::string(text);
  }
  deliver(to, line);
}

void ChatHub::cmd_nick(const SessionPtr& s, std::string_view arg) {
  if (!valid_name(arg, kMaxNickLen)) {
    sys(s, "nick must be 1-" + std::to_string(kMaxNickLen) + " chars of [A-Za-z0-9_-]");
    return;
  }
  Recipients to;
  std::string announce, err;
  {
    std::lock_guard<std::mutex> lk(mu_);
    std::string nn(arg);
    // `closed` is set under mu_ by remove_session; re-check here so a command
    // queued before the disconnect cannot resurrect the session's entries.
    if (s->closed.load()) return;
    if (nn == s->nick) {
      err = "you are already " + nn;
    } else if (nicks_.count(nn)) {
      err = "nick " + nn + " is taken";
    } else {
      nicks_.erase(s->nick);
      nicks_[nn] = s->id;
      announce = "* " + s->nick + " is now known as " + nn;
      s->nick = nn;
      to = members_locked(s->room);
    }
  }
  if (!err.empty()) sys(s, err);
  else deliver(to, announce);
}

void ChatHub::cmd_join(const SessionPtr& s, std::string_view arg) {
  if (!valid_name(arg, kMaxRoomLen)) {
    sys(s, "room must be 1-" + std::to_string(kMaxRoomLen) + " chars of [A-Za-z0-9_-]");
    return;
  }
  Recipients old_members, new_members;
  std::string left, joined, err;
  {
    std::lock_guard<std::mutex> lk(mu_);
    std::string room(arg);
    if (s->closed.load()) return;  // see cmd_nick
    if (room == s->room) {
      err = "already in #" + room;
    } else {
      leave_room_locked(s);
      old_members = members_locked(s->room, s->id);
      left = "* " + s->nick + " left " + s->room;
      s->room = room;
      rooms_[room].insert(s->id);
      new_members = members_locked(room);  // includes s: confirms the move
      joined = "* " + s->nick + " joined " + room;
    }
  }
  if (!err.empty()) { sys(s, err); return; }
  deliver(old_members, left);
  deliver(new_members, joined);
}

void ChatHub::cmd_who(const SessionPtr& s) {
  std::string out;
  {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::string> names;
    auto it = rooms_.find(s->room);
    if (it != rooms_.end())
      for (auto id : it->second) {
        auto sit = sessions_.find(id);
        if (sit != sessions_.end()) names.push_back(sit->second->nick);
      }
    std::sort(names.begin(), names.end());
    out = "in #" + s->room + " (" + std::to_string(names.size()) + "):";
    for (auto& n : names) out += " " + n;
  }
  sys(s, out);
}

void ChatHub::cmd_rooms(const SessionPtr& s) {
  std::string out;
  {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<std::string, std::size_t>> rs;
    for (auto& [name, ids] : rooms_) rs.emplace_back(name, ids.size());
    std::sort(rs.begin(), rs.end());
    out = "rooms:";
    for (auto& [name, n] : rs) out += " #" + name + "(" + std::to_string(n) + ")";
  }
  sys(s, out);
}

void ChatHub::cmd_msg(const SessionPtr& s, std::string_view arg) {
  auto [target, text] = split_word(arg);
  if (target.empty() || text.empty()) {
    sys(s, "usage: /msg <nick> <text>");
    return;
  }
  SessionPtr dest;
  std::string from, to_nick;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = nicks_.find(std::string(target));
    if (it != nicks_.end()) dest = sessions_[it->second];
    from = s->nick;
    to_nick = std::string(target);
  }
  if (!dest) {
    sys(s, "no such user: " + to_nick);
    return;
  }
  dest->send("[pm from " + from + "] " + std::string(text));
  if (dest != s) s->send("[pm to " + to_nick + "] " + std::string(text));
}

bool ChatHub::deliver_udp(const std::string& nick, std::string_view text) {
  Recipients to;
  std::string line;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = nicks_.find(nick);
    if (it == nicks_.end()) return false;
    const SessionPtr& s = sessions_[it->second];
    to = members_locked(s->room);
    line = "[" + s->room + "] (udp) " + s->nick + ": " + std::string(text);
  }
  deliver(to, line);
  return true;
}

}  // namespace chat
