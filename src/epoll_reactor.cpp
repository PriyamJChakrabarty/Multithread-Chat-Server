#include "chat/epoll_reactor.hpp"

#include <sys/epoll.h>
#include <sys/eventfd.h>

#include <cerrno>
#include <system_error>

#include "chat/logger.hpp"

namespace chat {

namespace {
[[noreturn]] void fail(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}
}  // namespace

EpollReactor::EpollReactor() {
  epfd_.reset(::epoll_create1(EPOLL_CLOEXEC));
  if (!epfd_) fail("epoll_create1");
  wake_.reset(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
  if (!wake_) fail("eventfd");
  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.fd = wake_.get();
  if (::epoll_ctl(epfd_.get(), EPOLL_CTL_ADD, wake_.get(), &ev) < 0) fail("epoll_ctl(wake)");
}

void EpollReactor::add(int fd, std::uint32_t events, Handler h) {
  epoll_event ev{};
  ev.events = events;
  ev.data.fd = fd;
  if (::epoll_ctl(epfd_.get(), EPOLL_CTL_ADD, fd, &ev) < 0) fail("epoll_ctl(ADD)");
  handlers_[fd] = std::make_shared<Handler>(std::move(h));
}

void EpollReactor::modify(int fd, std::uint32_t events) {
  epoll_event ev{};
  ev.events = events;
  ev.data.fd = fd;
  if (::epoll_ctl(epfd_.get(), EPOLL_CTL_MOD, fd, &ev) < 0) fail("epoll_ctl(MOD)");
}

void EpollReactor::remove(int fd) {
  ::epoll_ctl(epfd_.get(), EPOLL_CTL_DEL, fd, nullptr);  // ignore ENOENT/EBADF
  handlers_.erase(fd);
}

void EpollReactor::post(std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> lk(post_mu_);
    posted_.push_back(std::move(fn));
  }
  std::uint64_t one = 1;
  // EAGAIN means the counter is already saturated, i.e. a wakeup is pending.
  ssize_t r = ::write(wake_.get(), &one, sizeof one);
  (void)r;
}

void EpollReactor::stop() {
  stop_.store(true);
  std::uint64_t one = 1;
  ssize_t r = ::write(wake_.get(), &one, sizeof one);
  (void)r;
}

void EpollReactor::drain_posted() {
  std::vector<std::function<void()>> batch;
  {
    std::lock_guard<std::mutex> lk(post_mu_);
    batch.swap(posted_);
  }
  for (auto& fn : batch) {
    try {
      fn();
    } catch (const std::exception& e) {
      LOG_ERROR("posted task threw: " << e.what());
    }
  }
}

void EpollReactor::run() {
  constexpr int kMaxEvents = 512;
  epoll_event events[kMaxEvents];
  while (!stop_.load()) {
    int n = ::epoll_wait(epfd_.get(), events, kMaxEvents, -1);
    if (n < 0) {
      if (errno == EINTR) continue;
      fail("epoll_wait");
    }
    for (int i = 0; i < n; ++i) {
      const int fd = events[i].data.fd;
      if (fd == wake_.get()) {
        std::uint64_t cnt;
        ssize_t r = ::read(wake_.get(), &cnt, sizeof cnt);
        (void)r;
        drain_posted();
        continue;
      }
      auto it = handlers_.find(fd);
      if (it == handlers_.end()) continue;  // removed earlier in this batch
      auto h = it->second;                  // keep alive if the handler removes itself
      try {
        (*h)(events[i].events);
      } catch (const std::exception& e) {
        LOG_ERROR("handler for fd " << fd << " threw: " << e.what());
      }
    }
  }
}

}  // namespace chat
