#include "chat/thread_pool.hpp"

#include <exception>

#include "chat/logger.hpp"

namespace chat {

ThreadPool::ThreadPool(std::size_t threads) {
  if (threads == 0) threads = 1;
  workers_.reserve(threads);
  for (std::size_t i = 0; i < threads; ++i) workers_.emplace_back([this] { worker_loop(); });
}

bool ThreadPool::submit(Task task) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopping_) return false;
    queue_.push_back(std::move(task));
  }
  cv_.notify_one();  // notify outside the lock: the woken worker can take it at once
  return true;
}

void ThreadPool::stop() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopping_ && workers_.empty()) return;
    stopping_ = true;
  }
  cv_.notify_all();
  for (auto& t : workers_)
    if (t.joinable()) t.join();
  workers_.clear();
}

std::size_t ThreadPool::pending() const {
  std::lock_guard<std::mutex> lk(mu_);
  return queue_.size();
}

void ThreadPool::worker_loop() {
  for (;;) {
    Task task;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait(lk, [this] { return stopping_ || !queue_.empty(); });
      if (queue_.empty()) return;  // stopping_ and fully drained
      task = std::move(queue_.front());
      queue_.pop_front();
    }
    // Run outside the lock; a throwing task must not kill the worker.
    try {
      task();
    } catch (const std::exception& e) {
      LOG_ERROR("thread pool task threw: " << e.what());
    } catch (...) {
      LOG_ERROR("thread pool task threw an unknown exception");
    }
  }
}

}  // namespace chat
