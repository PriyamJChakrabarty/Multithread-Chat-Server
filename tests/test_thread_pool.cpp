#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include "chat/thread_pool.hpp"

using namespace chat;

TEST(ThreadPool, RunsAllTasks) {
  std::atomic<int> n{0};
  {
    ThreadPool p(4);
    for (int i = 0; i < 1000; ++i) ASSERT_TRUE(p.submit([&] { ++n; }));
    p.stop();  // drains
  }
  EXPECT_EQ(n, 1000);
}

TEST(ThreadPool, StopDrainsQueuedWork) {
  std::atomic<int> n{0};
  ThreadPool p(1);
  std::promise<void> gate;
  auto fut = gate.get_future().share();
  p.submit([fut] { fut.wait(); });  // block the only worker
  for (int i = 0; i < 50; ++i) p.submit([&] { ++n; });
  std::thread stopper([&] { p.stop(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  gate.set_value();
  stopper.join();
  EXPECT_EQ(n, 50);
}

TEST(ThreadPool, SubmitAfterStopIsRejected) {
  ThreadPool p(2);
  p.stop();
  EXPECT_FALSE(p.submit([] {}));
  p.stop();  // idempotent
}

TEST(ThreadPool, ThrowingTaskDoesNotKillWorker) {
  ThreadPool p(1);
  p.submit([] { throw std::runtime_error("boom"); });
  std::promise<int> done;
  p.submit([&] { done.set_value(42); });
  EXPECT_EQ(done.get_future().get(), 42);
}

TEST(ThreadPool, ConcurrentSubmitters) {
  std::atomic<int> n{0};
  ThreadPool p(4);
  std::vector<std::thread> ts;
  for (int t = 0; t < 8; ++t)
    ts.emplace_back([&] {
      for (int i = 0; i < 500; ++i) p.submit([&] { ++n; });
    });
  for (auto& t : ts) t.join();
  p.stop();
  EXPECT_EQ(n, 4000);
}
