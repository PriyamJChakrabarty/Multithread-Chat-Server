cmake_minimum_required(VERSION 3.16)
project(epoll_chat LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

option(CHAT_BUILD_TESTS "Build unit/integration tests (needs GoogleTest)" ON)
set(CHAT_SANITIZE "" CACHE STRING "Sanitizer: thread | address | (empty)")

if(CHAT_SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g -O1)
  add_link_options(-fsanitize=thread)
elseif(CHAT_SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g -O1)
  add_link_options(-fsanitize=address,undefined)
elseif(CHAT_SANITIZE)
  message(FATAL_ERROR "CHAT_SANITIZE must be 'thread', 'address' or empty")
endif()

find_package(Threads REQUIRED)

add_library(chatcore STATIC
  src/net.cpp
  src/codec.cpp
  src/thread_pool.cpp
  src/chat_hub.cpp
  src/epoll_reactor.cpp
  src/connection.cpp
  src/epoll_server.cpp
  src/threaded_server.cpp
  src/logger.cpp
  src/cli.cpp)
target_include_directories(chatcore PUBLIC include)
target_link_libraries(chatcore PUBLIC Threads::Threads)
# Strict warnings apply to our targets only (propagated PUBLIC), not to GoogleTest.
target_compile_options(chatcore PUBLIC -Wall -Wextra -Wpedantic -Werror)

# --- Servers -----------------------------------------------------------------
add_executable(chat_server src/main_epoll.cpp)
target_link_libraries(chat_server PRIVATE chatcore)

add_executable(chat_server_threaded src/main_threaded.cpp)
target_link_libraries(chat_server_threaded PRIVATE chatcore)

# --- Milestones 1-3: echo servers --------------------------------------------
foreach(m echo_blocking echo_threaded echo_epoll)
  add_executable(${m} src/milestones/${m}.cpp)
  target_link_libraries(${m} PRIVATE chatcore)
endforeach()

# --- Client ------------------------------------------------------------------
add_executable(chat_client client/chat_client.cpp)
target_link_libraries(chat_client PRIVATE chatcore)

# --- Tests -------------------------------------------------------------------
if(CHAT_BUILD_TESTS)
  enable_testing()
  find_package(GTest QUIET)
  if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
  endif()
  add_executable(chat_tests
    tests/test_codec.cpp
    tests/test_thread_pool.cpp
    tests/test_hub.cpp
    tests/test_server.cpp)
  target_link_libraries(chat_tests PRIVATE chatcore GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(chat_tests DISCOVERY_MODE PRE_TEST)
endif()
