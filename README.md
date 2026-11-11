# epoll-chat

A multithreaded TCP/UDP chat server in C++17 for Linux. It uses `epoll` for network I/O and a thread pool for message processing. A thread-per-client version of the same server is included, so the two designs can be compared.

- Edge-triggered (or level-triggered) `epoll`, non-blocking sockets
- Reactor thread(s) do I/O only; a worker pool handles commands and broadcasts
- Length-prefixed TCP framing, partial read/write handling, write backpressure
- UDP datagrams with sequence numbers for loss and reorder detection
- Rooms, nicknames, private messages
- Idle timeouts, connection limits, graceful shutdown on `SIGINT`/`SIGTERM`
- Unit and integration tests, run under ASan/UBSan and TSan

Linux only (uses `epoll`, `timerfd`, `signalfd`, `eventfd`). On Windows, use WSL.

## Build

A C++17 compiler (g++ 9+ or clang), CMake 3.16+, and internet access is required the first time (CMake downloads GoogleTest if it is not installed).

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Everything is built with `-Wall -Wextra -Wpedantic -Werror`.

Binaries in `build/`:

| Binary | What it is |
|---|---|
| `chat_server` | Main server: epoll + thread pool |
| `chat_server_threaded` | Baseline: one thread per client |
| `chat_client` | Command-line client |
| `echo_blocking`, `echo_threaded`, `echo_epoll` | The first three learning milestones |
| `chat_tests` | Test suite |

## Run

```bash
./build/chat_server --port 9000
```

In other terminals:

```bash
./build/chat_client --port 9000 --nick alice
./build/chat_client --port 9000 --nick bob
```

Type a line to talk to your room. Commands:

| Command | What it does |
|---|---|
| `/nick <name>` | Change your nickname |
| `/join <room>` | Join (or create) a room |
| `/who` | List users in your room |
| `/rooms` | List rooms |
| `/msg <nick> <text>` | Private message |
| `/ping` | (client) send a UDP ping, print the round trip time |
| `/udp <text>` | (client) send a message to your room over UDP |
| `/quit` | Leave |

### Server options

```
--port N         listen port (default 9000, TCP and UDP)
--host IP        bind address
--workers N      worker threads (default: CPU count)
--io-threads N   epoll loops, each with its own SO_REUSEPORT listener (default 1)
--max-conns N    connection limit (default 20000)
--idle SECONDS   close silent clients after this long, 0 = never (default 300)
--max-frame B    largest message in bytes (default 65536)
--max-out B      per-client unsent data before the client is dropped (default 1 MiB)
--lt             level-triggered instead of edge-triggered
--no-udp         disable UDP
--log-level L    debug | info | warn | error
```

For thousands of connections, raise the file descriptor limit first: `ulimit -n 65536`.

## How it works

```
        Clients (TCP + UDP)
              |
   +----------v-----------+
   |  epoll reactor       |  non-blocking sockets, edge-triggered
   |  (accept, read,      |  one or more I/O threads
   |   flush writes)      |
   +----------+-----------+
              | complete messages
   +----------v-----------+
   |  Thread pool         |  N workers, mutex + condition_variable
   |  (parse, route,      |
   |   broadcast)         |
   +----------+-----------+
              |
   Shared state: client registry and rooms (one mutex)
```

- Only the reactor thread reads, writes or closes a client socket. Workers never touch a file descriptor, so two threads can never use the same socket at the same time.
- Each connection has its own inbox, drained by one worker at a time, so one client's messages are handled in order.
- Workers send replies by putting frames in a per-connection queue and waking the reactor, which does the actual `send()`.
- The chat logic (`ChatHub`) knows nothing about sockets, so both servers share it.

More detail is in [docs/design.md](docs/design.md) (locking rules, shutdown, limits). The wire format is in [docs/protocol.md](docs/protocol.md).

### Source layout

```
include/chat/   headers
src/            server code
src/milestones/ the three echo servers
client/         command-line client
tests/          GoogleTest unit and integration tests
bench/          load generator and benchmark scripts
docs/           protocol and design notes
```

## Tests

```bash
./build/chat_tests
```

The suite has 60+ tests: framing edge cases (split and packed messages, oversize frames), the UDP codec and loss tracker, the thread pool, the chat commands, and integration tests against a real server. These cover partial writes, slow clients, idle timeouts, the connection limit, a client dropping mid-write, rapid connect/disconnect stress, and clean shutdown. The server tests run in both edge- and level-triggered mode.

Sanitizer builds:

```bash
cmake -S . -B build-asan -DCHAT_SANITIZE=address -DCMAKE_BUILD_TYPE=Debug
cmake --build build-asan -j && ./build-asan/chat_tests

cmake -S . -B build-tsan -DCHAT_SANITIZE=thread -DCMAKE_BUILD_TYPE=Debug
cmake --build build-tsan -j && setarch "$(uname -m)" -R ./build-tsan/chat_tests
```

`setarch -R` turns off address randomisation, which ThreadSanitizer needs on some newer kernels (including WSL2).

## Benchmarks

`bench/loadtest.py` opens N connections, puts clients in rooms of 10, and measures how long each client waits to see its own message come back. `bench/run_bench.py` runs both servers at several sizes and records latency, CPU and memory of the server process.

```bash
python3 bench/run_bench.py --build-dir build --procs 8 --rate 1
python3 bench/plot.py        # needs matplotlib
```

### Results

Machine: WSL2, 20 cores, 16 GB RAM, loopback. Load: 1 message per second per client, 10 clients per room, 4 worker threads. The epoll server here ran with **one** I/O thread.

| Connections | Server | p50 | p99 | CPU (100% = 1 core) | Peak memory |
|---|---|---|---|---|---|
| 1,000 | thread-per-client | 1.3 ms | 3.2 ms | 23% | 30 MB |
| 1,000 | epoll (1 I/O thread) | 1.4 ms | 3.6 ms | 35% | 95 MB |
| 2,000 | thread-per-client | 0.9 ms | 3.2 ms | 170% | 64 MB |
| 2,000 | epoll (1 I/O thread) | 1.8 ms | 6.6 ms | 91% | 394 MB |
| 5,000 | thread-per-client | 9.1 ms | 34.6 ms | 142% | 134 MB |
| 5,000 | epoll (1 I/O thread) | no replies | no replies | 95% | 1.1 GB |
| 10,000 | thread-per-client | 25.1 ms | 188.8 ms | 503% | 279 MB |
| 10,000 | epoll (1 I/O thread) | no replies | no replies | 95% | 1.6 GB |

Saturated (200 clients, each sends the next message when its last one returns): thread-per-client 16,940 msgs/s using 280% CPU; epoll 17,282 msgs/s using 98% CPU.

What this shows:

- Both servers accepted all 10,000 connections.
- With a single I/O thread, epoll **does not keep up** above a few thousand clients here. That one thread used a full core (loopback `send()` does the receiver's TCP work inline), and queued data piled up in memory. It uses much less CPU than the threaded server at the same load, but it was not faster.
- This is why `--io-threads` exists: it runs several epoll loops, each with its own listener. **Results with multiple I/O threads are not in this table yet**; a sweep with `--io-threads 4` has not been completed. Run the benchmark yourself before quoting numbers.
- The load generator is Python and runs on the same machine, so high-end latencies include its own delay. Treat the numbers as relative, not absolute.

Raw data from this run is in `bench/results/results_single_io_thread.csv`.

## Known issues

- The multi-loop integration test (`MultiLoop.*`, `--io-threads 4`) is flaky: in some full-suite runs a few clients missed a broadcast. The cause is not found yet. All other tests passed consistently, and ASan/UBSan and TSan runs were clean on the original suite. The multi-loop path has not been re-checked under the sanitizers.
- IPv4 only. No TLS.
- UDP is best effort. It is counted and tracked, but lost datagrams are not resent.

## Not done

From the brief's stretch list: reliable UDP (ACK/retransmit), a jitter buffer, TLS, and a lock-free queue for the pool. The UDP header already has a sequence number and timestamp so these can be added later without changing the format.

## Docker

```bash
docker build -t epoll-chat .
docker run --ulimit nofile=65536:65536 -p 9000:9000 -p 9000:9000/udp epoll-chat
```

## CI

GitHub Actions builds and tests on every push in three configurations: Release, ASan+UBSan, and TSan.
