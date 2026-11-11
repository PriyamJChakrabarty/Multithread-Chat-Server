# Design decisions

## Threading model

```
reactor thread (1)                          worker threads (N)
------------------                          ------------------
accept4 / EMFILE shedding                   pop from per-connection inbox
recv until EAGAIN (ET)                      ChatHub::handle_line
FrameDecoder -> complete messages           enqueue reply frames on recipients'
push to Connection inbox  ----------------> out queues, request a flush
flush out queues, EPOLLOUT  <-------------- (reactor_.post(), wakes via eventfd)
timerfd idle sweep, signalfd, UDP recv
```

**The reactor is the only thread that ever reads, writes or closes a client
socket.** Workers never touch an fd. That is how "two workers never handle the
same socket" is guaranteed without `EPOLLONESHOT`: ownership of the fd is
structural, not negotiated. (`EPOLLONESHOT` would be the alternative if
workers did the reads themselves.)

### Per-connection ordering

Messages from one client must be processed in order, but the pool has many
workers. `Connection` therefore has an inbox plus a `scheduled` flag:

* reactor pushes a message; if no worker owns the inbox it sets `scheduled`
  and submits one `process(conn)` task;
* the worker drains the inbox (up to 32 messages, then re-queues itself so one
  chatty client cannot monopolise a worker) and clears `scheduled` only when it
  observes the inbox empty **under the inbox lock**, so a concurrent push can
  never be lost.

`tests/test_server.cpp: MessageOrderIsPreservedPerClient` checks this with 500
messages through 3 workers.

### Connection lifetime

* The reactor owns the connection (map + epoll handler hold `shared_ptr`).
* A session's `send` callback holds only a `weak_ptr<Connection>`: a worker
  that wants to deliver to a client that has just disconnected gets a null
  `lock()` and skips it.
* `close_connection` removes the fd from epoll, closes it (so the number can be
  reused immediately), erases the connection and unregisters the session. After
  that nobody can reach the socket; stragglers in worker queues are ignored via
  the `closed` flags.

## Lock ordering

There are only leaf locks, and none is held across a syscall or a callback:

| Lock | Protects | Rules |
|---|---|---|
| `ChatHub::mu_` | sessions, nicks, rooms, each session's nick/room | **Never held while calling `Session::send`.** Handlers copy the recipient list under the lock, release, then deliver. Every mutator re-checks `session->closed` under the lock so a command queued before a disconnect cannot resurrect the session's entries. |
| `Connection::in_mu_` | inbox + `scheduled` | leaf |
| `Connection::out_mu_` | outbound frame queue | leaf; reactor swaps the whole queue out under the lock, then does the `send()` unlocked |
| `ThreadPool::mu_` | task queue | tasks run unlocked |
| `EpollReactor::post_mu_` | posted closures | leaf |

Order if two were ever needed: hub -> connection -> reactor/pool. In practice
nothing nests them, which is what the TSan runs confirm.

## Backpressure and limits

| Threat | Defence |
|---|---|
| Slow reader, unbounded write queue | `queued_bytes` is reserved atomically in `enqueue`; over `--max-out` (1 MiB) the client is disconnected (`closed_overflow`). Healthy clients are unaffected (tested). |
| Client floods faster than workers process | Inbox capped at 1024 messages -> disconnect. |
| Giant message | Frame length validated before any allocation; over `--max-frame` -> disconnect. |
| Connection exhaustion | `--max-conns`; extras are accepted and closed immediately so they are not left in the backlog. |
| `EMFILE`/`ENFILE` on accept | A spare fd (`/dev/null`) is released, one pending connection is accepted and dropped, the spare is re-opened. Without this a level-triggered listener would spin at 100% CPU. |
| Silent/dead peers | `timerfd` ticks once a second; clients silent longer than `--idle` are closed. `SO_KEEPALIVE` on too. |
| Dead peer mid-write | `SIGPIPE` ignored and `MSG_NOSIGNAL` used; `EPIPE`/`ECONNRESET` simply close the connection. |

## Edge- vs level-triggered

`--lt` switches the server (listener, clients and UDP) to level-triggered
mode. The same handler code serves both: in edge mode accept/recv/recvfrom
loop until `EAGAIN`; in level mode they do one bounded batch and rely on epoll
to report leftovers. The whole integration suite runs in both modes.

## Shutdown

`SIGINT`/`SIGTERM` are blocked before any thread is created (so every worker
inherits the mask) and consumed through a `signalfd` in the reactor. The
reactor stops, closes every connection, then `ThreadPool::stop()` lets queued
tasks finish and joins the workers.

## Thread-per-client baseline

`chat_server_threaded` shares `ChatHub`, `FrameDecoder` and the protocol, so
the benchmark compares architectures and not features. Its known weaknesses
(a broadcast does a blocking `send` into each recipient's socket under a
per-client mutex, bounded only by `SO_SNDTIMEO`; one OS thread per client) are
deliberate: they are what the epoll design removes.

## Not implemented (scope)

IPv6, TLS, reliable-UDP, jitter buffer and a lock-free pool queue (the brief's
"stretch goals"). The UDP header was designed RTP-like (sequence number +
timestamp) so a jitter buffer or ACK/retransmit layer can be added on top of
`SequenceTracker` without changing the wire format.
