# Protocol specification

## TCP: length-prefixed text frames

TCP is a byte stream with no message boundaries, so every message is wrapped in
a frame:

```
+----------------------+----------------------------+
| length  (4 bytes,    | payload (`length` bytes,   |
| big-endian, unsigned)| UTF-8 text, no terminator) |
+----------------------+----------------------------+
```

* `length` counts the payload only (not the 4-byte header). Zero is legal.
* Maximum payload: server option `--max-frame` (default 65 536). A header that
  announces more is a protocol violation: the stream cannot be resynchronised,
  so the server closes the connection (`closed_protocol` counter).
* Receivers must cope with a frame split across any number of reads and with
  many frames arriving in one read (`FrameDecoder` is the reference, tested
  byte-at-a-time and with 100 packed frames in `tests/test_codec.cpp`).

### Client -> server payloads

A payload is one line of text. If it starts with `/` it is a command, otherwise
it is a chat message to the sender's current room.

| Input | Effect |
|---|---|
| `text` | Broadcast `[room] nick: text` to everyone in the room, **including the sender** (doubles as an acknowledgement and is what the load tester times). |
| `/nick <name>` | Rename. 1-16 chars of `[A-Za-z0-9_-]`, must be unique. |
| `/join <room>` | Move to a room (created on demand, deleted when empty). 1-24 chars of `[A-Za-z0-9_-]`. New clients start in `lobby`. |
| `/who` | List nicknames in the current room. |
| `/rooms` | List rooms with member counts. |
| `/msg <nick> <text>` (alias `/w`) | Private message. |
| `/quit` | Server replies `* bye`, flushes, then closes. |
| `/help` | Command summary. |

### Server -> client payloads

| Form | Meaning |
|---|---|
| `* <text>` | System notice (welcome, join/leave, errors, `/who` output). |
| `[<room>] <nick>: <text>` | Room message. |
| `[<room>] (udp) <nick>: <text>` | Room message that originated as a UDP datagram. |
| `[pm from <nick>] <text>` / `[pm to <nick>] <text>` | Private message and its confirmation. |

## UDP: RTP-like datagrams

The server binds a UDP socket on the **same port number** as the TCP listener.
Each datagram has a 16-byte big-endian header, then an optional payload:

```
 0               1               2               3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------------------------+---------------+---------------+
|        magic = 0xC47A           |     type      |     flags     |
+---------------------------------+---------------+---------------+
|                       sequence number (u32)                     |
+-----------------------------------------------------------------+
|                       timestamp, microseconds (u64)             |
|                                                                 |
+-----------------------------------------------------------------+
|                          payload ...                            |
```

| type | name | behaviour |
|---|---|---|
| 1 | `Ping` | Presence ping. Server answers with a `Pong` that echoes `seq`, `timestamp` and payload, so the sender computes RTT as `now - timestamp`. |
| 2 | `Pong` | Reply to a ping (ignored by the server). |
| 3 | `Message` | Payload is `"<nick> <text>"`. If `<nick>` is a connected TCP user, `text` is broadcast to that user's room as a `(udp)` message. Otherwise it is dropped. |

Datagrams with a bad magic, unknown type or short header are counted
(`udp_invalid`) and ignored.

### Loss and reordering detection

The server keeps a `SequenceTracker` per source address. It uses the signed
32-bit distance to the highest sequence number seen (so wraparound works):

* `seq > highest` -> the skipped numbers become *missing*;
* a later packet that fills a missing slot is **reordered** (and is no longer
  counted as lost);
* a repeat of a number already seen is a **duplicate**;
* *lost* = numbers still missing (at most 1024 outstanding gaps are tracked;
  older ones are written off as lost).

Counters are exposed through `ChatServer::stats()` and logged at shutdown.

UDP is unreliable by design: a lost `Message` is simply gone. TCP carries the
authoritative chat; UDP is for lightweight/presence traffic.
