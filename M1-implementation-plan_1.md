# M1 Implementation Plan — Single-Threaded Async TCP Echo Server

Scope only. References the frozen architecture (`async-net-engine-architecture-FINAL.md`); does not modify it. Goal: a correct, single-threaded, non-blocking TCP echo server on level-triggered epoll.

---

**1. Project / Build Setup**
Implement: CMake project, C++20, `Debug`/`ASan+UBSan`/`Release` presets, `src/`/`include/`/`tests/` layout, GoogleTest as a dev dependency, minimal logger, CI build+test skeleton.
Correctness: configure sanitizer builds now — retrofitting later is painful.
Before moving on: all three configs build clean; empty test suite runs in CI.

**2. `EpollReactor`**
Implement: `IOReactor` interface (`register`/`modify`/`remove`/`poll`), `EpollReactor` wrapping `epoll_create1`/`epoll_ctl`/`epoll_wait`, level-triggered registration, callback dispatch per ready fd.
Correctness: `epoll_wait` must retry on `EINTR`, not propagate it as an error; fds must never be used after being closed/deregistered.
Before moving on: unit test using a `socketpair`/pipe — register, write, confirm `poll()` reports readiness and fires the callback; confirm an injected `EINTR` doesn't crash the loop.

**3. `EventLoop`**
Implement: owns one `EpollReactor`; `run()`/`stop()`; dispatches ready events to registered handlers.
Correctness: `stop()` called from inside a callback must exit cleanly after the current dispatch pass, not mid-iteration; block in `epoll_wait` rather than busy-spinning.
Before moving on: loop starts/stops cleanly under test; stop-from-callback doesn't crash or hang.

**4. `Acceptor`**
Implement: non-blocking listen socket (`SO_REUSEADDR`), register for `EPOLLIN`, `accept4()` looped until `EAGAIN`, hand new fds to `Connection` creation.
Correctness: must drain all pending connections per wakeup, not just one; `ECONNABORTED` is a normal transient condition, log and continue the loop. `EMFILE`/`ENFILE` (fd exhaustion) is a distinct case, not just another transient error to swallow — stop accepting for this wakeup rather than busy-looping on the same failure, log it, and document the chosen policy (retry on the next wakeup once fds free up) rather than treating it identically to `ECONNABORTED`.
Before moving on: a burst of simultaneous client connects is all accepted from a single wakeup; a simulated `EMFILE` is handled per the documented policy without crashing or busy-looping; other accept errors are logged and non-fatal.

**5. `RingBuffer`**
Implement: circular byte buffer; fill via `readv()`, drain via `writev()`, using up to two `iovec` entries to cover the wrap.
Correctness: correct handling at exact-boundary wraps, buffer-full, buffer-empty; partial `readv`/`writev` completions must update head/tail correctly; a 0-byte read (peer closed) must be distinguished from `EAGAIN`.
Before moving on: unit tests covering wrap-around, full/empty transitions, and boundary cases, plus integration tests using real sockets to verify partial `readv()`/`writev()` behavior.

**6. `Connection`**
Implement: per-fd object owning read/write `RingBuffer`s; `ACTIVE` with independent `readable`/`writable`/`closing`; `onReadable()`/`onWritable()`. M1's application logic is a plain echo (write back exactly what was read — no parsing yet, that's M2).
Correctness: readable and writable handled independently, never serialized; every syscall path distinguishes real errors from `EAGAIN`/`EWOULDBLOCK`/`EINTR`; fd closed exactly once, ownership never ambiguous.
Before moving on: one client's data survives multiple partial reads and is echoed correctly; a mid-transfer disconnect is cleaned up without crashing or leaking the fd; simultaneous readable+writable on the same connection is handled correctly.

**7. Backpressure**
Implement: bounded write buffer with LOW/HIGH watermarks; above HIGH, deregister `EPOLLIN`; below LOW, re-register it; buffer-full policy is close-on-full.
Correctness: watermark decisions are based on write-buffer occupancy only; deregistering `EPOLLIN` must not affect `EPOLLOUT` handling (still drains independently); use hysteresis (two thresholds) to avoid flapping on a single threshold.
Before moving on: a client that stops reading its echoes causes `EPOLLIN` to be deregistered before memory grows unbounded, and reading resumes once the client drains; a full-buffer close doesn't affect other connections.

**8. Idle Timeout**
Implement: `std::priority_queue` of `(expiry_time, connection_id)`, min-ordered; on activity, push a fresh entry; on expiry check, pop and validate against the connection's current last-activity value, discarding stale entries (lazy deletion).
Correctness: stale heap entries from earlier refreshes must never trigger a false close; no per-connection `timerfd`. The queue is not required to stay bounded in size — under lazy deletion, stale entries may temporarily accumulate for a connection with frequent activity. The only requirement is that stale entries are eventually discarded (when popped and found invalid) and never cause an active connection to be incorrectly expired.
Before moving on: an idle connection is closed at timeout; a connection kept active by periodic traffic is never closed despite stale entries sitting in the queue; popping and discarding a stale entry never triggers a close.

**9. Graceful Shutdown**
Implement: `SIGINT`/`SIGTERM` handler sets an atomic flag and wakes the loop via a Linux `eventfd` — the handler does only an async-signal-safe `write()` to the `eventfd`'s fd, nothing else; the `EventLoop` has the `eventfd` registered like any other fd and reacts to it on its own thread. On shutdown, stop accepting immediately, attempt to flush in-flight write buffers within a bounded grace period, then close remaining connections and exit.
Correctness: signal handler must be async-signal-safe; no fd leaks; grace period must be bounded — never hang indefinitely on a slow client.
Before moving on: `SIGINT` with active connections exits within the bounded window, no leaked fds (check under ASan/valgrind), no new connections accepted after the signal.

**10. Testing & Validation**
Implement: the full correctness matrix — partial reads/writes, multiple reads-worth of data in one wakeup, client disconnect mid-transfer, slow reader, slow writer, idle timeout, `EAGAIN`, `EINTR`, graceful shutdown with active connections, backpressure watermark transitions, write-buffer overflow — plus a concurrent-clients correctness test (100+ simultaneous connections on the single thread, checked for per-connection correctness under concurrency). This is an integration/correctness test, not a performance target — scalability and high-load benchmarking belong to M3.
Correctness: cover both unit level (`RingBuffer`, `EpollReactor` in isolation) and integration level (real loopback TCP clients against a running `EventLoop`).
Before considering M1 done: entire suite passes reliably (no flakiness) under `ASan`+`UBSan`.

---

## M1 Definition of Done

- Builds clean in `Debug`, `ASan+UBSan`, and `Release`.
- Correctly echoes data for 100+ simultaneous clients on a single thread, verified as a correctness/integration test — not a throughput or performance claim; scalability and high-load benchmarking are M3's job.
- Handles partial reads/writes, `EAGAIN`/`EWOULDBLOCK`, and `EINTR` without crashing or corrupting connection state.
- `RingBuffer` wrap-around via `readv`/`writev` verified correct by unit tests, including partial-completion cases.
- Backpressure watermarks correctly stop/resume reading under a slow client; overflow triggers close-on-full without affecting other connections.
- Idle connections are detected and closed by the priority-queue timeout mechanism within the configured window; active connections are never falsely closed by stale (lazily-deleted) queue entries — queue size itself is not required to stay bounded.
- `EMFILE`/`ENFILE` during accept is handled per a documented policy (not busy-looped, not conflated with transient errors like `ECONNABORTED`).
- `SIGINT`/`SIGTERM` triggers graceful shutdown via `eventfd`-based wakeup: stops accepting, bounds the drain of in-flight writes, exits with no fd leaks or crashes (ASan/valgrind clean).
- Full correctness test matrix (§10) passes reliably under sanitizers.
- No multi-threading, custom allocators, lock-free structures, timer wheel, `io_uring`, eBPF/XDP, or extra architectural layers present anywhere in the implementation.
