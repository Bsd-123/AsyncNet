# High-Performance Async Network Engine & Message Inspector
## Architecture & Implementation Plan — FROZEN

**Status: FROZEN.** This is the complete implementation baseline — architecture
and implementation-level defaults in one document. Do not continue expanding
or redesigning it. Only change it in response to an actual implementation
problem or a measured requirement encountered while building. Start M1.

**Project type:** learning + portfolio project (not a production library, not a research benchmark).

**Governing philosophy:**

```
Build
  ↓
Verify correctness
  ↓
Measure
  ↓
Identify bottlenecks
  ↓
Scale
  ↓
Optimize only when justified
  ↓
Measure again
```

The goal is a small but technically deep project demonstrating practical understanding of C++20, Linux, POSIX networking, TCP, epoll, asynchronous I/O, protocol parsing, concurrency, memory management, and performance engineering — not the most sophisticated framework possible. Every component must have a clear reason to exist; nothing is added because it would look advanced or impressive.

---

## 1. Architecture (frozen baseline)

```
Application Handler
        ↓
Message Inspector
        ↓
Protocol Parser
        ↓
Connection / Session
        ↓
EventLoop & Acceptor
        ↓
IOReactor → EpollReactor
        ↓
Linux / POSIX
```

Foundation throughout v1: STL containers, `std` smart pointers, ordinary `new`/`delete`. No custom allocator, no lock-free structures, no timer wheel — until a later milestone's measurements justify them (§9). No additional architectural layers unless a concrete implementation requirement justifies them.

---

## 2. MVP Scope

**In the MVP (M1 + M2):**
C++20 · Linux · POSIX non-blocking TCP · epoll (level-triggered, see §6) · single-threaded `EventLoop` · `Acceptor` · connection management · `RingBuffer` with vectored I/O (see §7) · length-prefixed binary protocol · streaming/partial-read-safe parser · `MessageInspector` · basic metrics · backpressure · idle-connection timeout (min-heap, see §5) · graceful shutdown · unit + integration/correctness tests.

**Explicitly not in the MVP:** custom allocator, lock-free queues, multi-threading, `TimerWheel`, `io_uring`, eBPF/XDP, `AF_PACKET`/`AF_XDP`, routing, NAT, firewall, embedded/firmware.

The MVP is fully functional and independently demoable after M2.

---

## 3. Connection Model

No linear state machine — a socket is readable and writable independently:

```
Connection
 └── ACTIVE
      ├── readable   (independent of writable)
      ├── writable   (independent of readable)
      └── closing    (draining / teardown)
```

Processing model (not a state transition):

```
read
 → parse
 → inspect
 → handle
 → enqueue response
 → write
```

Lifecycle (`ACTIVE` / `closing`) stays separate from I/O readiness. Readable and writable events remain independently manageable.

---

## 4. Backpressure (correctness requirement, part of the MVP)

```
write buffer
    │
    ├── below LOW  → EPOLLIN remains enabled
    ├── above HIGH → temporarily stop reading (deregister EPOLLIN) until it drains
    └── full       → explicit overflow policy — close-on-full as the default,
                      unless implementation/testing gives a strong reason to change it
```

The system must never allow a slow client to cause unbounded memory growth.

---

## 5. Idle Timeout Mechanism

A `std::priority_queue` of `(expiry_time, connection_id)` entries, min-ordered on `expiry_time` — not a linear sweep, and not a custom timer wheel. O(log N) push, O(log N)-amortized expiry check; still a standard-library structure, so it stays lightweight.

- `std::priority_queue` has no decrease-key: on activity, push a **new** entry rather than mutating the old one.
- Use lazy deletion: when an entry is popped, validate it against the connection's current last-activity value; discard silently if stale.

---

## 6. `IOReactor` — Minimal, Not a Universal Abstraction

```
IOReactor
    └── EpollReactor
```

Only `EpollReactor` touches epoll syscalls directly. The interface is minimal and based on what the current implementation actually needs: `register` / `modify` / `remove` / `poll`.

`IOReactor` is **not** designed as a universal abstraction spanning epoll and io_uring. epoll is readiness-based; io_uring is completion-based. If io_uring is ever added, it's acceptable — expected, even — to redesign or replace this abstraction at that time rather than have pre-fitted it now.

**Triggering mode (phased):**
- **M1–M2:** Level-Triggered (LT). Minimizes state-tracking bugs while correctness is the priority — a missed edge in ET mode can silently stall a connection, which is exactly the kind of subtle bug that eats disproportionate time during initial correctness work.
- **M5:** evaluate switching to Edge-Triggered (ET) against the M3/M4 baseline. ET's benefit — reduced `epoll_ctl` overhead — is real but secondary, and belongs where there's a baseline to justify it against.
- **From M1 on, regardless of mode:** every socket read/write loop must drain until `EAGAIN`/`EWOULDBLOCK`. This makes the eventual LT→ET switch at M5 a one-line flag change, not a rewrite.

---

## 7. `RingBuffer` I/O — Vectored I/O

Fill/drain the ring buffer with `readv()`/`writev()` over a POSIX `iovec` array (up to two entries, covering the buffer's wrap-around slices), instead of linearizing via a copy or issuing two separate syscalls. Applies from **M1** — this is the natural implementation of wraparound handling, not an M5-only optimization: a wrapped ring buffer has up to two contiguous slices, and vectored I/O handles both in one syscall without a memmove-based realignment step first.

---

## 8. `MessageInspector`

Operates on parsed application-level messages extracted from TCP streams — not raw Ethernet/IP packets. Name reflects that directly.

v1 responsibilities:
- message count
- bytes processed
- per-message-type statistics
- latency measurements
- optional logging

Not designed as a future NAT/routing/firewall framework. If those are ever required, they get their own abstraction, designed against their actual semantics at that time.

**Counter ownership:** counters live on the owning `EventLoop`/`Worker` instance, never in a shared/global singleton — starting at M2, even though M1–M3 only ever have one loop (so no behavioral difference yet, and no atomics needed). At M4, when multiple `EventLoop`s exist, this ownership boundary already gives isolation for free: counters become genuinely `thread_local`/per-`Worker`, non-atomic, with no cache-line bouncing or lock contention on the I/O path. Aggregation across workers happens only when a reporter asks for a snapshot.

---

## 9. Memory Management — Simple Until Measured Otherwise

Use STL containers, `std` smart pointers, ordinary `new`/`delete`. No custom allocator or memory pool built in advance.

At M3, measure: allocation frequency, allocation cost, memory usage, impact on throughput, impact on latency. Only introduce a custom allocator, slab pool, or other specialized mechanism if measurements demonstrate a meaningful bottleneck. "Zero allocations per request" is not a requirement.

**Measure first, optimize second.**

---

## 10. Concurrency

Start with exactly one `EventLoop`. After the single-threaded system is correct and has a performance baseline (M3), introduce:

```
Engine
 ├── Worker 0
 │    └── EventLoop
 ├── Worker 1
 │    └── EventLoop
 └── Worker N
      └── EventLoop
```

Called `Worker`/`WorkerContext` — independent reactors, not a generic task-oriented `ThreadPool`. Cross-thread mechanisms (MPSC queue, `eventfd`, explicit connection handoff, per-worker resources) are introduced only once multi-threading is actually being built.

**M4's justification is not limited to a measured bottleneck.** This is a learning + portfolio project, and multi-worker/reactor architecture is valuable in its own right — it teaches thread ownership, event-driven concurrency, cross-thread communication, `eventfd`, connection handoff, per-worker state, and scalability. So M4 may be justified either by measured scalability need **or** by its learning/portfolio value, once the single-threaded baseline (M1–M3) is stable. Actual performance *optimizations* (M5) still must be justified by measurements — that gate does not relax.

---

## 11. Milestone Roadmap

**M1 — Single-threaded async engine.** Non-blocking TCP, level-triggered epoll (§6), `EventLoop`, `Acceptor`, `Connection` (§3), `RingBuffer` with vectored I/O (§7), backpressure (§4), min-heap idle timeout (§5), graceful shutdown, unit tests, integration/correctness tests (§13).
*Goal: correctness. Deliverable: a reliable asynchronous TCP echo server.*

**M2 — Binary protocol + Message Inspector.** Length-prefixed binary framing, fragmented input handling, multiple messages in one read, protocol parser, `MessageInspector` with per-`EventLoop`-owned counters (§8), basic metrics.
*Deliverable: a complete, small, fully demoable MVP.*

**M3 — Performance baseline.** No optimization yet. Build the load generator (§12) and measure: throughput, messages/sec, latency, p50/p95/p99, active connections, CPU utilization, memory usage, allocation behavior.
*Deliverable: a baseline performance report identifying actual bottlenecks.*

**M4 — Multi-worker/reactor scaling.** Multiple `Worker`/`EventLoop`s, per-worker resources (including `MessageInspector` counters becoming genuinely thread-local, §8), cross-thread communication (MPSC queue, `eventfd`), connection distribution. Compare `SO_REUSEPORT` vs. explicit accept + handoff if both are useful for learning and benchmarking. Justified by measured need or learning value (§10).
*Deliverable: measured scaling behavior as worker count increases.*

**M5 — Optimization.** Only optimize what M3/M4 measurements justify: buffer management, allocation reduction, batching, syscall reduction, cache/locality improvements, custom allocator only if justified, LT→ET epoll evaluation (§6). Every optimization gets a before/after measurement using the same load generator (§12).
*Deliverable: documented performance improvements, or a documented conclusion that further optimization isn't worthwhile.*

**M6 — Hardening.** ASan, UBSan, TSan, parser fuzzing, stress testing, documentation, architecture documentation, final benchmark results.
*Deliverable: polished `v1.0`.*

Each milestone is independently stoppable and demoable.

---

## 12. Load Generator (built at M3, reused through M5)

A small, dedicated tool — not a separate networking framework. Controls:
- number of connections
- message rate
- payload size
- test duration

Produces: throughput, latency (p50/p95/p99), CPU usage, memory usage, connection count.

**Isolation is mandatory for valid measurements.** For every M3 baseline run and every M5 before/after comparison: pin the load generator to CPU cores dedicated to it (`taskset`/cgroup), separate from the cores serving the engine — or run it on a separate machine/VM entirely. Unpinned, contending measurements would quietly invalidate the "measure first, optimize second" discipline the whole M3→M5 sequence depends on.

---

## 13. Correctness Test Matrix (M1)

More valuable than additional abstractions — these are the actual failure modes of async networking code:

- partial header
- partial payload
- multiple messages in one read
- partial writes
- client disconnect during a message
- slow reader
- slow writer
- idle timeout
- `EAGAIN`
- `EINTR`
- graceful shutdown with active connections
- backpressure watermark transitions
- write-buffer overflow behavior

---

## 14. Explicitly Deferred (documented, not built)

`io_uring`, eBPF, XDP/AF_XDP, `AF_PACKET`, custom TCP/IP stack, routing, NAT, firewall, embedded/firmware. Revisit only if there's a concrete reason to continue expanding the project.
