# AsyncNet

A high-performance async network engine and message inspector, built as a
learning + portfolio project. C++20, Linux, POSIX non-blocking TCP, epoll.

Architecture and milestone roadmap: see
[`async-net-engine-architecture-FINAL.md`](async-net-engine-architecture-FINAL.md)
(frozen baseline). Current milestone plan:
[`M1-implementation-plan_1.md`](M1-implementation-plan_1.md).

**Status:** M1 in progress — single-threaded async TCP echo server.

## Building

Requires a C++20 compiler, CMake >= 3.20, Ninja, and GoogleTest
(`libgtest-dev` / `libgmock-dev` on Debian/Ubuntu).

```bash
cmake --preset debug          # or: asan-ubsan, release
cmake --build --preset debug
ctest --preset debug
```
