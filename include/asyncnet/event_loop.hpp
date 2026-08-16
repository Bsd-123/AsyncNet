#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "asyncnet/io_reactor.hpp"

namespace asyncnet {

// Owns the reactor and drives the dispatch loop. Lifecycle (run/stop) stays
// separate from I/O readiness -- see architecture doc §3.
class EventLoop {
public:
    EventLoop();
    explicit EventLoop(std::unique_ptr<IOReactor> reactor);

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // Blocks, dispatching ready I/O events, until stop() takes effect.
    // Blocks inside the reactor's poll() rather than busy-spinning.
    void run();

    // Runs one poll() cycle (dispatching ready I/O events) followed by any
    // actions queued via post() during that cycle. run() is just this in a
    // loop; exposed directly so tests/components can drive a single cycle
    // without needing stop() triggered from inside a handler.
    void runOnce(int timeoutMs = -1);

    // Requests the loop to exit. Safe to call from within a handler running
    // on the loop's own thread: the batch of handlers the reactor already
    // picked up always finishes running first -- this only prevents the
    // *next* poll() call. Calling this from another thread or a signal
    // handler does NOT wake a poll() that's already blocked; that needs an
    // fd-based wakeup (see the eventfd-driven graceful shutdown mechanism,
    // M1 plan step 9).
    void stop();

    // Queues action to run once the current dispatch batch has fully
    // finished. Needed so components can close a fd safely: removing a fd
    // from epoll (IOReactor::removeFd) stops further dispatch, but doesn't
    // by itself make the fd number safe to reuse -- that requires close()
    // to wait until every handler in the current batch has run, so the OS
    // can't hand the same fd number to e.g. a new accept() mid-batch.
    void post(std::function<void()> action);

    IOReactor& reactor() { return *reactor_; }

private:
    std::unique_ptr<IOReactor> reactor_;
    std::vector<std::function<void()>> deferredActions_;
    bool stopRequested_ = false;
};

} // namespace asyncnet
