#pragma once

#include <memory>

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

    // Requests the loop to exit. Safe to call from within a handler running
    // on the loop's own thread: the batch of handlers the reactor already
    // picked up always finishes running first -- this only prevents the
    // *next* poll() call. Calling this from another thread or a signal
    // handler does NOT wake a poll() that's already blocked; that needs an
    // fd-based wakeup (see the eventfd-driven graceful shutdown mechanism,
    // M1 plan step 9).
    void stop();

    IOReactor& reactor() { return *reactor_; }

private:
    std::unique_ptr<IOReactor> reactor_;
    bool stopRequested_ = false;
};

} // namespace asyncnet
