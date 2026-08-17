#pragma once

#include <functional>

#include "asyncnet/event_loop.hpp"

namespace asyncnet {

// Bridges SIGINT/SIGTERM into the EventLoop via an eventfd. The signal
// handler itself does only an async-signal-safe write() to that fd --
// nothing else runs in signal context. The EventLoop reacts to the eventfd
// like any other fd, on its own thread, invoking onShutdownRequested from
// there (M1 plan step 9).
class ShutdownSignal {
public:
    using Handler = std::function<void()>;

    ShutdownSignal(EventLoop& loop, Handler onShutdownRequested);
    ~ShutdownSignal();

    ShutdownSignal(const ShutdownSignal&) = delete;
    ShutdownSignal& operator=(const ShutdownSignal&) = delete;

private:
    void onReadable(int fd, IOEvent events);

    EventLoop& loop_;
    int eventFd_;
    Handler onShutdownRequested_;
};

} // namespace asyncnet
