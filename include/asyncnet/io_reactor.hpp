#pragma once

#include <cstdint>
#include <functional>

namespace asyncnet {

enum class IOEvent : std::uint32_t {
    None     = 0,
    Readable = 1u << 0,
    Writable = 1u << 1,
    Error    = 1u << 2,
    HangUp   = 1u << 3,
};

constexpr IOEvent operator|(IOEvent lhs, IOEvent rhs) {
    return static_cast<IOEvent>(static_cast<std::uint32_t>(lhs) |
                                 static_cast<std::uint32_t>(rhs));
}

constexpr IOEvent operator&(IOEvent lhs, IOEvent rhs) {
    return static_cast<IOEvent>(static_cast<std::uint32_t>(lhs) &
                                 static_cast<std::uint32_t>(rhs));
}

constexpr bool hasEvent(IOEvent mask, IOEvent flag) {
    return (mask & flag) != IOEvent::None;
}

// Invoked by the reactor with the fd and the set of events that are ready.
using EventHandler = std::function<void(int fd, IOEvent events)>;

// Minimal readiness-based I/O reactor interface -- not a universal
// abstraction spanning readiness-based (epoll) and completion-based
// (io_uring) models. See architecture doc §6: if io_uring is ever added,
// this interface gets redesigned then, not pre-fitted now.
class IOReactor {
public:
    virtual ~IOReactor() = default;

    // Starts watching fd for the given events, invoking handler when ready.
    virtual void registerFd(int fd, IOEvent events, EventHandler handler) = 0;

    // Changes the set of events watched for an already-registered fd.
    virtual void modifyFd(int fd, IOEvent events) = 0;

    // Stops watching fd. Guarantees the fd's handler will not be invoked
    // again after this call returns, even for events already read from the
    // kernel as part of the current poll() batch. Does NOT close the fd --
    // callers own that fd's lifetime, and must defer the actual close()
    // until after the current dispatch pass finishes, so the OS can't hand
    // the same fd number to a new connection while this batch is still
    // being processed.
    virtual void removeFd(int fd) = 0;

    // Blocks up to timeoutMs (negative = block indefinitely) waiting for
    // events, then dispatches ready handlers. Retries internally on EINTR
    // rather than propagating it. Returns the number of ready fds
    // dispatched.
    virtual int poll(int timeoutMs) = 0;
};

} // namespace asyncnet
