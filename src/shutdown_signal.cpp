#include "asyncnet/shutdown_signal.hpp"

#include <sys/eventfd.h>
#include <unistd.h>
#include <csignal>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include "asyncnet/logger.hpp"

namespace asyncnet {

namespace {

// Signal handlers are plain C function pointers with no capture, so the
// target fd has to live somewhere static. Only one ShutdownSignal is
// expected to be alive at a time (one per process, owned by EchoServer) --
// the constructor/destructor set and clear this on that assumption.
std::atomic<int> g_shutdownEventFd{-1};

extern "C" void handleShutdownSignal(int /*signum*/) {
    const int fd = g_shutdownEventFd.load(std::memory_order_relaxed);
    if (fd < 0) {
        return;
    }
    constexpr std::uint64_t one = 1;
    // write() is on POSIX's async-signal-safe list. The result is
    // deliberately ignored: there is nothing safe to do with an error from
    // inside a signal handler, and the fd is non-blocking so this never
    // stalls the handler.
    const ssize_t result = write(fd, &one, sizeof(one));
    (void)result;
}

[[noreturn]] void throwErrno(const char* what) {
    throw std::runtime_error(std::string(what) + " failed: " + std::strerror(errno));
}

} // namespace

ShutdownSignal::ShutdownSignal(EventLoop& loop, Handler onShutdownRequested)
    : loop_(loop), eventFd_(-1), onShutdownRequested_(std::move(onShutdownRequested)) {
    eventFd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (eventFd_ < 0) {
        throwErrno("eventfd");
    }

    g_shutdownEventFd.store(eventFd_, std::memory_order_relaxed);

    struct sigaction sa {};
    sa.sa_handler = handleShutdownSignal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    loop_.reactor().registerFd(eventFd_, IOEvent::Readable,
                                [this](int fd, IOEvent e) { onReadable(fd, e); });
}

ShutdownSignal::~ShutdownSignal() {
    std::signal(SIGINT, SIG_DFL);
    std::signal(SIGTERM, SIG_DFL);
    g_shutdownEventFd.store(-1, std::memory_order_relaxed);

    loop_.reactor().removeFd(eventFd_);
    close(eventFd_);
}

void ShutdownSignal::onReadable(int fd, IOEvent /*events*/) {
    std::uint64_t value = 0;
    // Standard eventfd protocol: read() drains the accumulated counter.
    // The value itself doesn't matter -- only that a signal arrived at
    // least once since the last check.
    const ssize_t n = read(fd, &value, sizeof(value));
    (void)n;

    ASYNCNET_LOG_INFO("shutdown signal received");
    if (onShutdownRequested_) {
        onShutdownRequested_();
    }
}

} // namespace asyncnet
