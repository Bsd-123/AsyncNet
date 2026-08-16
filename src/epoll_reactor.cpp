#include "asyncnet/epoll_reactor.hpp"

#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>

namespace asyncnet {

namespace {

constexpr std::size_t kEventBufferSize = 128;

std::uint32_t toEpollBits(IOEvent events) {
    std::uint32_t bits = 0;
    if (hasEvent(events, IOEvent::Readable)) bits |= EPOLLIN;
    if (hasEvent(events, IOEvent::Writable)) bits |= EPOLLOUT;
    bits |= EPOLLRDHUP; // always watch for peer half-close
    return bits;
}

IOEvent fromEpollBits(std::uint32_t bits) {
    IOEvent events = IOEvent::None;
    if (bits & EPOLLIN) events = events | IOEvent::Readable;
    if (bits & EPOLLOUT) events = events | IOEvent::Writable;
    if (bits & EPOLLERR) events = events | IOEvent::Error;
    if (bits & (EPOLLHUP | EPOLLRDHUP)) events = events | IOEvent::HangUp;
    return events;
}

[[noreturn]] void throwErrno(const char* what) {
    throw std::runtime_error(std::string(what) + " failed: " + std::strerror(errno));
}

} // namespace

EpollReactor::EpollReactor() : epollFd_(epoll_create1(EPOLL_CLOEXEC)) {
    if (epollFd_ < 0) {
        throwErrno("epoll_create1");
    }
    eventBuffer_.resize(kEventBufferSize);
}

EpollReactor::~EpollReactor() {
    if (epollFd_ >= 0) {
        close(epollFd_);
    }
}

void EpollReactor::registerFd(int fd, IOEvent events, EventHandler handler) {
    epoll_event ev{};
    ev.events = toEpollBits(events);
    ev.data.fd = fd;

    if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
        throwErrno("epoll_ctl(ADD)");
    }
    handlers_[fd] = std::move(handler);
}

void EpollReactor::modifyFd(int fd, IOEvent events) {
    epoll_event ev{};
    ev.events = toEpollBits(events);
    ev.data.fd = fd;

    if (epoll_ctl(epollFd_, EPOLL_CTL_MOD, fd, &ev) != 0) {
        throwErrno("epoll_ctl(MOD)");
    }
}

void EpollReactor::removeFd(int fd) {
    // The fd may already be closed by the caller in some teardown paths, in
    // which case the kernel has already dropped it from the interest list;
    // that shows up as ENOENT/EBADF here and is not an error for us.
    epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, nullptr);
    handlers_.erase(fd);
}

int EpollReactor::poll(int timeoutMs) {
    int n;
    for (;;) {
        n = epoll_wait(epollFd_, eventBuffer_.data(),
                        static_cast<int>(eventBuffer_.size()), timeoutMs);
        if (n >= 0) break;
        if (errno == EINTR) continue; // not an error: retry the wait
        throwErrno("epoll_wait");
    }

    for (int i = 0; i < n; ++i) {
        const int fd = eventBuffer_[static_cast<std::size_t>(i)].data.fd;
        const auto it = handlers_.find(fd);
        if (it == handlers_.end()) {
            // Removed by an earlier handler within this same batch; skip.
            continue;
        }
        const IOEvent events =
            fromEpollBits(eventBuffer_[static_cast<std::size_t>(i)].events);
        it->second(fd, events);
    }

    return n;
}

} // namespace asyncnet
