#include "asyncnet/event_loop.hpp"

#include "asyncnet/epoll_reactor.hpp"

namespace asyncnet {

EventLoop::EventLoop() : EventLoop(std::make_unique<EpollReactor>()) {}

EventLoop::EventLoop(std::unique_ptr<IOReactor> reactor)
    : reactor_(std::move(reactor)) {}

void EventLoop::run() {
    while (!stopRequested_) {
        reactor_->poll(-1);
    }
}

void EventLoop::stop() {
    stopRequested_ = true;
}

} // namespace asyncnet
