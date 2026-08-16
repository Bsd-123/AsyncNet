#include "asyncnet/event_loop.hpp"

#include "asyncnet/epoll_reactor.hpp"

namespace asyncnet {

EventLoop::EventLoop() : EventLoop(std::make_unique<EpollReactor>()) {}

EventLoop::EventLoop(std::unique_ptr<IOReactor> reactor)
    : reactor_(std::move(reactor)) {}

void EventLoop::run() {
    while (!stopRequested_) {
        runOnce(-1);
    }
}

void EventLoop::runOnce(int timeoutMs) {
    reactor_->poll(timeoutMs);

    // Actions may themselves post further actions (e.g. a connection close
    // callback erasing another connection); swap the queue out first so
    // any such re-entrant posts land in the *next* cycle, not this one.
    std::vector<std::function<void()>> actions;
    actions.swap(deferredActions_);
    for (auto& action : actions) {
        action();
    }
}

void EventLoop::stop() {
    stopRequested_ = true;
}

void EventLoop::post(std::function<void()> action) {
    deferredActions_.push_back(std::move(action));
}

} // namespace asyncnet
