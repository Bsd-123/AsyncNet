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
    // Never block indefinitely (or longer than necessary) while deferred
    // work is already waiting -- post() can be called between runOnce()
    // cycles (e.g. by an idle-timeout sweep run after this function
    // returns), not just from within a dispatch handler, so a queued
    // action must not have to wait on an unrelated I/O event to get its
    // chance to run.
    const int effectiveTimeoutMs = deferredActions_.empty() ? timeoutMs : 0;
    reactor_->poll(effectiveTimeoutMs);

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
