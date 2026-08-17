#include "asyncnet/idle_timeout_manager.hpp"

namespace asyncnet {

IdleTimeoutManager::IdleTimeoutManager(std::chrono::milliseconds timeout)
    : timeout_(timeout) {}

void IdleTimeoutManager::recordActivity(ConnectionId id, Clock::time_point now) {
    lastActivity_[id] = now;
    queue_.push(Entry{now + timeout_, id});
}

void IdleTimeoutManager::remove(ConnectionId id) {
    lastActivity_.erase(id);
}

void IdleTimeoutManager::checkExpired(const ExpiredHandler& onExpired, Clock::time_point now) {
    while (!queue_.empty() && queue_.top().expiry <= now) {
        const Entry entry = queue_.top();
        queue_.pop();

        const auto it = lastActivity_.find(entry.id);
        if (it == lastActivity_.end()) {
            continue; // no longer tracked (already closed/removed)
        }
        if (now < it->second + timeout_) {
            // Stale: real last activity is more recent than what this
            // entry assumed, so it isn't actually idle yet. A fresher
            // entry (pushed at that later activity) is still in the queue
            // and will be the one that eventually validates.
            continue;
        }

        lastActivity_.erase(it);
        if (onExpired) {
            onExpired(entry.id);
        }
    }
}

int IdleTimeoutManager::nextPollTimeoutMs(Clock::time_point now) const {
    if (queue_.empty()) {
        return -1;
    }
    const auto remaining = queue_.top().expiry - now;
    if (remaining <= Clock::duration::zero()) {
        return 0;
    }
    // Round UP to milliseconds, not truncate: truncating a sub-millisecond
    // positive remainder to 0 would turn this into a non-blocking poll()
    // that returns instantly without waiting out the gap, and since real
    // time barely advances per iteration at that point, the caller's loop
    // would busy-spin for many cycles instead of just blocking the
    // remaining sliver of a millisecond.
    return static_cast<int>(std::chrono::ceil<std::chrono::milliseconds>(remaining).count());
}

} // namespace asyncnet
