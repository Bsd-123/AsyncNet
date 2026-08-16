#pragma once

#include <chrono>
#include <functional>
#include <queue>
#include <unordered_map>
#include <vector>

namespace asyncnet {

// Detects idle connections via a std::priority_queue of
// (expiry_time, connection_id), min-ordered on expiry_time -- not a linear
// sweep, and not a custom timer wheel (architecture doc §5).
//
// std::priority_queue has no decrease-key: on activity, a fresh entry is
// pushed rather than mutating an old one. Lazy deletion discards stale
// entries -- validated against the connection's current last-activity
// value -- when popped. The queue is not required to stay bounded; the
// only requirement is that stale entries are eventually discarded and
// never cause a false expiry.
class IdleTimeoutManager {
public:
    using ConnectionId = int;
    using ExpiredHandler = std::function<void(ConnectionId)>;
    using Clock = std::chrono::steady_clock;

    explicit IdleTimeoutManager(std::chrono::milliseconds timeout);

    void recordActivity(ConnectionId id, Clock::time_point now = Clock::now());

    // Stops tracking id (e.g. it already closed for another reason). Not
    // strictly required for correctness -- a stale entry for it would just
    // be discarded on lazy deletion regardless -- but avoids it lingering
    // in the last-activity map until then.
    void remove(ConnectionId id);

    // Pops and validates every entry whose expiry has passed, invoking
    // onExpired for each connection that is genuinely idle. Entries
    // superseded by more recent activity are discarded silently.
    void checkExpired(const ExpiredHandler& onExpired, Clock::time_point now = Clock::now());

    // Timeout (ms) to pass to IOReactor::poll(), so the loop wakes up in
    // time to notice the next possible expiry. -1 (block indefinitely)
    // when nothing is currently being tracked.
    int nextPollTimeoutMs(Clock::time_point now = Clock::now()) const;

private:
    struct Entry {
        Clock::time_point expiry;
        ConnectionId id;
    };
    struct EntryCompare {
        // std::priority_queue is a max-heap by default; reversing the
        // comparison here makes top() the *soonest* expiry instead.
        bool operator()(const Entry& a, const Entry& b) const { return a.expiry > b.expiry; }
    };

    std::chrono::milliseconds timeout_;
    std::priority_queue<Entry, std::vector<Entry>, EntryCompare> queue_;
    std::unordered_map<ConnectionId, Clock::time_point> lastActivity_;
};

} // namespace asyncnet
