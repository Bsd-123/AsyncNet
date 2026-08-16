#pragma once

#include <sys/epoll.h>

#include <unordered_map>
#include <vector>

#include "asyncnet/io_reactor.hpp"

namespace asyncnet {

// Only class that touches epoll syscalls directly (architecture doc §6).
class EpollReactor final : public IOReactor {
public:
    EpollReactor();
    ~EpollReactor() override;

    EpollReactor(const EpollReactor&) = delete;
    EpollReactor& operator=(const EpollReactor&) = delete;

    void registerFd(int fd, IOEvent events, EventHandler handler) override;
    void modifyFd(int fd, IOEvent events) override;
    void removeFd(int fd) override;
    int poll(int timeoutMs) override;

private:
    int epollFd_;
    std::unordered_map<int, EventHandler> handlers_;
    std::vector<epoll_event> eventBuffer_;
};

} // namespace asyncnet
