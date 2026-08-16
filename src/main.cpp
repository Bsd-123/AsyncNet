#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>

#include "asyncnet/acceptor.hpp"
#include "asyncnet/connection.hpp"
#include "asyncnet/event_loop.hpp"
#include "asyncnet/logger.hpp"

namespace {
constexpr std::uint16_t kDefaultPort = 9000;
}

// Plain TCP echo server: no parsing yet (M2), no idle timeout, backpressure
// watermarks, or graceful shutdown yet (later M1 steps) -- just the
// Acceptor/Connection wiring proving the pipeline works end to end.
int main(int argc, char** argv) {
    const std::uint16_t port =
        argc > 1 ? static_cast<std::uint16_t>(std::stoi(argv[1])) : kDefaultPort;

    asyncnet::EventLoop loop;
    std::unordered_map<int, std::unique_ptr<asyncnet::Connection>> connections;

    asyncnet::Acceptor acceptor(loop, port, [&](int fd) {
        connections[fd] = std::make_unique<asyncnet::Connection>(
            loop, fd, [&connections](int closedFd) { connections.erase(closedFd); });
    });

    ASYNCNET_LOG_INFO("AsyncNet echo server listening on port " +
                       std::to_string(acceptor.boundPort()));

    loop.run();

    return 0;
}
