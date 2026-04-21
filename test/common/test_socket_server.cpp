#include "catch.hpp"
#include "main/socket/turbolynx_socket_server.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <thread>

namespace {

class ScopedSocketPair {
public:
    ScopedSocketPair() {
        REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
    }

    ~ScopedSocketPair() {
        if (fds[0] >= 0) {
            ::close(fds[0]);
        }
        if (fds[1] >= 0) {
            ::close(fds[1]);
        }
    }

    int reader() const { return fds[0]; }
    int writer() const { return fds[1]; }

private:
    int fds[2] = {-1, -1};
};

void WriteAllFd(int fd, const void *buffer, size_t len) {
    auto *ptr = static_cast<const char *>(buffer);
    size_t total_written = 0;
    while (total_written < len) {
        ssize_t written = ::write(fd, ptr + total_written, len - total_written);
        REQUIRE(written > 0);
        total_written += static_cast<size_t>(written);
    }
}

} // namespace

TEST_CASE("Socket server reads fragmented length-prefixed JSON requests",
          "[common][socket]") {
    ScopedSocketPair sockets;
    std::string payload = R"({"query":"RETURN 1"})";

    std::thread writer([&] {
        uint32_t frame_size = htonl(static_cast<uint32_t>(payload.size()));
        auto *prefix = reinterpret_cast<const char *>(&frame_size);
        WriteAllFd(sockets.writer(), prefix, 2);
        WriteAllFd(sockets.writer(), prefix + 2, 2);
        WriteAllFd(sockets.writer(), payload.data(), 5);
        WriteAllFd(sockets.writer(), payload.data() + 5, payload.size() - 5);
    });

    std::string received;
    REQUIRE(TurboLynxSocketServer::ReadLengthPrefixedRequest(sockets.reader(), received));
    CHECK(received == payload);

    writer.join();
}

TEST_CASE("Socket server reads requests larger than legacy buffer size",
          "[common][socket]") {
    ScopedSocketPair sockets;
    std::string large_query(BUFFER_SIZE + 512, 'x');
    std::string payload = std::string("{\"query\":\"") + large_query + "\"}";

    std::thread writer([&] {
        uint32_t frame_size = htonl(static_cast<uint32_t>(payload.size()));
        WriteAllFd(sockets.writer(), &frame_size, sizeof(frame_size));
        WriteAllFd(sockets.writer(), payload.data(), payload.size());
    });

    std::string received;
    REQUIRE(TurboLynxSocketServer::ReadLengthPrefixedRequest(sockets.reader(), received));
    CHECK(received == payload);
    CHECK(received.size() > BUFFER_SIZE);

    writer.join();
}
