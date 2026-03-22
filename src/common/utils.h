#ifndef __UTILS_H__
#define __UTILS_H__

#include <vector>
#include <cstdint>
#include <string>
#include <iostream>
#include <unistd.h>
#include <chrono>

#include <arpa/inet.h>
#include <sys/socket.h>

#define COUNT_RETRY_MAX 500

std::vector<uint64_t> GenerateRandom64(std::size_t count);

// std::vector<uint64_t> GenerateRandomOpenSSL(std::size_t count);

int reliable_send(int sockfd, const void *data, uint32_t length);
int reliable_recv(int sockfd, void *data, uint32_t length);

void assert_else(bool condition, const std::string& message, bool exit_on_failure=false);

inline uint64_t upperpower2(uint64_t x)
{
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    x++;
    return x;
}

void alloc_aligned_64(void** ptr, std::size_t size);

void sync_client(int const &sockfd);
void sync_server(std::vector<int> const &list_sockfd);

// change: socket_list, remote_info
template<typename T>
void tcp_exchange_server(uint32_t const tcp_port, int const client_count, std::vector<int> &list_sockfd, std::vector<T> &list_remote_info, std::vector<T *> const &list_local_info) {
    uint32_t size_info = sizeof(T);
    // 开放 TCP 监听
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(tcp_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    auto bind_result = bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    assert_else(bind_result == 0, "Server bind TCP failed", true);
    std::cout << "[Server] Listening for client TCP connections..." << std::endl;

    auto listen_result = listen(listen_fd, client_count);
    assert_else(listen_result == 0, "Server listen TCP failed", true);

    // 建立连接 和 交换信息
    for (int i = 0; i < client_count; i++) {
        int client_fd = accept(listen_fd, NULL, NULL);
        assert_else(client_fd >= 0, "accept failed", true);
        list_sockfd[i] = client_fd;
        uint32_t res_send = reliable_send(client_fd, list_local_info[i], size_info);
        assert_else(res_send == size_info, "info send failed");
        uint32_t res_recv = reliable_recv(client_fd, &(list_remote_info[i]), size_info);
        assert_else(res_recv == size_info, "info recv failed");

        std::cout << "[Server] connected client: " << i + 1 << '/' << client_count << std::endl;
    }
    // 关闭监听 socket，避免端口残留导致 "Address already in use"
    close(listen_fd);
    return;
}

// change: remote_info
template<typename T>
void tcp_exchange_client(uint32_t const &tcp_port, const char* server_ip, int const &sockfd, T *local_info, T *remote_info) {
    uint32_t size_info = sizeof(T);
    // 连接服务器
    sockaddr_in sock_addr = {};
    sock_addr.sin_family = AF_INET;
    sock_addr.sin_port = htons(tcp_port);
    inet_pton(AF_INET, server_ip, &sock_addr.sin_addr);
    std::cout << "[Client] Connecting to server at " << server_ip << "..." << std::endl;
    int return_connect = 1, count_retry = COUNT_RETRY_MAX;
    while (return_connect != 0) {
        assert_else((count_retry--) > 0, "[Client] connect timeout", true);
        return_connect = connect(sockfd, (sockaddr*)&sock_addr, sizeof(sock_addr));
        sleep(1);
    }
    std::cout << "[Client] Connected to server at " << server_ip << std::endl;

    // 交换信息
    uint32_t res_recv = reliable_recv(sockfd, remote_info, size_info);
    assert_else(res_recv == size_info, "info recv failed");
    uint32_t res_send = reliable_send(sockfd, local_info, size_info);
    assert_else(res_send == size_info, "info send failed");
    return;
}

std::string get_current_time_string();

void hugepage_alloc(void** ptr, uint64_t const size);

#endif /* __UTILS_H__ */