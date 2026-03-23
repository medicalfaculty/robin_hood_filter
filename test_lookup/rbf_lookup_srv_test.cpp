#include "rdma_rbf.h"
#include "utils.h"

#include <iostream>

#define RNIC_NAME "mlx4_0"
#define RNIC_PORT (2)
#define GID_INDEX (0)
#define TCP_PORT (18516)

#define CLIENT_COUNT (1)

// RBF 参数（与客户端保持一致）
#define RBF_NUM_SLOTS (1024 * 1024 * 8)
#define RBF_MAX_DISTANCE (63)

int main(int argc, char **argv) {
    char cmd[16];
    int count_clients = CLIENT_COUNT;
#ifdef TOGGLE_LOCK_FREE
    count_clients = 1;
    std::cout << "[MODE] TOGGLE_LOCK_FREE" << std::endl;
#endif
    std::cout << "==== RBF Lookup Performance Test Server ====" << std::endl;
    std::cout << "Current Time: " << get_current_time_string() << std::endl;

    std::cout << "=== RdmaRBF Server ===" << std::endl;
    struct RdmaRBF_Srv srv;
    RdmaRBF_Srv_init(&srv, RBF_NUM_SLOTS, RBF_MAX_DISTANCE, count_clients, RNIC_NAME, RNIC_PORT, TCP_PORT, GID_INDEX);

    sync_server(srv.list_sockfd);
    std::cout << "[Server] Initialization successfully!" << std::endl;
    std::cout << "[Server] Waiting for client to complete all 20 rounds..." << std::endl;

    // 等待客户端完成所有测试轮次后发送 EXIT
    for (int i = 0; i < count_clients; i++) {
        reliable_recv(srv.list_sockfd[i], cmd, 5);
        std::cout << "[Server] Received close message from client: " << i + 1 << "/" << count_clients << std::endl;
    }
    RdmaRBF_Srv_destroy(&srv);

    std::cout << "==== Experiment End ====" << std::endl;
    std::cout << "Current Time: " << get_current_time_string() << std::endl;
    return 0;
}
