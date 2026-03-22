#ifndef __RDMA_RBF_H__
#define __RDMA_RBF_H__

#include <cstdint>
#include <stdlib.h>
#include <assert.h>
#include <cstring>
#include <sstream>
#include <iostream>
#include <vector>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include "rdma_common.h"
#include "hash.h"

// RBF (Robin Hood Bloom Filter) 参数定义
#define RBF_MAX_DISTANCE_M 63       // 最大距离参数 M（6-bit 最大值）
#define RBF_K_SLOTS_READ 64         // 每次读取的 slot 数量 K（2的幂次，K>=M）
#define RBF_MAX_KICK_CHAIN 500      // 最大踢出链长度
#define RBF_FINGERPRINT_BITS 12     // 指纹位数
#define RBF_DISTANCE_BITS 6         // 距离位数
#define RBF_SLOT_BITS 18            // 每个 slot 总位数 (12 + 6)

// RBF Slot 结构：18 bits = 12-bit fingerprint + 6-bit distance
// 内存布局：使用 32-bit 存储，高 12-bit 为指纹，低 6-bit 为距离
struct rbf_slot {
    uint16_t fingerprint;  // 12-bit fingerprint
    uint8_t distance;      // 6-bit distance (0-63)
};

// RDMA 连接信息结构
struct rdma_conn_info_rbf {
    uint32_t qp_num;
    uint32_t psn;               // initial packet sequence number
    union ibv_gid gid;
    uint64_t data_addr;         // server端 RBF slot 数组地址
    uint32_t data_rkey;         // memory region key
    uint64_t mutex_addr;        // server端 mutex 地址
    uint32_t mutex_rkey;        // mutex memory region key
} __attribute__((packed));

// 状态枚举
enum RBFStatus {
    RBF_Ok = 0,
    RBF_NotFound = 1,
    RBF_NotEnoughSpace = 2,
    RBF_NotSupported = 3,
};

// ================= RDMA RBF Client =================
struct RdmaRBF_Cli {
    // RBF parameters
    uint32_t num_slots;         // slot 总数（用于 hash 取模和负载因子计算）
    uint32_t total_slots;       // 实际分配的 slot 数 = num_slots + max_distance（溢出缓冲区）
    uint32_t max_distance;      // 最大距离 M
    uint32_t k_slots_read;      // 每次读取的 slot 数量 K
    uint32_t max_kick_chain;    // 最大踢出链长度
    uint64_t insert_count;      // 记录成功插入的数量

    // RDMA resources
    ibv_context *ctx;
    ibv_pd *pd;
    ibv_cq *cq;
    ibv_qp *qp;

    // 本地缓冲区：用于存储从服务器读取的 K 个 slot
    // 每个 slot 用 32-bit 存储（虽然实际只用 18-bit）
    uint32_t *buf_slots;        // 大小为 K 个 slot
    uint64_t *buf_mutex;

    ibv_mr *mr_slots;
    ibv_mr *mr_mutex;
    ibv_sge *sge_slots;
    ibv_sge *sge_mutex;

    // Remote info
    rdma_conn_info_rbf remote_info;

    int sockfd;

    TwoIndependentMultiplyShift hasher;
};

// 初始化与销毁
void RdmaRBF_Cli_init(struct RdmaRBF_Cli *cli, uint32_t num_slots, uint32_t max_distance, 
                      uint32_t k_slots_read, uint32_t max_kick_chain,
                      const char* server_ip, const char* name_dev, 
                      uint8_t rnic_port, uint32_t tcp_port, uint8_t gid_index);
void RdmaRBF_Cli_destroy(struct RdmaRBF_Cli *cli);

// 核心操作
RBFStatus RdmaRBF_Cli_insert(struct RdmaRBF_Cli *cli, uint64_t key);
RBFStatus RdmaRBF_Cli_lookup(struct RdmaRBF_Cli *cli, uint64_t key);

// 辅助函数
int RdmaRBF_Cli_conn(struct RdmaRBF_Cli *cli, const char* server_ip, const char* name_dev, 
                     uint8_t rnic_port, uint32_t tcp_port, uint8_t gid_index);
void RdmaRBF_Cli_generate_hash(struct RdmaRBF_Cli *cli, uint64_t key, uint32_t &hash_pos, uint16_t &fingerprint);

// RDMA 操作
int RdmaRBF_Cli_lock(struct RdmaRBF_Cli *cli);
int RdmaRBF_Cli_unlock(struct RdmaRBF_Cli *cli);
int RdmaRBF_Cli_read_slots(struct RdmaRBF_Cli *cli, uint32_t start_slot_idx, uint32_t count);
int RdmaRBF_Cli_write_slots(struct RdmaRBF_Cli *cli, uint32_t start_slot_idx, uint32_t count);

// 本地操作
bool RdmaRBF_Cli_slot_is_empty(struct RdmaRBF_Cli *cli, int local_idx);
void RdmaRBF_Cli_get_slot(struct RdmaRBF_Cli *cli, int local_idx, rbf_slot &slot);
void RdmaRBF_Cli_set_slot(struct RdmaRBF_Cli *cli, int local_idx, const rbf_slot &slot);
void RdmaRBF_Cli_clear_slot(struct RdmaRBF_Cli *cli, int local_idx);

// ================= RDMA RBF Server =================
struct RdmaRBF_Srv {
    uint32_t num_slots;         // slot 总数（用于 hash 取模和负载因子计算）
    uint32_t total_slots;       // 实际分配的 slot 数 = num_slots + max_distance（溢出缓冲区）
    uint32_t max_distance;      // 最大距离 M
    uint32_t count_clients_expected;

    // RBF data: 每个 slot 用 32-bit 存储
    uint32_t *data;             // slot 数组
    uint32_t size_data;         // 数据区大小（字节）
    
    // Mutex
    uint64_t *mutex_list;
    uint32_t count_mutex;

    // RDMA resources
    ibv_context *ctx;
    ibv_pd *pd;
    ibv_cq *cq;
    std::vector<ibv_qp *> list_qp;
    ibv_mr *data_mr;
    ibv_mr *mutex_mr;

    std::vector<int> list_sockfd;
    std::vector<rdma_conn_info_rbf> list_remote_info;

    bool use_hp;
};

// 初始化与销毁
void RdmaRBF_Srv_init(struct RdmaRBF_Srv *srv, uint32_t num_slots, uint32_t max_distance,
                      int client_count, const char* name_dev, 
                      uint8_t rnic_port, uint32_t tcp_port, uint8_t gid_index);
void RdmaRBF_Srv_destroy(struct RdmaRBF_Srv *srv);

// 连接
int RdmaRBF_Srv_conn(struct RdmaRBF_Srv *srv, int client_count, const char* name_dev, 
                     uint8_t rnic_port, uint32_t tcp_port, uint8_t gid_index);

// 日志和统计辅助函数（定义在 rdma_rbf.cpp 中）
void RdmaRBF_print_stats();
void RdmaRBF_reset_stats();

#endif // __RDMA_RBF_H__
