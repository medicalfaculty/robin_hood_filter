#include <sys/mman.h>
#include <chrono>
#include <iomanip>
#include <atomic>

#include "rdma_rbf.h"
#include "utils.h"

// Slot 编码/解码辅助函数
// 内存布局：32-bit 中，高 12-bit 为指纹，低 6-bit 为距离，中间 14-bit 保留
#define RBF_ENCODE_SLOT(fingerprint, distance) \
    ((((uint32_t)(fingerprint) & 0xFFF) << 6) | ((uint32_t)(distance) & 0x3F))

#define RBF_DECODE_FINGERPRINT(encoded) (((encoded) >> 6) & 0xFFF)
#define RBF_DECODE_DISTANCE(encoded) ((encoded) & 0x3F)
#define RBF_IS_EMPTY(encoded) ((encoded) == 0)

// ==================== 增强日志系统 ====================
// 日志级别控制：可通过编译宏 RBF_LOG_LEVEL 控制
// 0 = 关闭, 1 = 错误, 2 = 基本调试, 3 = 详细（含 buffer dump）
#ifndef RBF_LOG_LEVEL
#define RBF_LOG_LEVEL 1
#endif

// 全局操作计数器（用于追踪 RDMA 操作序列）
static std::atomic<uint64_t> g_rbf_op_seq(0);

// RDMA 操作统计
struct RbfRdmaStats {
    std::atomic<uint64_t> total_reads{0};
    std::atomic<uint64_t> total_writes{0};
    std::atomic<uint64_t> read_failures{0};
    std::atomic<uint64_t> write_failures{0};
    std::atomic<uint64_t> lock_attempts{0};
    std::atomic<uint64_t> lock_failures{0};
    std::atomic<uint64_t> total_inserts{0};
    std::atomic<uint64_t> insert_successes{0};
    std::atomic<uint64_t> insert_failures{0};
    std::atomic<uint64_t> total_kicks{0};
    std::atomic<uint64_t> max_kick_chain_seen{0};
    std::atomic<uint64_t> total_lookups{0};
    std::atomic<uint64_t> lookup_found{0};
    std::atomic<uint64_t> lookup_not_found{0};
};

static RbfRdmaStats g_rbf_stats;

// 基本调试日志宏
#if RBF_LOG_LEVEL >= 2
#define RBF_DEBUG_LOG(msg) do { \
    uint64_t _seq = g_rbf_op_seq.fetch_add(1); \
    std::cout << "[RBF_DEBUG][seq=" << _seq << "] " << msg << std::endl; \
} while(0)
#define RBF_DEBUG_LOG_KEY(msg, key) do { \
    uint64_t _seq = g_rbf_op_seq.fetch_add(1); \
    std::cout << "[RBF_DEBUG][seq=" << _seq << "] " << msg << " key=" << key << std::endl; \
} while(0)
#define RBF_DEBUG_LOG_SLOT(msg, idx, fp, dist) do { \
    uint64_t _seq = g_rbf_op_seq.fetch_add(1); \
    std::cout << "[RBF_DEBUG][seq=" << _seq << "] " << msg \
              << " idx=" << idx << " fp=0x" << std::hex << fp << std::dec \
              << " dist=" << (int)dist << std::endl; \
} while(0)
#else
#define RBF_DEBUG_LOG(msg) ((void)0)
#define RBF_DEBUG_LOG_KEY(msg, key) ((void)0)
#define RBF_DEBUG_LOG_SLOT(msg, idx, fp, dist) ((void)0)
#endif

// 错误日志（始终开启，级别 >= 1）
#if RBF_LOG_LEVEL >= 1
#define RBF_ERROR_LOG(msg) do { \
    uint64_t _seq = g_rbf_op_seq.fetch_add(1); \
    std::cerr << "[RBF_ERROR][seq=" << _seq << "] " << msg << std::endl; \
} while(0)
#else
#define RBF_ERROR_LOG(msg) ((void)0)
#endif

// 详细日志（含 buffer dump，级别 >= 3）
#if RBF_LOG_LEVEL >= 3
#define RBF_VERBOSE_LOG(msg) do { \
    uint64_t _seq = g_rbf_op_seq.fetch_add(1); \
    std::cout << "[RBF_VERBOSE][seq=" << _seq << "] " << msg << std::endl; \
} while(0)
#else
#define RBF_VERBOSE_LOG(msg) ((void)0)
#endif

// 辅助函数：打印 buffer 中所有 slot 的内容
static void rbf_dump_buffer(const uint32_t *buf, uint32_t count, uint32_t start_slot_idx) {
#if RBF_LOG_LEVEL >= 3
    std::cout << "[RBF_VERBOSE] Buffer dump (" << count << " slots starting at " << start_slot_idx << "):" << std::endl;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t encoded = buf[i];
        std::cout << "  [" << std::setw(4) << (start_slot_idx + i) << "] raw=0x"
                  << std::hex << std::setw(8) << std::setfill('0') << encoded << std::dec << std::setfill(' ')
                  << " fp=0x" << std::hex << std::setw(3) << std::setfill('0') << RBF_DECODE_FINGERPRINT(encoded)
                  << std::dec << std::setfill(' ')
                  << " dist=" << std::setw(2) << (int)RBF_DECODE_DISTANCE(encoded)
                  << (RBF_IS_EMPTY(encoded) ? " [EMPTY]" : "") << std::endl;
    }
#endif
}

// 辅助函数：打印 RDMA 操作统计信息
void RdmaRBF_print_stats() {
    std::cout << "\n========== RBF RDMA Operation Statistics ==========" << std::endl;
    std::cout << "  RDMA Reads:        total=" << g_rbf_stats.total_reads
              << " failures=" << g_rbf_stats.read_failures << std::endl;
    std::cout << "  RDMA Writes:       total=" << g_rbf_stats.total_writes
              << " failures=" << g_rbf_stats.write_failures << std::endl;
    std::cout << "  Lock Attempts:     total=" << g_rbf_stats.lock_attempts
              << " failures=" << g_rbf_stats.lock_failures << std::endl;
    std::cout << "  Inserts:           total=" << g_rbf_stats.total_inserts
              << " success=" << g_rbf_stats.insert_successes
              << " fail=" << g_rbf_stats.insert_failures << std::endl;
    std::cout << "  Kick Operations:   total=" << g_rbf_stats.total_kicks
              << " max_chain=" << g_rbf_stats.max_kick_chain_seen << std::endl;
    std::cout << "  Lookups:           total=" << g_rbf_stats.total_lookups
              << " found=" << g_rbf_stats.lookup_found
              << " not_found=" << g_rbf_stats.lookup_not_found << std::endl;
    double read_fail_rate = g_rbf_stats.total_reads > 0 ?
        (double)g_rbf_stats.read_failures / g_rbf_stats.total_reads * 100.0 : 0.0;
    double write_fail_rate = g_rbf_stats.total_writes > 0 ?
        (double)g_rbf_stats.write_failures / g_rbf_stats.total_writes * 100.0 : 0.0;
    double insert_fail_rate = g_rbf_stats.total_inserts > 0 ?
        (double)g_rbf_stats.insert_failures / g_rbf_stats.total_inserts * 100.0 : 0.0;
    std::cout << "  Read Failure Rate:   " << std::fixed << std::setprecision(2) << read_fail_rate << "%" << std::endl;
    std::cout << "  Write Failure Rate:  " << std::fixed << std::setprecision(2) << write_fail_rate << "%" << std::endl;
    std::cout << "  Insert Failure Rate: " << std::fixed << std::setprecision(2) << insert_fail_rate << "%" << std::endl;
    std::cout << "==================================================\n" << std::endl;
}

// 辅助函数：重置统计计数器
void RdmaRBF_reset_stats() {
    g_rbf_stats.total_reads = 0;
    g_rbf_stats.total_writes = 0;
    g_rbf_stats.read_failures = 0;
    g_rbf_stats.write_failures = 0;
    g_rbf_stats.lock_attempts = 0;
    g_rbf_stats.lock_failures = 0;
    g_rbf_stats.total_inserts = 0;
    g_rbf_stats.insert_successes = 0;
    g_rbf_stats.insert_failures = 0;
    g_rbf_stats.total_kicks = 0;
    g_rbf_stats.max_kick_chain_seen = 0;
    g_rbf_stats.total_lookups = 0;
    g_rbf_stats.lookup_found = 0;
    g_rbf_stats.lookup_not_found = 0;
    g_rbf_op_seq = 0;
}

// ================= RDMA RBF Client Implementation =================

void RdmaRBF_Cli_init(struct RdmaRBF_Cli *cli, uint32_t num_slots, uint32_t max_distance,
                      uint32_t k_slots_read, uint32_t max_kick_chain,
                      const char* server_ip, const char* name_dev,
                      uint8_t rnic_port, uint32_t tcp_port, uint8_t gid_index) {
    memset(cli, 0, sizeof(*cli));
    
    // 确保 num_slots 是 2 的幂次，便于哈希取模
    cli->num_slots = upperpower2(num_slots);
    cli->max_distance = max_distance;
    cli->total_slots = cli->num_slots + cli->max_distance;  // 实际可访问范围包含溢出缓冲区
    cli->k_slots_read = k_slots_read;
    cli->max_kick_chain = max_kick_chain;
    cli->insert_count = 0;
    cli->hasher = TwoIndependentMultiplyShift();

    // 分配本地缓冲区
    alloc_aligned_64((void**)&cli->buf_slots, k_slots_read * sizeof(uint32_t));
    alloc_aligned_64((void**)&cli->buf_mutex, sizeof(uint64_t));

    // 建立连接
    RdmaRBF_Cli_conn(cli, server_ip, name_dev, rnic_port, tcp_port, gid_index);
    return;
}

int RdmaRBF_Cli_conn(struct RdmaRBF_Cli *cli, const char* server_ip, const char* name_dev,
                     uint8_t rnic_port, uint32_t tcp_port, uint8_t gid_index) {
    // 打开 ctx 和创建 pd 和 cq
    cli->ctx = open_rdma_ctx(name_dev);
    cli->pd = ibv_alloc_pd(cli->ctx);
    cli->cq = ibv_create_cq(cli->ctx, 32, NULL, NULL, 0);  // 增加 CQ 大小以支持批量操作

    // 注册 mr
    cli->mr_slots = ibv_reg_mr(cli->pd, cli->buf_slots, cli->k_slots_read * sizeof(uint32_t), MR_FLAGS_RW);
    cli->mr_mutex = ibv_reg_mr(cli->pd, cli->buf_mutex, sizeof(uint64_t), MR_FLAGS_ATOMIC);

    // 创建 sge
    cli->sge_slots = create_sge(cli->mr_slots);
    cli->sge_mutex = create_sge(cli->mr_mutex);

    // 创建 qp 并修改到 init 状态
    cli->qp = create_rc_qp(cli->pd, cli->cq);
    modify_init_qp(cli->qp, rnic_port);

    // 创建 local_info
    rdma_conn_info_rbf *local_info = create_rdma_conn_info<rdma_conn_info_rbf>(cli->ctx, rnic_port, gid_index);
    local_info->qp_num = cli->qp->qp_num;

    // 交换信息 over TCP
    cli->sockfd = socket(AF_INET, SOCK_STREAM, 0);
    cli->remote_info = {};
    tcp_exchange_client<rdma_conn_info_rbf>(tcp_port, server_ip, cli->sockfd, local_info, &(cli->remote_info));

    // 修改 qp 到 rtr 和 rts
    modify_rtr_qp(cli->qp, cli->remote_info.qp_num, cli->remote_info.psn, cli->remote_info.gid, gid_index, rnic_port);
    modify_rts_qp(cli->qp, local_info->psn);

    return 1;
}

void RdmaRBF_Cli_destroy(struct RdmaRBF_Cli *cli) {
    if (cli->sockfd) close(cli->sockfd);
    if (cli->qp) ibv_destroy_qp(cli->qp);
    if (cli->sge_slots) free(cli->sge_slots);
    if (cli->sge_mutex) free(cli->sge_mutex);
    if (cli->mr_slots) ibv_dereg_mr(cli->mr_slots);
    if (cli->mr_mutex) ibv_dereg_mr(cli->mr_mutex);
    if (cli->buf_slots) free(cli->buf_slots);
    if (cli->buf_mutex) free(cli->buf_mutex);
    if (cli->cq) ibv_destroy_cq(cli->cq);
    if (cli->pd) ibv_dealloc_pd(cli->pd);
    if (cli->ctx) ibv_close_device(cli->ctx);
    return;
}

void RdmaRBF_Cli_generate_hash(struct RdmaRBF_Cli *cli, uint64_t key, uint32_t &hash_pos, uint16_t &fingerprint) {
    const uint64_t hash = cli->hasher(key);
    // 高 32-bit 用于确定 slot 位置
    hash_pos = (hash >> 32) & (cli->num_slots - 1);
    // 低 32-bit 用于生成 12-bit 指纹（避免 0，用 0 表示空 slot）
    fingerprint = (hash & 0xFFF);
    if (fingerprint == 0) fingerprint = 1;  // 0 保留给空 slot
    return;
}

// 本地 slot 操作
bool RdmaRBF_Cli_slot_is_empty(struct RdmaRBF_Cli *cli, int local_idx) {
    return RBF_IS_EMPTY(cli->buf_slots[local_idx]);
}

void RdmaRBF_Cli_get_slot(struct RdmaRBF_Cli *cli, int local_idx, rbf_slot &slot) {
    uint32_t encoded = cli->buf_slots[local_idx];
    slot.fingerprint = RBF_DECODE_FINGERPRINT(encoded);
    slot.distance = RBF_DECODE_DISTANCE(encoded);
}

void RdmaRBF_Cli_set_slot(struct RdmaRBF_Cli *cli, int local_idx, const rbf_slot &slot) {
    cli->buf_slots[local_idx] = RBF_ENCODE_SLOT(slot.fingerprint, slot.distance);
}

void RdmaRBF_Cli_clear_slot(struct RdmaRBF_Cli *cli, int local_idx) {
    cli->buf_slots[local_idx] = 0;
}

// RDMA 操作
int RdmaRBF_Cli_lock(struct RdmaRBF_Cli *cli) {
#ifndef TOGGLE_LOCK_FREE
    g_rbf_stats.lock_attempts++;
    RBF_DEBUG_LOG("LOCK: attempting CAS lock at mutex_addr=0x" << std::hex
                  << cli->remote_info.mutex_addr << std::dec
                  << " mutex_rkey=0x" << std::hex << cli->remote_info.mutex_rkey << std::dec);
    auto res_cas = rdma_atomic_cas(cli->qp, 100, cli->sge_mutex, cli->cq, 
                                   cli->remote_info.mutex_addr, cli->remote_info.mutex_rkey, 0, 1);
    if (res_cas != 1) {
        g_rbf_stats.lock_failures++;
        RBF_ERROR_LOG("LOCK FAILED: CAS returned " << res_cas
                      << " mutex_addr=0x" << std::hex << cli->remote_info.mutex_addr << std::dec);
    } else {
        RBF_DEBUG_LOG("LOCK: acquired successfully");
    }
    assert_else(res_cas == 1, "Failed to lock mutex");
#endif
    return 1;
}

int RdmaRBF_Cli_unlock(struct RdmaRBF_Cli *cli) {
#ifndef TOGGLE_LOCK_FREE
    RBF_DEBUG_LOG("UNLOCK: releasing lock");
    *(cli->buf_mutex) = (uint64_t)0;
    rdma_one_side(cli->qp, 101, cli->sge_mutex, cli->remote_info.mutex_addr, 
                  cli->remote_info.mutex_rkey, IBV_WR_RDMA_WRITE);
    int ret = check_cq(cli->cq, 101);
    if (ret != 1) {
        RBF_ERROR_LOG("UNLOCK FAILED: check_cq returned " << ret
                      << " mutex_addr=0x" << std::hex << cli->remote_info.mutex_addr << std::dec);
    } else {
        RBF_DEBUG_LOG("UNLOCK: released successfully");
    }
#endif
    return 1;
}

int RdmaRBF_Cli_read_slots(struct RdmaRBF_Cli *cli, uint32_t start_slot_idx, uint32_t count) {
    g_rbf_stats.total_reads++;

    // 计算实际读取数量（不超过缓冲区大小，且不超出 total_slots 边界）
    uint32_t actual_count = std::min(count, cli->k_slots_read);
    if (start_slot_idx + actual_count > cli->total_slots) {
        actual_count = cli->total_slots - start_slot_idx;
    }
    uint64_t addr = cli->remote_info.data_addr + start_slot_idx * sizeof(uint32_t);
    uint32_t read_size = actual_count * sizeof(uint32_t);

    RBF_DEBUG_LOG("READ_SLOTS: start=" << start_slot_idx << " count=" << actual_count
                  << " remote_addr=0x" << std::hex << addr << std::dec
                  << " rkey=0x" << std::hex << cli->remote_info.data_rkey << std::dec
                  << " size=" << read_size << " bytes"
                  << " local_buf=0x" << std::hex << (uintptr_t)cli->buf_slots << std::dec);

    // 记录读取前的 buffer 内容（用于对比检测数据变化）
    RBF_VERBOSE_LOG("READ_SLOTS: buffer BEFORE read:");
    rbf_dump_buffer(cli->buf_slots, actual_count, start_slot_idx);

    // 临时修改 sge 的长度
    struct ibv_sge temp_sge;
    memcpy(&temp_sge, cli->sge_slots, sizeof(ibv_sge));
    temp_sge.length = read_size;

    RBF_VERBOSE_LOG("READ_SLOTS: posting RDMA READ wr_id=1"
                    << " sge.addr=0x" << std::hex << temp_sge.addr
                    << " sge.length=" << std::dec << temp_sge.length
                    << " sge.lkey=0x" << std::hex << temp_sge.lkey << std::dec);

    int post_ret = rdma_one_side(cli->qp, 1, &temp_sge, addr, cli->remote_info.data_rkey, IBV_WR_RDMA_READ);
    if (post_ret != 1) {
        g_rbf_stats.read_failures++;
        RBF_ERROR_LOG("READ_SLOTS: ibv_post_send FAILED! start=" << start_slot_idx
                      << " post_ret=" << post_ret);
        return 0;
    }

    int ret = check_cq(cli->cq, 1);

    if (ret != 1) {
        g_rbf_stats.read_failures++;
        RBF_ERROR_LOG("READ_SLOTS FAILED: start_slot=" << start_slot_idx
                      << " count=" << actual_count
                      << " remote_addr=0x" << std::hex << addr << std::dec
                      << " rkey=0x" << std::hex << cli->remote_info.data_rkey << std::dec
                      << " check_cq_ret=" << ret);
    } else {
        RBF_DEBUG_LOG("READ_SLOTS SUCCESS: start=" << start_slot_idx << " count=" << actual_count);
        // 打印读取后的完整 buffer 内容
        RBF_VERBOSE_LOG("READ_SLOTS: buffer AFTER read:");
        rbf_dump_buffer(cli->buf_slots, actual_count, start_slot_idx);

        // 统计非空 slot 数量
        uint32_t non_empty = 0;
        for (uint32_t i = 0; i < actual_count; i++) {
            if (!RBF_IS_EMPTY(cli->buf_slots[i])) non_empty++;
        }
        RBF_DEBUG_LOG("READ_SLOTS: non_empty_slots=" << non_empty << "/" << actual_count);
    }
    return ret;
}

int RdmaRBF_Cli_write_slots(struct RdmaRBF_Cli *cli, uint32_t start_slot_idx, uint32_t count) {
    g_rbf_stats.total_writes++;

    uint32_t actual_count = std::min(count, cli->k_slots_read);
    if (start_slot_idx + actual_count > cli->total_slots) {
        actual_count = cli->total_slots - start_slot_idx;
    }
    uint64_t addr = cli->remote_info.data_addr + start_slot_idx * sizeof(uint32_t);
    uint32_t write_size = actual_count * sizeof(uint32_t);

    RBF_DEBUG_LOG("WRITE_SLOTS: start=" << start_slot_idx << " count=" << actual_count
                  << " remote_addr=0x" << std::hex << addr << std::dec
                  << " rkey=0x" << std::hex << cli->remote_info.data_rkey << std::dec
                  << " size=" << write_size << " bytes");

    // 打印将要写入的完整 buffer 内容
    RBF_VERBOSE_LOG("WRITE_SLOTS: buffer content to write:");
    rbf_dump_buffer(cli->buf_slots, actual_count, start_slot_idx);

    struct ibv_sge temp_sge;
    memcpy(&temp_sge, cli->sge_slots, sizeof(ibv_sge));
    temp_sge.length = write_size;

    RBF_VERBOSE_LOG("WRITE_SLOTS: posting RDMA WRITE wr_id=2"
                    << " sge.addr=0x" << std::hex << temp_sge.addr
                    << " sge.length=" << std::dec << temp_sge.length
                    << " sge.lkey=0x" << std::hex << temp_sge.lkey << std::dec);

    int post_ret = rdma_one_side(cli->qp, 2, &temp_sge, addr, cli->remote_info.data_rkey, IBV_WR_RDMA_WRITE);
    if (post_ret != 1) {
        g_rbf_stats.write_failures++;
        RBF_ERROR_LOG("WRITE_SLOTS: ibv_post_send FAILED! start=" << start_slot_idx
                      << " post_ret=" << post_ret);
        return 0;
    }

    int ret = check_cq(cli->cq, 2);

    if (ret != 1) {
        g_rbf_stats.write_failures++;
        RBF_ERROR_LOG("WRITE_SLOTS FAILED: start_slot=" << start_slot_idx
                      << " count=" << actual_count
                      << " remote_addr=0x" << std::hex << addr << std::dec
                      << " rkey=0x" << std::hex << cli->remote_info.data_rkey << std::dec
                      << " check_cq_ret=" << ret);
    } else {
        RBF_DEBUG_LOG("WRITE_SLOTS SUCCESS: start=" << start_slot_idx << " count=" << actual_count);
    }
    return ret;
}

// 插入操作
RBFStatus RdmaRBF_Cli_insert(struct RdmaRBF_Cli *cli, uint64_t key) {
    g_rbf_stats.total_inserts++;
    uint64_t insert_id = g_rbf_stats.total_inserts.load();

    uint32_t hash_pos;
    uint16_t fingerprint;
    RdmaRBF_Cli_generate_hash(cli, key, hash_pos, fingerprint);

    RBF_DEBUG_LOG_KEY("INSERT START [#" << insert_id << "]", key);
    RBF_DEBUG_LOG("  hash_pos=" << hash_pos << " fingerprint=0x" << std::hex << fingerprint << std::dec
                  << " num_slots=" << cli->num_slots << " max_distance=" << cli->max_distance
                  << " current_insert_count=" << cli->insert_count);

    // 加锁
    RBF_DEBUG_LOG("  acquiring lock...");
    RdmaRBF_Cli_lock(cli);
    RBF_DEBUG_LOG("  lock acquired");

    // 准备插入的元素
    rbf_slot new_slot;
    new_slot.fingerprint = fingerprint;
    new_slot.distance = 0;  // 初始距离为 0

    uint32_t curr_pos = hash_pos;
    int kick_chain_len = 0;
    uint32_t original_hash_pos = hash_pos;  // 保存原始 hash 位置用于日志
    int rdma_reads_this_insert = 0;
    int rdma_writes_this_insert = 0;

    while (kick_chain_len < (int)cli->max_kick_chain) {
        // 边界检查：curr_pos 不能超出实际分配的范围（含溢出缓冲区）
        if (curr_pos >= cli->total_slots) {
            RBF_ERROR_LOG("  INSERT[#" << insert_id << "] TABLE BOUNDARY REACHED: curr_pos=" << curr_pos
                          << " >= total_slots=" << cli->total_slots
                          << " kick_chain_len=" << kick_chain_len
                          << " original_hash=" << original_hash_pos);
            g_rbf_stats.insert_failures++;
            RdmaRBF_Cli_unlock(cli);
            return RBF_NotEnoughSpace;
        }

        // 计算需要读取的 slot 范围
        uint32_t batch_start = (curr_pos / cli->k_slots_read) * cli->k_slots_read;
        uint32_t offset_in_batch = curr_pos - batch_start;

        RBF_DEBUG_LOG("  KICK_CHAIN[" << kick_chain_len << "]: curr_pos=" << curr_pos 
                      << " batch_start=" << batch_start << " offset=" << offset_in_batch
                      << " inserting fp=0x" << std::hex << new_slot.fingerprint << std::dec
                      << " original_hash=" << original_hash_pos);

        // 读取 K 个 slot
        int read_ret = RdmaRBF_Cli_read_slots(cli, batch_start, cli->k_slots_read);
        rdma_reads_this_insert++;
        if (read_ret != 1) {
            RBF_ERROR_LOG("  INSERT[#" << insert_id << "] READ FAILED at batch_start=" << batch_start
                          << " kick_chain_len=" << kick_chain_len
                          << " reads_so_far=" << rdma_reads_this_insert);
            g_rbf_stats.insert_failures++;
            RdmaRBF_Cli_unlock(cli);
            return RBF_NotEnoughSpace;
        }

        // 在当前 batch 中尝试插入
        bool kicked = false;
        for (uint32_t i = offset_in_batch; i < cli->k_slots_read; i++) {
            // 计算实际距离
            uint32_t actual_pos = batch_start + i;

            // 边界检查：actual_pos 不能超出实际分配的范围（含溢出缓冲区）
            if (actual_pos >= cli->total_slots) {
                RBF_ERROR_LOG("  INSERT[#" << insert_id << "] SLOT BOUNDARY REACHED: actual_pos=" << actual_pos
                              << " >= total_slots=" << cli->total_slots
                              << " kick_chain_len=" << kick_chain_len);
                g_rbf_stats.insert_failures++;
                RdmaRBF_Cli_unlock(cli);
                return RBF_NotEnoughSpace;
            }

            uint32_t distance = actual_pos - original_hash_pos;
            
            // 检查是否超过最大距离
            if (distance > cli->max_distance) {
                RBF_ERROR_LOG("  INSERT[#" << insert_id << "] MAX DISTANCE EXCEEDED: actual_pos=" << actual_pos
                              << " distance=" << distance << " > M=" << cli->max_distance
                              << " original_hash=" << original_hash_pos
                              << " kick_chain_len=" << kick_chain_len
                              << " fp=0x" << std::hex << new_slot.fingerprint << std::dec);
                g_rbf_stats.insert_failures++;
                RdmaRBF_Cli_unlock(cli);
                return RBF_NotEnoughSpace;
            }

            new_slot.distance = (uint8_t)distance;

            if (RdmaRBF_Cli_slot_is_empty(cli, i)) {
                // 空 slot，直接插入
                RBF_DEBUG_LOG_SLOT("  EMPTY SLOT FOUND", actual_pos, new_slot.fingerprint, new_slot.distance);
                RdmaRBF_Cli_set_slot(cli, i, new_slot);

                RBF_VERBOSE_LOG("  SET slot[" << i << "] in buffer: encoded=0x" << std::hex
                                << RBF_ENCODE_SLOT(new_slot.fingerprint, new_slot.distance) << std::dec);

                int write_ret = RdmaRBF_Cli_write_slots(cli, batch_start, cli->k_slots_read);
                rdma_writes_this_insert++;
                if (write_ret != 1) {
                    RBF_ERROR_LOG("  INSERT[#" << insert_id << "] WRITE FAILED at batch_start=" << batch_start
                                  << " after finding empty slot at pos=" << actual_pos);
                    g_rbf_stats.insert_failures++;
                    RdmaRBF_Cli_unlock(cli);
                    return RBF_NotEnoughSpace;
                }
                cli->insert_count++;
                g_rbf_stats.insert_successes++;
                // 更新最大 kick chain 统计
                uint64_t prev_max = g_rbf_stats.max_kick_chain_seen.load();
                while (kick_chain_len > (int)prev_max &&
                       !g_rbf_stats.max_kick_chain_seen.compare_exchange_weak(prev_max, kick_chain_len));

                RBF_DEBUG_LOG_KEY("INSERT SUCCESS [#" << insert_id << "]", key);
                RBF_DEBUG_LOG("  insert_summary: kick_chain=" << kick_chain_len
                              << " rdma_reads=" << rdma_reads_this_insert
                              << " rdma_writes=" << rdma_writes_this_insert
                              << " final_pos=" << actual_pos
                              << " final_dist=" << (int)new_slot.distance
                              << " total_inserted=" << cli->insert_count);
                RdmaRBF_Cli_unlock(cli);
                return RBF_Ok;
            } else {
                // 比较距离（Robin Hood 策略）
                rbf_slot existing_slot;
                RdmaRBF_Cli_get_slot(cli, i, existing_slot);
                
                RBF_DEBUG_LOG("  COMPARE[pos=" << actual_pos << "]: new(fp=0x" << std::hex << new_slot.fingerprint
                              << " dist=" << std::dec << (int)new_slot.distance
                              << ") vs existing(fp=0x" << std::hex << existing_slot.fingerprint
                              << " dist=" << std::dec << (int)existing_slot.distance << ")");

                if (new_slot.distance > existing_slot.distance) {
                    // Robin Hood: 新元素距离更大（更"穷"），踢出距离更小的"富"元素
                    g_rbf_stats.total_kicks++;
                    RBF_DEBUG_LOG_SLOT("  KICK OUT (Robin Hood: new is poorer)", actual_pos, existing_slot.fingerprint, existing_slot.distance);
                    RBF_DEBUG_LOG("  KICK DETAIL: new(fp=0x" << std::hex << new_slot.fingerprint
                                  << " dist=" << std::dec << (int)new_slot.distance
                                  << ") replaces existing(fp=0x" << std::hex << existing_slot.fingerprint
                                  << " dist=" << std::dec << (int)existing_slot.distance << ")");
                    RdmaRBF_Cli_set_slot(cli, i, new_slot);
                    
                    // 被踢出的元素继续向后查找
                    new_slot = existing_slot;
                    uint32_t kicked_original_pos = actual_pos - new_slot.distance;
                    RBF_DEBUG_LOG("  KICKED ELEMENT: original_hash_pos=" << kicked_original_pos 
                                  << " fp=0x" << std::hex << new_slot.fingerprint << std::dec
                                  << " old_dist=" << (int)new_slot.distance
                                  << " must_relocate_from_pos=" << (actual_pos + 1));
                    
                    hash_pos = kicked_original_pos;  // 恢复被踢出元素的原始 hash 位置
                    original_hash_pos = hash_pos;    // 更新用于距离计算的基准
                    new_slot.distance = 0;  // 重置距离，后续会重新计算
                    
                    // 写入当前 batch（因为发生了修改）
                    int write_ret = RdmaRBF_Cli_write_slots(cli, batch_start, cli->k_slots_read);
                    rdma_writes_this_insert++;
                    if (write_ret != 1) {
                        RBF_ERROR_LOG("  INSERT[#" << insert_id << "] WRITE FAILED after kick at batch_start=" << batch_start
                                      << " kick_chain_len=" << kick_chain_len);
                        g_rbf_stats.insert_failures++;
                        RdmaRBF_Cli_unlock(cli);
                        return RBF_NotEnoughSpace;
                    }
                    
                    // 从下一个位置继续
                    curr_pos = actual_pos + 1;
                    kick_chain_len++;
                    RBF_DEBUG_LOG("  CONTINUE KICK CHAIN: kick_len=" << kick_chain_len
                                  << " next_pos=" << curr_pos
                                  << " kicked_fp=0x" << std::hex << new_slot.fingerprint << std::dec
                                  << " rdma_reads=" << rdma_reads_this_insert
                                  << " rdma_writes=" << rdma_writes_this_insert);
                    kicked = true;
                    break;  // 跳出内层循环，重新读取下一个 batch
                } else {
                    // Robin Hood: 新元素距离更小或相等（更"富"），继续后移寻找位置
                    RBF_VERBOSE_LOG("  SKIP: new_dist(" << (int)new_slot.distance
                                    << ") <= existing_dist(" << (int)existing_slot.distance
                                    << ") - existing is poorer, keep searching");
                    continue;
                }
            }
        }

        // 如果遍历完当前 batch 的所有 slot 仍未插入成功且未发生踢出
        if (!kicked && curr_pos < batch_start + cli->k_slots_read) {
            curr_pos = batch_start + cli->k_slots_read;
            RBF_DEBUG_LOG("  BATCH COMPLETE without insert: moving to next batch at pos=" << curr_pos);
        }
    }

    // 踢出链过长，插入失败
    g_rbf_stats.insert_failures++;
    RBF_ERROR_LOG("INSERT[#" << insert_id << "] FAILED: kick chain too long (" << kick_chain_len
                  << " >= " << cli->max_kick_chain << ")"
                  << " key=" << key
                  << " original_hash=" << hash_pos
                  << " rdma_reads=" << rdma_reads_this_insert
                  << " rdma_writes=" << rdma_writes_this_insert);
    RdmaRBF_Cli_unlock(cli);
    return RBF_NotEnoughSpace;
}

// 查询操作
RBFStatus RdmaRBF_Cli_lookup(struct RdmaRBF_Cli *cli, uint64_t key) {
    g_rbf_stats.total_lookups++;
    uint64_t lookup_id = g_rbf_stats.total_lookups.load();

    uint32_t hash_pos;
    uint16_t fingerprint;
    RdmaRBF_Cli_generate_hash(cli, key, hash_pos, fingerprint);

    RBF_DEBUG_LOG_KEY("LOOKUP START [#" << lookup_id << "]", key);
    RBF_DEBUG_LOG("  hash_pos=" << hash_pos << " fingerprint=0x" << std::hex << fingerprint << std::dec
                  << " max_distance=" << cli->max_distance);

    // 加锁（查询也需要锁来保证一致性）
    RdmaRBF_Cli_lock(cli);

    // 读取从 hash_pos 开始的 M+1 个 slot
    uint32_t batch_start = (hash_pos / cli->k_slots_read) * cli->k_slots_read;
    int rdma_reads_this_lookup = 0;
    
    // 可能需要跨多个 batch 读取
    for (uint32_t pos = hash_pos; pos <= hash_pos + cli->max_distance && pos < cli->total_slots; pos++) {
        uint32_t current_batch = (pos / cli->k_slots_read) * cli->k_slots_read;
        uint32_t offset = pos - current_batch;

        // 如果进入了新的 batch，需要重新读取
        if (current_batch != batch_start) {
            batch_start = current_batch;
            int read_ret = RdmaRBF_Cli_read_slots(cli, batch_start, cli->k_slots_read);
            rdma_reads_this_lookup++;
            if (read_ret != 1) {
                RBF_ERROR_LOG("  LOOKUP[#" << lookup_id << "] READ FAILED at batch=" << batch_start);
                g_rbf_stats.lookup_not_found++;
                RdmaRBF_Cli_unlock(cli);
                return RBF_NotFound;
            }
        } else if (pos == hash_pos) {
            // 第一次读取
            int read_ret = RdmaRBF_Cli_read_slots(cli, batch_start, cli->k_slots_read);
            rdma_reads_this_lookup++;
            if (read_ret != 1) {
                RBF_ERROR_LOG("  LOOKUP[#" << lookup_id << "] FIRST READ FAILED at batch=" << batch_start);
                g_rbf_stats.lookup_not_found++;
                RdmaRBF_Cli_unlock(cli);
                return RBF_NotFound;
            }
        }

        // 检查当前 slot
        if (!RdmaRBF_Cli_slot_is_empty(cli, offset)) {
            rbf_slot slot;
            RdmaRBF_Cli_get_slot(cli, offset, slot);
            uint32_t target_dib = pos - hash_pos;

            RBF_DEBUG_LOG("  CHECK pos=" << pos << " target_dib=" << target_dib 
                          << " slot_dist=" << (int)slot.distance 
                          << " slot_fp=0x" << std::hex << slot.fingerprint << std::dec
                          << " (looking for fp=0x" << std::hex << fingerprint << std::dec << ")");

            // Robin Hood 查询终止条件
            if (slot.distance < target_dib) {
                RBF_DEBUG_LOG("  EARLY TERMINATION: slot.distance(" << (int)slot.distance
                              << ") < target_dib(" << target_dib << ")"
                              << " rdma_reads=" << rdma_reads_this_lookup);
                g_rbf_stats.lookup_not_found++;
                RdmaRBF_Cli_unlock(cli);
                return RBF_NotFound;
            }

            // 检查指纹是否匹配
            if (slot.fingerprint == fingerprint) {
                g_rbf_stats.lookup_found++;
                RBF_DEBUG_LOG_KEY("LOOKUP FOUND [#" << lookup_id << "]", key);
                RBF_DEBUG_LOG("  found_at_pos=" << pos << " distance=" << (int)slot.distance
                              << " rdma_reads=" << rdma_reads_this_lookup);
                RdmaRBF_Cli_unlock(cli);
                return RBF_Ok;
            }
        } else {
            // 遇到空 slot，元素不存在
            RBF_DEBUG_LOG("  EMPTY SLOT at pos=" << pos << " -> NOT FOUND"
                          << " rdma_reads=" << rdma_reads_this_lookup);
            g_rbf_stats.lookup_not_found++;
            RdmaRBF_Cli_unlock(cli);
            return RBF_NotFound;
        }
    }

    g_rbf_stats.lookup_not_found++;
    RBF_DEBUG_LOG_KEY("LOOKUP NOT FOUND [#" << lookup_id << "] (exhausted range)", key);
    RBF_DEBUG_LOG("  searched range=[" << hash_pos << ", " << hash_pos + cli->max_distance << "]"
                  << " rdma_reads=" << rdma_reads_this_lookup);
    RdmaRBF_Cli_unlock(cli);
    return RBF_NotFound;
}

// ================= RDMA RBF Server Implementation =================

void RdmaRBF_Srv_init(struct RdmaRBF_Srv *srv, uint32_t num_slots, uint32_t max_distance,
                      int client_count, const char* name_dev,
                      uint8_t rnic_port, uint32_t tcp_port, uint8_t gid_index) {
    memset(srv, 0, sizeof(*srv));

    srv->num_slots = upperpower2(num_slots);
    srv->max_distance = max_distance;
    srv->total_slots = srv->num_slots + srv->max_distance;  // 额外 M 个 slot 作为溢出缓冲区
    srv->count_clients_expected = client_count;
    srv->list_sockfd = std::vector<int>(client_count);
    srv->list_remote_info = std::vector<rdma_conn_info_rbf>(client_count);
    srv->use_hp = false;

    // 计算数据区大小（每个 slot 4 字节，分配 total_slots 个）
    srv->size_data = srv->total_slots * sizeof(uint32_t);

    // 计算 mutex 数量（简化：使用单个 mutex）
    srv->count_mutex = 1;

    // 分配空间
#ifdef TOGGLE_HUGEPAGE
    if (srv->size_data > 0 && srv->size_data % (1 << 21) == 0) {
        hugepage_alloc((void**)&(srv->data), srv->size_data);
        srv->use_hp = true;
    } else {
        alloc_aligned_64((void**)&srv->data, srv->size_data);
    }
#else
    alloc_aligned_64((void**)&srv->data, srv->size_data);
#endif
    alloc_aligned_64((void**)&srv->mutex_list, srv->count_mutex * sizeof(uint64_t));

    // 初始化为 0（空 slot）
    memset(srv->data, 0, srv->size_data);
    memset(srv->mutex_list, 0, srv->count_mutex * sizeof(uint64_t));

    // 输出信息
    std::cout << "[Server] RBF slots: " << srv->num_slots << " (+" << srv->max_distance << " overflow = " << srv->total_slots << " total)" << std::endl;
    std::cout << "[Server] RBF size(MB): " << srv->size_data / 1024.0 / 1024.0 << std::endl;
    std::cout << "[Server] Max distance M: " << srv->max_distance << std::endl;
    std::cout << "[Server] Expected client count: " << client_count << std::endl;
    if (srv->use_hp) std::cout << "[Server] Hugepage enabled for data region." << std::endl;

    // 建立连接
    RdmaRBF_Srv_conn(srv, client_count, name_dev, rnic_port, tcp_port, gid_index);
    return;
}

int RdmaRBF_Srv_conn(struct RdmaRBF_Srv *srv, int client_count, const char* name_dev,
                     uint8_t rnic_port, uint32_t tcp_port, uint8_t gid_index) {
    // 打开 ctx, 创建 pd, cq
    srv->ctx = open_rdma_ctx(name_dev);
    srv->pd = ibv_alloc_pd(srv->ctx);
    srv->cq = ibv_create_cq(srv->ctx, 32, NULL, NULL, 0);

    // 注册 mr
    srv->data_mr = ibv_reg_mr(srv->pd, srv->data, srv->size_data, MR_FLAGS_RW);
    srv->mutex_mr = ibv_reg_mr(srv->pd, srv->mutex_list, srv->count_mutex * sizeof(uint64_t), MR_FLAGS_ATOMIC);

    // 初始化 list_qp
    srv->list_qp = std::vector<ibv_qp *>(client_count);
    for (int i = 0; i < client_count; ++i) {
        srv->list_qp[i] = create_rc_qp(srv->pd, srv->cq);
        modify_init_qp(srv->list_qp[i], rnic_port);
    }

    // 创建并填充 local_info
    std::vector<rdma_conn_info_rbf *> list_local_info(client_count);
    for (int i = 0; i < client_count; i++) {
        list_local_info[i] = create_rdma_conn_info<rdma_conn_info_rbf>(srv->ctx, rnic_port, gid_index);
        list_local_info[i]->data_addr = (uintptr_t)srv->data;
        list_local_info[i]->data_rkey = srv->data_mr->rkey;
        list_local_info[i]->mutex_addr = (uintptr_t)srv->mutex_list;
        list_local_info[i]->mutex_rkey = srv->mutex_mr->rkey;
        list_local_info[i]->qp_num = srv->list_qp[i]->qp_num;
    }

    // 交换信息 over TCP
    tcp_exchange_server<rdma_conn_info_rbf>(tcp_port, client_count, srv->list_sockfd, 
                                            srv->list_remote_info, list_local_info);

    // 修改 QP 到 RTR 和 RTS
    for (int i = 0; i < client_count; i++) {
        modify_rtr_qp(srv->list_qp[i], srv->list_remote_info[i].qp_num, srv->list_remote_info[i].psn,
                      srv->list_remote_info[i].gid, gid_index, rnic_port);
        modify_rts_qp(srv->list_qp[i], list_local_info[i]->psn);
    }

    return 1;
}

void RdmaRBF_Srv_destroy(struct RdmaRBF_Srv *srv) {
    for (int i = 0; i < srv->count_clients_expected; i++) {
        if (srv->list_qp[i]) ibv_destroy_qp(srv->list_qp[i]);
        if (srv->list_sockfd[i] >= 0) close(srv->list_sockfd[i]);
    }
    if (srv->data_mr) ibv_dereg_mr(srv->data_mr);
    if (srv->mutex_mr) ibv_dereg_mr(srv->mutex_mr);
    if (srv->cq) ibv_destroy_cq(srv->cq);
    if (srv->pd) ibv_dealloc_pd(srv->pd);
    if (srv->ctx) ibv_close_device(srv->ctx);
    if (srv->data) {
        if (srv->use_hp) munmap(srv->data, srv->size_data);
        else free(srv->data);
    }
    if (srv->mutex_list) free(srv->mutex_list);
}
