#include "rdma_rbf.h"
#include "utils.h"

#include <iostream>
#include <unordered_set>
#include <chrono>
#include <vector>
#include <iomanip>
#include <cstring>

#define SERVER_IP "10.10.1.1"
#define RNIC_NAME "mlx4_0"
#define RNIC_PORT (2)
#define GID_INDEX (0)
#define TCP_PORT (18516)

// RBF 参数（与 rbf_cli_test 保持一致）
#define RBF_NUM_SLOTS (1024 * 1024 * 8)    // 8M slots
#define RBF_MAX_DISTANCE (63)
// RBF_K_SLOTS_READ (64) 和 RBF_MAX_KICK_CHAIN (500) 已在 rdma_rbf.h 中定义

// Lookup 测试参数
#define TOTAL_INSERT (1024 * 1024 * 8)     // 总插入量 8M
#define BATCH_COUNT (20)                    // 分 20 批
#define FP_LOOKUP_COUNT (200000)            // 假阳性测试查询数量

// 进度打印间隔
#define PROGRESS_INTERVAL (500000)

int main(int argc, char **argv) {

    std::cout << "============================================================" << std::endl;
    std::cout << "  RBF Lookup Performance Test" << std::endl;
    std::cout << "  20-Round Batch Insert + Interleaved Lookup" << std::endl;
    std::cout << "============================================================" << std::endl;
    std::cout << "Current Time: " << get_current_time_string() << std::endl;
    std::cout << std::endl;

    // 打印参数
    int batch_size = TOTAL_INSERT / BATCH_COUNT;
    std::cout << "= Test Parameters =" << std::endl;
    std::cout << "  RBF_NUM_SLOTS:      " << RBF_NUM_SLOTS << std::endl;
    std::cout << "  RBF_MAX_DISTANCE:   " << RBF_MAX_DISTANCE << std::endl;
    std::cout << "  RBF_K_SLOTS_READ:   " << RBF_K_SLOTS_READ << std::endl;
    std::cout << "  RBF_MAX_KICK_CHAIN: " << RBF_MAX_KICK_CHAIN << std::endl;
    std::cout << "  TOTAL_INSERT:       " << TOTAL_INSERT << std::endl;
    std::cout << "  BATCH_COUNT:        " << BATCH_COUNT << std::endl;
    std::cout << "  BATCH_SIZE:         " << batch_size << " (last batch: " << (TOTAL_INSERT - (BATCH_COUNT - 1) * batch_size) << ")" << std::endl;
    std::cout << "  FP_LOOKUP_COUNT:    " << FP_LOOKUP_COUNT << std::endl;
    std::cout << std::endl;

    // ==================== 数据准备 ====================
    std::cout << "= Dataset Preparing =" << std::endl;
    std::cout << "  Generating " << TOTAL_INSERT << " random keys for insertion..." << std::endl;
    std::vector<uint64_t> to_insert = GenerateRandom64(TOTAL_INSERT);
    std::unordered_set<uint64_t> to_insert_set(to_insert.begin(), to_insert.end());
    std::cout << "  Generated " << to_insert.size() << " insert keys" << std::endl;

    std::cout << "  Generating " << FP_LOOKUP_COUNT << " non-existing keys for FP test..." << std::endl;
    std::vector<uint64_t> fp_keys;
    fp_keys.reserve(FP_LOOKUP_COUNT);
    while ((int)fp_keys.size() < FP_LOOKUP_COUNT) {
        int need = FP_LOOKUP_COUNT - (int)fp_keys.size();
        auto tmp = GenerateRandom64(need + need / 10);  // generate extra to account for collisions
        for (auto k : tmp) {
            if (to_insert_set.find(k) == to_insert_set.end()) {
                fp_keys.push_back(k);
                if ((int)fp_keys.size() >= FP_LOOKUP_COUNT) break;
            }
        }
    }
    std::cout << "  Generated " << fp_keys.size() << " non-existing keys" << std::endl;
    std::cout << std::endl;

    // ==================== RDMA 初始化 ====================
    std::cout << "=== RdmaRBF Client Initialization ===" << std::endl;
    struct RdmaRBF_Cli cli;
    RdmaRBF_Cli_init(&cli, RBF_NUM_SLOTS, RBF_MAX_DISTANCE, RBF_K_SLOTS_READ, RBF_MAX_KICK_CHAIN,
                     SERVER_IP, RNIC_NAME, RNIC_PORT, TCP_PORT, GID_INDEX);

    sync_client(cli.sockfd);
    std::cout << "[Client] Initialization successfully!" << std::endl;
    std::cout << "[Client] Total Slots: " << cli.num_slots << " (+" << cli.max_distance
              << " overflow = " << cli.total_slots << " total)" << std::endl;
    std::cout << std::endl;

    RdmaRBF_reset_stats();

    // 用于追踪所有成功插入的 key
    std::vector<uint64_t> successfully_inserted;
    successfully_inserted.reserve(TOTAL_INSERT);

    int total_insert_success = 0, total_insert_fail = 0;

    // ==================== 打印结果表头 ====================
    std::cout << "============================================================" << std::endl;
    std::cout << "  STARTING 20-ROUND BATCH INSERT + LOOKUP TEST" << std::endl;
    std::cout << "============================================================" << std::endl;
    std::cout << std::endl;

    // 存储每轮结果用于最终汇总表
    struct RoundResult {
        int round;
        double load_pct;
        int inserted_so_far;
        int batch_success;
        int batch_fail;
        double testA_kops;
        double testA_tp_pct;
        int testA_found;
        int testA_not_found;
        double testB_kops;
        double testB_fp_pct;
        int testB_fp_count;
    };
    std::vector<RoundResult> results;

    auto total_start_time = std::chrono::high_resolution_clock::now();

    // ==================== 20 轮测试 ====================
    for (int round = 1; round <= BATCH_COUNT; round++) {
        int batch_start = (round - 1) * batch_size;
        int batch_end = (round == BATCH_COUNT) ? TOTAL_INSERT : round * batch_size;
        int this_batch_size = batch_end - batch_start;

        std::cout << "------------------------------------------------------------" << std::endl;
        std::cout << "  ROUND " << round << "/" << BATCH_COUNT
                  << ": Insert batch [" << batch_start << ", " << batch_end << ") = " << this_batch_size << " items" << std::endl;
        std::cout << "------------------------------------------------------------" << std::endl;

        // --- 插入本批次 ---
        auto insert_start = std::chrono::high_resolution_clock::now();
        int batch_success = 0, batch_fail = 0;

        for (int idx = batch_start; idx < batch_end; idx++) {
            uint64_t key = to_insert[idx];
            RBFStatus status = RdmaRBF_Cli_insert(&cli, key);
            if (status == RBF_Ok) {
                batch_success++;
                successfully_inserted.push_back(key);
            } else {
                batch_fail++;
            }

            // 进度打印
            int done = idx - batch_start + 1;
            if (done % PROGRESS_INTERVAL == 0 || idx == batch_end - 1) {
                auto now = std::chrono::high_resolution_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - insert_start).count();
                double tp = elapsed > 0 ? (double)done / elapsed * 1000.0 : 0;
                std::cout << "  [INSERT] " << done << "/" << this_batch_size
                          << " success=" << batch_success << " fail=" << batch_fail
                          << " " << std::fixed << std::setprecision(0) << tp << " op/s" << std::endl;
            }
        }

        auto insert_end = std::chrono::high_resolution_clock::now();
        auto insert_ms = std::chrono::duration_cast<std::chrono::milliseconds>(insert_end - insert_start).count();

        total_insert_success += batch_success;
        total_insert_fail += batch_fail;
        double load_pct = (double)total_insert_success / cli.num_slots * 100.0;

        std::cout << "  Batch insert done: success=" << batch_success << " fail=" << batch_fail
                  << " time=" << insert_ms << "ms"
                  << " total_inserted=" << total_insert_success
                  << " load=" << std::fixed << std::setprecision(2) << load_pct << "%" << std::endl;

        // --- Test A: Lookup 所有已插入元素 ---
        std::cout << "  [Test A] Lookup all " << successfully_inserted.size() << " inserted elements..." << std::endl;
        auto testA_start = std::chrono::high_resolution_clock::now();
        int testA_found = 0, testA_not_found = 0;

        for (int i = 0; i < (int)successfully_inserted.size(); i++) {
            RBFStatus status = RdmaRBF_Cli_lookup(&cli, successfully_inserted[i]);
            if (status == RBF_Ok) {
                testA_found++;
            } else {
                testA_not_found++;
            }

            // 每 100 万条打印一次进度
            if ((i + 1) % 1000000 == 0) {
                auto now = std::chrono::high_resolution_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - testA_start).count();
                double kops = elapsed > 0 ? (double)(i + 1) / elapsed : 0;
                std::cout << "    [A progress] " << (i + 1) << "/" << successfully_inserted.size()
                          << " found=" << testA_found << " " << std::fixed << std::setprecision(2)
                          << kops << " KOPS" << std::endl;
            }
        }

        auto testA_end = std::chrono::high_resolution_clock::now();
        auto testA_ms = std::chrono::duration_cast<std::chrono::milliseconds>(testA_end - testA_start).count();
        double testA_kops = testA_ms > 0 ? (double)successfully_inserted.size() / testA_ms : 0;
        double testA_tp_pct = successfully_inserted.size() > 0
            ? (double)testA_found / successfully_inserted.size() * 100.0 : 0;

        std::cout << "  [Test A] Done: found=" << testA_found << " not_found=" << testA_not_found
                  << " TP=" << std::fixed << std::setprecision(4) << testA_tp_pct << "%"
                  << " time=" << testA_ms << "ms"
                  << " KOPS=" << std::fixed << std::setprecision(2) << testA_kops << std::endl;

        // --- Test B: Lookup 20 万个不存在的元素 ---
        std::cout << "  [Test B] Lookup " << FP_LOOKUP_COUNT << " non-existing elements..." << std::endl;
        auto testB_start = std::chrono::high_resolution_clock::now();
        int testB_fp_count = 0;

        for (int i = 0; i < FP_LOOKUP_COUNT; i++) {
            RBFStatus status = RdmaRBF_Cli_lookup(&cli, fp_keys[i]);
            if (status == RBF_Ok) {
                testB_fp_count++;
            }
        }

        auto testB_end = std::chrono::high_resolution_clock::now();
        auto testB_ms = std::chrono::duration_cast<std::chrono::milliseconds>(testB_end - testB_start).count();
        double testB_kops = testB_ms > 0 ? (double)FP_LOOKUP_COUNT / testB_ms : 0;
        double testB_fp_pct = (double)testB_fp_count / FP_LOOKUP_COUNT * 100.0;

        std::cout << "  [Test B] Done: FP=" << testB_fp_count << "/" << FP_LOOKUP_COUNT
                  << " FP_rate=" << std::fixed << std::setprecision(4) << testB_fp_pct << "%"
                  << " time=" << testB_ms << "ms"
                  << " KOPS=" << std::fixed << std::setprecision(2) << testB_kops << std::endl;

        // 记录结果
        RoundResult r;
        r.round = round;
        r.load_pct = load_pct;
        r.inserted_so_far = total_insert_success;
        r.batch_success = batch_success;
        r.batch_fail = batch_fail;
        r.testA_kops = testA_kops;
        r.testA_tp_pct = testA_tp_pct;
        r.testA_found = testA_found;
        r.testA_not_found = testA_not_found;
        r.testB_kops = testB_kops;
        r.testB_fp_pct = testB_fp_pct;
        r.testB_fp_count = testB_fp_count;
        results.push_back(r);

        std::cout << std::endl;
    }

    auto total_end_time = std::chrono::high_resolution_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(total_end_time - total_start_time).count();

    // ==================== 汇总结果表 ====================
    std::cout << "============================================================" << std::endl;
    std::cout << "  SUMMARY TABLE" << std::endl;
    std::cout << "============================================================" << std::endl;
    std::cout << std::endl;

    // 表头
    std::cout << std::left
              << std::setw(6)  << "Round"
              << std::setw(9)  << "Load%"
              << std::setw(12) << "Inserted"
              << std::setw(10) << "Batch_OK"
              << std::setw(10) << "Batch_Fail"
              << std::setw(12) << "TestA_KOPS"
              << std::setw(11) << "TestA_TP%"
              << std::setw(12) << "TestB_KOPS"
              << std::setw(11) << "TestB_FP%"
              << std::setw(10) << "FP_Count"
              << std::endl;

    std::cout << std::string(101, '-') << std::endl;

    for (auto &r : results) {
        std::cout << std::left
                  << std::setw(6)  << r.round
                  << std::fixed << std::setprecision(2) << std::setw(9) << r.load_pct
                  << std::setw(12) << r.inserted_so_far
                  << std::setw(10) << r.batch_success
                  << std::setw(10) << r.batch_fail
                  << std::fixed << std::setprecision(2) << std::setw(12) << r.testA_kops
                  << std::fixed << std::setprecision(4) << std::setw(11) << r.testA_tp_pct
                  << std::fixed << std::setprecision(2) << std::setw(12) << r.testB_kops
                  << std::fixed << std::setprecision(4) << std::setw(11) << r.testB_fp_pct
                  << r.testB_fp_count
                  << std::endl;
    }

    std::cout << std::endl;
    std::cout << "Total test time: " << total_ms << " ms (" << std::fixed << std::setprecision(1)
              << total_ms / 60000.0 << " min)" << std::endl;
    std::cout << "Total insert: success=" << total_insert_success << " fail=" << total_insert_fail << std::endl;

    // ==================== RDMA 统计 ====================
    std::cout << std::endl;
    std::cout << "============================================================" << std::endl;
    std::cout << "  FINAL RDMA OPERATION STATISTICS" << std::endl;
    std::cout << "============================================================" << std::endl;
    RdmaRBF_print_stats();

    // ==================== 清理 ====================
    reliable_send(cli.sockfd, "EXIT", 5);
    RdmaRBF_Cli_destroy(&cli);

    std::cout << "\n==== Experiment End ====" << std::endl;
    std::cout << "Current Time: " << get_current_time_string() << std::endl;
    return 0;
}
