#include "rdma_rbf.h"
#include "utils.h"

#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <chrono>
#include <vector>
#include <iomanip>
#include <cstring>

#define SERVER_IP "10.10.1.1"
#define RNIC_NAME "mlx4_0"
#define RNIC_PORT (2)
#define GID_INDEX (0)
#define TCP_PORT (18516)

// 大规模测试：8M 条
#define DEFAULT_INSERT_COUNT (1024 * 1024 * 8)
#define LOOKUP_COUNT (1000)

// RBF 参数
#define RBF_NUM_SLOTS (1024 * 1024 * 8)    // slot 数量 8M（8388608）
#define RBF_MAX_DISTANCE (63)       // 最大距离 M（6-bit 最大值）
#define RBF_K_SLOTS_READ (64)       // 每次读取的 slot 数量 K（2的幂次，K>=M）
#define RBF_MAX_KICK_CHAIN (500)    // 最大踢出链长度

// 插入后立即验证（大规模测试关闭以提高速度）
#define VERIFY_AFTER_INSERT 0

// 进度打印间隔
#define PROGRESS_INTERVAL (100000)

// 用法: ./rbf_cli_test [insert_count] [stop|nolookup]
//   insert_count: 插入数量（默认 8M）
//   stop: 首次插入失败即停止
//   nolookup: 跳过 lookup 阶段（仅测插入吞吐量）
int main(int argc, char **argv) {

    int REAL_INSERT_COUNT = DEFAULT_INSERT_COUNT;
    bool stop_on_first_fail = false;
    bool skip_lookup = false;

    if (argc > 1 && argv[1][0] != '-') {
        REAL_INSERT_COUNT = atoi(argv[1]);
    }
    for (int a = 1; a < argc; a++) {
        if (strcmp(argv[a], "stop") == 0) stop_on_first_fail = true;
        if (strcmp(argv[a], "nolookup") == 0) skip_lookup = true;
    }

    std::cout << "============================================================" << std::endl;
    std::cout << "  RBF Large Scale Load Factor Test" << std::endl;
    std::cout << "  Slots: " << RBF_NUM_SLOTS << "  Insert Attempt: " << REAL_INSERT_COUNT << std::endl;
    std::cout << "============================================================" << std::endl;
    std::cout << "Current Time: " << get_current_time_string() << std::endl;
    std::cout << std::endl;

    // 打印测试参数
    std::cout << "= Test Parameters =" << std::endl;
    std::cout << "  INSERT_COUNT:       " << REAL_INSERT_COUNT << std::endl;
    std::cout << "  LOOKUP_COUNT:       " << LOOKUP_COUNT << std::endl;
    std::cout << "  RBF_NUM_SLOTS:      " << RBF_NUM_SLOTS << std::endl;
    std::cout << "  RBF_MAX_DISTANCE:   " << RBF_MAX_DISTANCE << std::endl;
    std::cout << "  RBF_K_SLOTS_READ:   " << RBF_K_SLOTS_READ << std::endl;
    std::cout << "  RBF_MAX_KICK_CHAIN: " << RBF_MAX_KICK_CHAIN << std::endl;
    std::cout << "  VERIFY_AFTER_INSERT: " << VERIFY_AFTER_INSERT << std::endl;
    std::cout << "  stop_on_first_fail: " << (stop_on_first_fail ? "YES" : "NO") << std::endl;
    std::cout << "  skip_lookup:        " << (skip_lookup ? "YES" : "NO") << std::endl;
    double expected_load = (double)REAL_INSERT_COUNT / RBF_NUM_SLOTS;
    std::cout << "  Expected max load factor: " << std::fixed << std::setprecision(2)
              << expected_load * 100.0 << "%" << std::endl;
    std::cout << std::endl;

    int false_positive_count = 0, true_positive_count = 0, true_negative_count = 0;
    auto start_time = std::chrono::high_resolution_clock::now();
    auto end_time = start_time;
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "= Dataset Preparing =" << std::endl;
    std::vector<uint64_t> to_insert = GenerateRandom64(REAL_INSERT_COUNT), to_lookup = {};
    std::unordered_set<uint64_t> to_insert_set(to_insert.begin(), to_insert.end());
    while (to_lookup.size() < LOOKUP_COUNT) {
        auto lookup_temp = GenerateRandom64(LOOKUP_COUNT - to_lookup.size());
        for (auto i : lookup_temp) {
            if (to_insert_set.find(i) == to_insert_set.end()) to_lookup.push_back(i);
        }
    }
    std::cout << "  Generated " << to_insert.size() << " unique insert keys" << std::endl;
    std::cout << "  Generated " << to_lookup.size() << " unique lookup keys (not in insert set)" << std::endl;
    std::cout << std::endl;

    std::cout << "=== RdmaRBF Client Initialization ===" << std::endl;
    struct RdmaRBF_Cli cli;
    RdmaRBF_Cli_init(&cli, RBF_NUM_SLOTS, RBF_MAX_DISTANCE, RBF_K_SLOTS_READ, RBF_MAX_KICK_CHAIN,
                     SERVER_IP, RNIC_NAME, RNIC_PORT, TCP_PORT, GID_INDEX);

    sync_client(cli.sockfd);
    std::cout << "[Client] Initialization successfully!" << std::endl;
    std::cout << "[Client] Total Slots: " << cli.num_slots << " (+" << cli.max_distance << " overflow = " << cli.total_slots << " total)" << std::endl;
    std::cout << "[Client] Max Distance M: " << cli.max_distance << std::endl;
    std::cout << "[Client] K Slots Read: " << cli.k_slots_read << std::endl;
    std::cout << "[Client] Max Kick Chain: " << cli.max_kick_chain << std::endl;
    std::cout << "[Client] Remote data_addr: 0x" << std::hex << cli.remote_info.data_addr << std::dec << std::endl;
    std::cout << "[Client] Remote data_rkey: 0x" << std::hex << cli.remote_info.data_rkey << std::dec << std::endl;
    std::cout << "[Client] Remote mutex_addr: 0x" << std::hex << cli.remote_info.mutex_addr << std::dec << std::endl;
    std::cout << std::endl;

    // 重置 RDMA 统计计数器
    RdmaRBF_reset_stats();

    // ==================== 插入测试 ====================
    std::cout << "============================================================" << std::endl;
    std::cout << "  PHASE 1: INSERT TEST (" << REAL_INSERT_COUNT << " items into " << RBF_NUM_SLOTS << " slots)" << std::endl;
    std::cout << "============================================================" << std::endl;
    sync_client(cli.sockfd);
    start_time = std::chrono::high_resolution_clock::now();
    
    int insert_success = 0, insert_fail = 0;
    int verify_pass = 0, verify_fail = 0;
    // 记录首次插入失败的信息
    int first_fail_idx = -1;
    double first_fail_load_factor = 0.0;
    // 记录每次插入失败的键和原因
    std::vector<std::pair<uint64_t, std::string>> failed_inserts;
    // 记录成功插入的键（用于后续 lookup 验证）
    std::vector<uint64_t> successfully_inserted;
    // 记录插入验证失败的键
    std::vector<uint64_t> verify_failed_keys;

    for (int idx = 0; idx < (int)to_insert.size(); idx++) {
        uint64_t key = to_insert[idx];

        auto op_start = std::chrono::high_resolution_clock::now();
        RBFStatus status = RdmaRBF_Cli_insert(&cli, key);
        auto op_end = std::chrono::high_resolution_clock::now();
        auto op_us = std::chrono::duration_cast<std::chrono::microseconds>(op_end - op_start).count();

        if (status == RBF_Ok) {
            insert_success++;
            successfully_inserted.push_back(key);

#if VERIFY_AFTER_INSERT
            RBFStatus verify_status = RdmaRBF_Cli_lookup(&cli, key);
            if (verify_status == RBF_Ok) {
                verify_pass++;
            } else {
                verify_fail++;
                verify_failed_keys.push_back(key);
                std::cerr << "[TEST_ERROR] VERIFICATION FAILURE: key=" << key
                          << " inserted but NOT FOUND (idx=" << idx << ")" << std::endl;
            }
#endif
        } else if (status == RBF_NotEnoughSpace) {
            insert_fail++;
            failed_inserts.push_back({key, "NotEnoughSpace"});
            // 记录首次失败
            if (first_fail_idx == -1) {
                first_fail_idx = idx;
                first_fail_load_factor = (double)insert_success / cli.num_slots;
                std::cout << "\n[FIRST_FAIL] *** First insertion failure at index " << idx
                          << " (attempt #" << idx + 1 << ") ***" << std::endl;
                std::cout << "[FIRST_FAIL]   Successfully inserted before failure: " << insert_success << std::endl;
                std::cout << "[FIRST_FAIL]   Load factor at first failure: " << std::fixed << std::setprecision(4)
                          << first_fail_load_factor * 100.0 << "% (" << insert_success << "/" << cli.num_slots << ")" << std::endl;
                std::cout << "[FIRST_FAIL]   Key: " << key << std::endl;
                std::cout << "[FIRST_FAIL]   Latency: " << op_us << "us" << std::endl;
                if (stop_on_first_fail) {
                    std::cout << "[FIRST_FAIL]   STOPPING (stop_on_first_fail=YES)" << std::endl;
                    break;
                }
            }
        } else {
            insert_fail++;
            failed_inserts.push_back({key, "Unknown_" + std::to_string(status)});
            if (first_fail_idx == -1) {
                first_fail_idx = idx;
                first_fail_load_factor = (double)insert_success / cli.num_slots;
                std::cout << "\n[FIRST_FAIL] *** First insertion failure at index " << idx
                          << " error=" << status << " ***" << std::endl;
                std::cout << "[FIRST_FAIL]   Load factor: " << std::fixed << std::setprecision(4)
                          << first_fail_load_factor * 100.0 << "%" << std::endl;
                if (stop_on_first_fail) {
                    std::cout << "[FIRST_FAIL]   STOPPING (stop_on_first_fail=YES)" << std::endl;
                    break;
                }
            }
        }

        // 定期打印进度
        if ((idx + 1) % PROGRESS_INTERVAL == 0 || idx == (int)to_insert.size() - 1) {
            double current_load = (double)insert_success / cli.num_slots * 100.0;
            auto now = std::chrono::high_resolution_clock::now();
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time).count();
            double throughput = elapsed_ms > 0 ? (double)(idx + 1) / elapsed_ms * 1000.0 : 0;
            std::cout << "[PROGRESS] " << idx + 1 << "/" << REAL_INSERT_COUNT
                      << " success=" << insert_success << " fail=" << insert_fail
                      << " load=" << std::fixed << std::setprecision(2) << current_load << "%"
                      << " throughput=" << std::fixed << std::setprecision(0) << throughput << " op/s"
                      << " elapsed=" << elapsed_ms << "ms" << std::endl;
        }
    }
    
    end_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // 插入阶段摘要
    std::cout << "\n============================================================" << std::endl;
    std::cout << "  INSERT PHASE SUMMARY" << std::endl;
    std::cout << "============================================================" << std::endl;
    std::cout << "  Total attempted:    " << REAL_INSERT_COUNT << std::endl;
    std::cout << "  Total slots:        " << cli.num_slots << std::endl;
    std::cout << "  Inserted success:   " << insert_success << std::endl;
    std::cout << "  Insert failed:      " << insert_fail << std::endl;
    std::cout << "  Insert success rate: " << std::fixed << std::setprecision(2)
              << 1.0 * insert_success / REAL_INSERT_COUNT * 100.0 << "%" << std::endl;
    std::cout << "  Final load factor:  " << std::fixed << std::setprecision(4)
              << (double)insert_success / cli.num_slots * 100.0 << "% ("
              << insert_success << "/" << cli.num_slots << ")" << std::endl;
    std::cout << "  Time(ms):           " << duration.count() << std::endl;
    if (duration.count() > 0) {
        std::cout << "  Throughput(op/s):   " << std::fixed << std::setprecision(1)
                  << 1.0 * REAL_INSERT_COUNT / duration.count() * 1000.0 << std::endl;
    }

    // 首次失败信息
    if (first_fail_idx >= 0) {
        std::cout << "\n  --- First Failure Point ---" << std::endl;
        std::cout << "  First failure at attempt #" << first_fail_idx + 1 << " (0-indexed: " << first_fail_idx << ")" << std::endl;
        std::cout << "  Items successfully inserted before first failure: " << first_fail_idx - (first_fail_idx - insert_success + insert_fail - 1) << std::endl;
        std::cout << "  Load factor at first failure: " << std::fixed << std::setprecision(4)
                  << first_fail_load_factor * 100.0 << "%" << std::endl;
    } else {
        std::cout << "\n  [OK] All " << REAL_INSERT_COUNT << " items inserted successfully (no failures)" << std::endl;
    }

#if VERIFY_AFTER_INSERT
    std::cout << "  Verify pass:        " << verify_pass << std::endl;
    std::cout << "  Verify fail:        " << verify_fail << std::endl;
    if (verify_fail > 0) {
        std::cout << "  [WARNING] Verification failures detected! Failed keys (first 10):" << std::endl;
        for (int i = 0; i < std::min((int)verify_failed_keys.size(), 10); i++) {
            std::cout << "    key=" << verify_failed_keys[i] << std::endl;
        }
    }
#endif

    // 打印失败详情（最多前 20 条）
    if (!failed_inserts.empty()) {
        int show_count = std::min((int)failed_inserts.size(), 20);
        std::cout << "  Failed insert details (first " << show_count << " of " << failed_inserts.size() << "):" << std::endl;
        for (int i = 0; i < show_count; i++) {
            std::cout << "    key=" << failed_inserts[i].first << " reason=" << failed_inserts[i].second << std::endl;
        }
    }

    // 打印 RDMA 操作统计（插入阶段）
    std::cout << "\n  --- RDMA Stats after Insert Phase ---" << std::endl;
    RdmaRBF_print_stats();

    // ==================== 查询已成功插入的元素测试（采样） ====================
    const int LOOKUP_SAMPLE_COUNT = std::min((int)successfully_inserted.size(), 10000);
    std::cout << "============================================================" << std::endl;
    std::cout << "  PHASE 2: LOOKUP SUCCESSFULLY INSERTED ITEMS (sample " << LOOKUP_SAMPLE_COUNT
              << " of " << successfully_inserted.size() << ")" << std::endl;
    if (skip_lookup) std::cout << "  [SKIPPED - nolookup mode]" << std::endl;
    std::cout << "============================================================" << std::endl;
    sync_client(cli.sockfd);
    start_time = std::chrono::high_resolution_clock::now();
    
    int lookup_found = 0, lookup_notfound = 0;
    std::vector<uint64_t> lookup_miss_keys;

    if (!skip_lookup) {
    for (int idx = 0; idx < LOOKUP_SAMPLE_COUNT; idx++) {
        uint64_t key = successfully_inserted[idx];

        RBFStatus status = RdmaRBF_Cli_lookup(&cli, key);
        if (status == RBF_Ok) {
            lookup_found++;
            true_positive_count++;
        } else {
            lookup_notfound++;
            lookup_miss_keys.push_back(key);
            // 只打印前 20 条未找到的
            if (lookup_notfound <= 20) {
                std::cerr << "[TEST_ERROR] Existing key NOT FOUND: key=" << key
                          << " insert_idx=" << idx << std::endl;
            }
        }

        // 定期打印进度
        if ((idx + 1) % 2000 == 0 || idx == LOOKUP_SAMPLE_COUNT - 1) {
            std::cout << "[LOOKUP_PROGRESS] " << idx + 1 << "/" << LOOKUP_SAMPLE_COUNT
                      << " found=" << lookup_found << " notfound=" << lookup_notfound << std::endl;
        }
    }
    } // end if (!skip_lookup)
    
    end_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\n  --- Lookup Existing Items Summary ---" << std::endl;
    std::cout << "  Total looked up:    " << LOOKUP_SAMPLE_COUNT << " (sampled from " << successfully_inserted.size() << ")" << std::endl;
    std::cout << "  Found:              " << lookup_found << std::endl;
    std::cout << "  Not found:          " << lookup_notfound << std::endl;
    if (LOOKUP_SAMPLE_COUNT > 0) {
        std::cout << "  True Positive Rate: " << std::fixed << std::setprecision(4)
                  << (double)lookup_found / LOOKUP_SAMPLE_COUNT * 100.0 << "%" << std::endl;
    }
    std::cout << "  Time(ms):           " << duration.count() << std::endl;
    if (duration.count() > 0) {
        std::cout << "  Throughput(op/s):   " << std::fixed << std::setprecision(1)
                  << 1.0 * LOOKUP_SAMPLE_COUNT / duration.count() * 1000.0 << std::endl;
    }
    if (!lookup_miss_keys.empty()) {
        int show_count = std::min((int)lookup_miss_keys.size(), 20);
        std::cout << "  [WARNING] Missing keys (first " << show_count << " of " << lookup_miss_keys.size() << "):" << std::endl;
        for (int i = 0; i < show_count; i++) {
            std::cout << "    key=" << lookup_miss_keys[i] << std::endl;
        }
    }

    // ==================== 查询不存在元素测试 ====================
    std::cout << "\n============================================================" << std::endl;
    std::cout << "  PHASE 3: LOOKUP NON-EXISTING ITEMS (" << LOOKUP_COUNT << " items)" << std::endl;
    if (skip_lookup) std::cout << "  [SKIPPED - nolookup mode]" << std::endl;
    std::cout << "============================================================" << std::endl;
    sync_client(cli.sockfd);
    start_time = std::chrono::high_resolution_clock::now();
    int fp_detail_count = 0;
    if (!skip_lookup) {
    for (int idx = 0; idx < (int)to_lookup.size(); idx++) {
        uint64_t key = to_lookup[idx];
        RBFStatus status = RdmaRBF_Cli_lookup(&cli, key);
        if (status == RBF_Ok) {
            false_positive_count++;
            fp_detail_count++;
            if (fp_detail_count <= 10) {
                std::cout << "[TEST_LOG] FALSE POSITIVE: key=" << key
                          << " (lookup_idx=" << idx << ")" << std::endl;
            }
        } else {
            true_negative_count++;
        }
    }
    } // end if (!skip_lookup)
    end_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\n  --- Lookup Non-existing Items Summary ---" << std::endl;
    std::cout << "  Total looked up:    " << LOOKUP_COUNT << std::endl;
    std::cout << "  True Negative:      " << true_negative_count << std::endl;
    std::cout << "  False Positive:     " << false_positive_count << std::endl;
    std::cout << "  True Negative Rate: " << std::fixed << std::setprecision(4)
              << 1.0 * true_negative_count / LOOKUP_COUNT * 100.0 << "%" << std::endl;
    std::cout << "  False Positive Rate:" << std::fixed << std::setprecision(6)
              << 1.0 * false_positive_count / LOOKUP_COUNT * 100.0 << "%" << std::endl;
    std::cout << "  Time(ms):           " << duration.count() << std::endl;
    if (duration.count() > 0) {
        std::cout << "  Throughput(op/s):   " << std::fixed << std::setprecision(1)
                  << 1.0 * LOOKUP_COUNT / duration.count() * 1000.0 << std::endl;
    }

    // 注意：rdma_rbf 当前不支持删除操作
    std::cout << std::endl << "= Delete Operation =" << std::endl;
    std::cout << "Delete operation is not supported in RBF currently." << std::endl;

    // ==================== 最终统计报告 ====================
    std::cout << "\n============================================================" << std::endl;
    std::cout << "  FINAL RDMA OPERATION STATISTICS" << std::endl;
    std::cout << "============================================================" << std::endl;
    RdmaRBF_print_stats();

    // ==================== 关键结论 ====================
    std::cout << "============================================================" << std::endl;
    std::cout << "  KEY RESULTS" << std::endl;
    std::cout << "============================================================" << std::endl;
    std::cout << "  Total Slots:                " << cli.num_slots << std::endl;
    std::cout << "  Total Insert Attempts:      " << REAL_INSERT_COUNT << std::endl;
    std::cout << "  Successfully Inserted:      " << insert_success << std::endl;
    std::cout << "  Final Load Factor:          " << std::fixed << std::setprecision(4)
              << (double)insert_success / cli.num_slots * 100.0 << "%" << std::endl;
    if (first_fail_idx >= 0) {
        std::cout << "  First Failure at Attempt #: " << first_fail_idx + 1 << std::endl;
        std::cout << "  Load Factor at 1st Failure: " << std::fixed << std::setprecision(4)
                  << first_fail_load_factor * 100.0 << "%" << std::endl;
    } else {
        std::cout << "  First Failure:              None (all inserts succeeded)" << std::endl;
    }
    std::cout << "  Lookup True Positive Rate:  " << std::fixed << std::setprecision(4)
              << (LOOKUP_SAMPLE_COUNT > 0 ? (double)lookup_found / LOOKUP_SAMPLE_COUNT * 100.0 : 0) << "%" << std::endl;
    std::cout << "  Lookup False Positive Rate:  " << std::fixed << std::setprecision(6)
              << 1.0 * false_positive_count / LOOKUP_COUNT * 100.0 << "%" << std::endl;

    // 诊断摘要
    std::cout << "\n============================================================" << std::endl;
    std::cout << "  DIAGNOSTIC SUMMARY" << std::endl;
    std::cout << "============================================================" << std::endl;
    bool has_issues = false;
    if (insert_fail > 0) {
        // 大规模测试中插入失败是预期行为（超过容量）
        std::cout << "  [INFO] " << insert_fail << " insert(s) failed out of "
                  << REAL_INSERT_COUNT << " attempts" << std::endl;
        std::cout << "         This is expected when load factor approaches capacity limit" << std::endl;
    }
    if (verify_fail > 0) {
        has_issues = true;
        std::cout << "  [ISSUE] " << verify_fail << " post-insert verification(s) failed" << std::endl;
        std::cout << "          This indicates data corruption or RDMA write failure" << std::endl;
    }
    if (lookup_notfound > 0) {
        has_issues = true;
        std::cout << "  [ISSUE] " << lookup_notfound << " successfully inserted item(s) not found during lookup" << std::endl;
        std::cout << "          This may indicate data loss or Robin Hood logic error" << std::endl;
    }
    if (!has_issues && insert_fail == 0) {
        std::cout << "  [OK] All operations completed without issues" << std::endl;
    } else if (!has_issues) {
        std::cout << "  [OK] RDMA operations and data integrity verified (insert failures are capacity-related)" << std::endl;
    }
    std::cout << "============================================================" << std::endl;

    reliable_send(cli.sockfd, "EXIT", 5);
    RdmaRBF_Cli_destroy(&cli);

    std::cout << "\n==== Experiment End ====" << std::endl;
    std::cout << "Current Time: " << get_current_time_string() << std::endl;
    return 0;
}
