# RDMA Robin Hood Bloom Filter (RBF) - Lookup Performance Test

This directory contains a self-contained **lookup (query) performance test** for the RDMA Robin Hood Bloom Filter. It measures lookup throughput (KOPS) and accuracy (True Positive / False Positive rates) at different load factors from 5% to 100%.

This test is **independent** from the insert performance test in `../test/`. Both share the same RBF core implementation in `../src/` but have separate test programs, build targets, and automation scripts.

## Relationship to Insert Performance Test

| Feature | Insert Test (`../test/`) | Lookup Test (this directory) |
|---------|--------------------------|------------------------------|
| Purpose | Measure insert throughput at different load factors | Measure lookup throughput and accuracy at different load factors |
| Test Flow | Single-pass full insert, then sample lookup | 20-round batch insert + interleaved full lookup |
| Sync Protocol | 4 sync points (init, insert, lookup-existing, lookup-nonexisting) | 1 sync point (init only, client drives all 20 rounds) |
| Server Program | `rbf_srv_test` | `rbf_lookup_srv_test` |
| Client Program | `rbf_cli_test` | `rbf_lookup_cli_test` |
| Lookup Scale | Sample 10,000 existing + 1,000 non-existing | Full scan all inserted + 200,000 non-existing per round |
| Output | Insert throughput (KOPS) per load factor | Lookup KOPS + TP% + FP% per load factor |

## Test Design

The test divides 8,388,608 (8M) elements into 20 equal batches (419,430 each = 5% load per batch). After each batch insertion:

1. **Test A (True Positive)**: Query **all** successfully inserted elements so far. Measures lookup KOPS and TP rate.
2. **Test B (False Positive)**: Query **200,000** non-existing elements. Measures lookup KOPS and FP rate.

This produces 20 rounds x 2 tests = **40 lookup performance measurements** across load factors from 5% to 100%.

## Directory Structure

```
test_lookup/
├── CMakeLists.txt              # Build targets for lookup test
├── README.md                   # This file
├── rbf_lookup_cli_test.cpp     # Client: 20-round batch insert + lookup
├── rbf_lookup_srv_test.cpp     # Server: single sync, wait for EXIT
└── run_lookup_test.sh          # Automation script
```

## Environment Requirements

- **Two nodes** connected via RDMA network (InfiniBand or RoCE)
- **libibverbs** development package (`libibverbs-dev` or `rdma-core-devel`)
- **CMake** >= 3.5
- **C++11** compatible compiler (GCC recommended)
- **OS**: Linux (tested on Ubuntu 20.04 with Mellanox ConnectX adapters)
- **memlock**: Must be set to `unlimited` in `/etc/security/limits.conf`

## Network Configuration

Before building, modify the hardcoded network parameters in the test source files:

### `rbf_lookup_cli_test.cpp` (client side)

```cpp
#define SERVER_IP "10.10.1.1"     // Server node IP on RDMA network
#define RNIC_NAME "mlx4_0"        // RDMA NIC device name (check: ibv_devices)
#define RNIC_PORT (2)             // RDMA NIC port number
#define GID_INDEX (0)             // GID table index
#define TCP_PORT (18516)          // TCP port for initial info exchange
```

### `rbf_lookup_srv_test.cpp` (server side)

```cpp
#define RNIC_NAME "mlx4_0"
#define RNIC_PORT (2)
#define GID_INDEX (0)
#define TCP_PORT (18516)          // Must match client
```

Use `ibv_devices` and `ibv_devinfo` on each node to find the correct RNIC_NAME and RNIC_PORT.

## Build Instructions

Build from the repository root (builds both insert and lookup tests):

```bash
cd robin_hood_filter
mkdir build && cd build

# Single-client lock-free mode (recommended)
cmake .. -DTOGGLE_LOCK_FREE=ON
make -j$(nproc)
```

The lookup test binaries will be at:
- `build/test_lookup/rbf_lookup_srv_test`
- `build/test_lookup/rbf_lookup_cli_test`

## Running the Test

### Manual Execution

**Step 1**: Deploy binaries to both nodes. Copy the entire `build/` directory or at minimum:
- Server node: `build/test_lookup/rbf_lookup_srv_test`
- Client node: `build/test_lookup/rbf_lookup_cli_test`

**Step 2**: Start server on the server node:

```bash
./test_lookup/rbf_lookup_srv_test
```

**Step 3**: Start client on the client node:

```bash
./test_lookup/rbf_lookup_cli_test
```

The client will automatically execute all 20 rounds and print results.

### Automated Execution (Recommended)

Edit the SSH parameters at the top of `run_lookup_test.sh`:

```bash
SERVER="user@server-address"
CLIENT="user@client-address"
KEY="-i ~/.ssh/your_key"
BUILD_DIR="exp1/build"
```

Then run:

```bash
chmod +x test_lookup/run_lookup_test.sh
./test_lookup/run_lookup_test.sh
```

The script uses `nohup` and log files to survive SSH disconnections. This is important because the full test can take 30+ minutes (high load rounds have very slow insert phases).

## Output Format

The client prints per-round results during execution and a summary table at the end:

```
Round  Load%    Inserted    Batch_OK  Batch_Fail  TestA_KOPS  TestA_TP%   TestB_KOPS  TestB_FP%   FP_Count
-----------------------------------------------------------------------------------------------------
1      5.00     419430      419430    0           131.52      100.0000    131.41      0.0010      2
2      10.00    838860      419430    0           131.32      100.0000    131.23      0.0025      5
...
17     85.00    7130310     419430    0           125.65      100.0000    124.92      0.0760      152
```

### Key Metrics

| Metric | Description |
|--------|-------------|
| TestA_KOPS | Lookup throughput for querying existing elements (thousands of operations per second) |
| TestA_TP% | True Positive rate: percentage of inserted elements successfully found |
| TestB_KOPS | Lookup throughput for querying non-existing elements |
| TestB_FP% | False Positive rate: percentage of non-existing elements incorrectly reported as found |
| FP_Count | Absolute number of false positives out of 200,000 queries |

## RBF Parameters

Key parameters are defined in `../src/rdma_rbf/rdma_rbf.h` (shared with insert test):

| Parameter | Value | Description |
|-----------|-------|-------------|
| `RBF_MAX_DISTANCE_M` | 63 | Maximum Robin Hood probe distance (6-bit max) |
| `RBF_K_SLOTS_READ` | 64 | Slots per RDMA batch read (power of 2, >= M) |
| `RBF_MAX_KICK_CHAIN` | 500 | Maximum displacement chain length |
| `RBF_FINGERPRINT_BITS` | 12 | Fingerprint bits per slot |
| `RBF_DISTANCE_BITS` | 6 | Distance bits per slot |

Test-specific parameters in `rbf_lookup_cli_test.cpp`:

| Parameter | Value | Description |
|-----------|-------|-------------|
| `RBF_NUM_SLOTS` | 8,388,608 (8M) | Total hash table slots |
| `TOTAL_INSERT` | 8,388,608 | Total elements to insert across all rounds |
| `BATCH_COUNT` | 20 | Number of insertion rounds |
| `FP_LOOKUP_COUNT` | 200,000 | Non-existing elements queried per round |

## Expected Results Summary (M=63 K=64)

Based on actual test runs (17 rounds, 5%-85% load):

- **Lookup KOPS**: Extremely stable at 125-131 KOPS across all load factors (only ~4.5% variation)
- **TP Rate**: 100.0000% for all rounds below the first-failure threshold (87.16%)
- **FP Rate**: Very low, increasing from 0.001% (5% load) to 0.076% (85% load)
- **Lookup vs Insert**: At 85% load, lookup is 18x faster than insert (125 KOPS vs 6.85 KOPS)

Note: Rounds beyond 85% load were not completed because the insert phase becomes extremely slow at high load factors (Robin Hood kick chains grow very long). The 17-round data covers the practical operating range of the RBF.
