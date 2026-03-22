# RDMA Robin Hood Bloom Filter (RBF) - Insert Performance Test

This repository contains a complete, self-contained codebase for testing the **insert performance** of a Robin Hood Bloom Filter implemented over **RDMA (Remote Direct Memory Access)** one-sided operations.

The RBF uses Robin Hood hashing with linear probing, where elements with larger probe distances can displace ("kick") elements with smaller distances. Each slot is encoded as an 18-bit value (12-bit fingerprint + 6-bit distance). All data resides on a remote server, accessed exclusively via RDMA Read/Write without server-side CPU involvement.

## Directory Structure

```
.
├── CMakeLists.txt              # Root build configuration
├── src/
│   ├── CMakeLists.txt          # Library build targets
│   ├── common/
│   │   ├── hash.h              # TwoIndependentMultiplyShift hash
│   │   ├── murmur3.h/cpp       # MurmurHash3 implementation
│   │   ├── rdma_common.h/cpp   # RDMA QP state transitions, one-sided ops
│   │   └── utils.h/cpp         # TCP communication, sync, memory allocation
│   └── rdma_rbf/
│       ├── rdma_rbf.h          # RBF struct definitions, macros (M, K, etc.)
│       └── rdma_rbf.cpp        # RBF insert/lookup, Robin Hood kick chain
├── test/
│   ├── CMakeLists.txt          # Test executable targets
│   ├── rbf_srv_test.cpp        # Server: allocate RBF memory, wait for client
│   ├── rbf_cli_test.cpp        # Client: insert/lookup test with CLI args
│   └── run_throughput_tests.sh # 9-round throughput automation script
└── README.md
```

## Environment Requirements

- **Two nodes** connected via RDMA network (InfiniBand or RoCE)
- **libibverbs** development package (`libibverbs-dev` or `rdma-core-devel`)
- **CMake** >= 3.5
- **C++11** compatible compiler (GCC recommended)
- **OS**: Linux (tested on Ubuntu with Mellanox ConnectX adapters)

## Network Configuration (Hardcoded Parameters)

Before building, you **must** modify the hardcoded network parameters in the test source files to match your environment:

### `test/rbf_cli_test.cpp` (client side)

```cpp
#define SERVER_IP "10.10.1.1"     // Server node IP on RDMA network
#define RNIC_NAME "mlx4_0"        // RDMA NIC device name (check: ibv_devices)
#define RNIC_PORT (2)             // RDMA NIC port number
#define GID_INDEX (0)             // GID table index
#define TCP_PORT (18516)          // TCP port for initial info exchange
```

### `test/rbf_srv_test.cpp` (server side)

```cpp
#define RNIC_NAME "mlx4_0"        // Same RDMA NIC config
#define RNIC_PORT (2)
#define GID_INDEX (0)
#define TCP_PORT (18516)          // Must match client
```

Use `ibv_devices` and `ibv_devinfo` on each node to find the correct RNIC_NAME and RNIC_PORT.

## Build Instructions

```bash
git clone git@github.com:medicalfaculty/robin_hood_filter.git
cd robin_hood_filter

mkdir build && cd build

# Single-client lock-free mode (recommended for performance testing)
cmake .. -DTOGGLE_LOCK_FREE=ON
make -j$(nproc)
```

### CMake Options

| Option | Default | Description |
|--------|---------|-------------|
| `TOGGLE_LOCK_FREE` | OFF | Enable lock-free mode (skip mutex, single client only) |
| `TOGGLE_HUGEPAGE` | OFF | Use 2MB hugepages for memory allocation |

## Running Tests

### Step 1: Deploy to Both Nodes

Copy the entire `build/` directory (or at minimum `build/test/rbf_srv_test` and `build/test/rbf_cli_test`) to both server and client nodes.

### Step 2: Start Server

On the **server** node:

```bash
./test/rbf_srv_test
```

The server allocates 8M slots of RBF memory and waits for client connections.

### Step 3: Start Client

On the **client** node:

```bash
# Default: insert 8M elements, run all 3 phases (insert + 2 lookup phases)
./test/rbf_cli_test

# Custom insert count, stop on first failure
./test/rbf_cli_test 5000000 stop

# Insert only (skip lookup), for throughput measurement
./test/rbf_cli_test 838861 nolookup
```

### Client CLI Arguments

```
Usage: ./rbf_cli_test [insert_count] [stop|nolookup]

  insert_count   Number of elements to insert (default: 8388608 = 8M)
  stop           Stop immediately on first insert failure
  nolookup       Skip lookup phases (insert-only throughput test)
```

Options can be combined: `./rbf_cli_test 4194304 nolookup`

## 9-Round Throughput Test

The script `test/run_throughput_tests.sh` automates 9 rounds of insert-only throughput tests at different load factors (10% to 90% of 8M slots).

### Before Running

Edit the SSH parameters at the top of the script to match your environment:

```bash
SERVER="user@server-address"
CLIENT_VIA_SERVER="ssh -i ~/.ssh/key user@client-address"
KEY="-i ~/.ssh/key"
```

The script assumes:
- You can SSH from local machine to server
- The server can SSH to the client
- The built binaries are at `exp1/build/` on both nodes

### Test Matrix

| Round | Load Factor | Insert Count |
|-------|-------------|-------------|
| 1 | 10% | 838,861 |
| 2 | 20% | 1,677,722 |
| 3 | 30% | 2,516,582 |
| 4 | 40% | 3,355,443 |
| 5 | 50% | 4,194,304 |
| 6 | 60% | 5,033,165 |
| 7 | 70% | 5,872,025 |
| 8 | 80% | 6,710,886 |
| 9 | 90% | 7,549,747 |

### Running

```bash
chmod +x test/run_throughput_tests.sh
./test/run_throughput_tests.sh
```

Results are saved to `/tmp/rbf_throughput_results.txt` in CSV format.

## RBF Parameter Tuning

Key parameters are defined in `src/rdma_rbf/rdma_rbf.h`:

| Parameter | Current Value | Description |
|-----------|---------------|-------------|
| `RBF_MAX_DISTANCE_M` | 63 | Maximum probe distance (6-bit field, max = 63) |
| `RBF_K_SLOTS_READ` | 64 | Slots per RDMA batch read (must be power of 2, >= M) |
| `RBF_MAX_KICK_CHAIN` | 500 | Maximum Robin Hood displacement chain length |
| `RBF_FINGERPRINT_BITS` | 12 | Bits for fingerprint in each slot |
| `RBF_DISTANCE_BITS` | 6 | Bits for distance in each slot |

Test parameters are in `test/rbf_cli_test.cpp` and `test/rbf_srv_test.cpp`:

| Parameter | Current Value | Description |
|-----------|---------------|-------------|
| `RBF_NUM_SLOTS` | 8,388,608 (8M) | Total number of hash table slots |
| `DEFAULT_INSERT_COUNT` | 8,388,608 | Default elements to insert (overridable via CLI) |
| `LOOKUP_COUNT` | 1,000 | Elements to lookup in each lookup phase |

### Rules for Choosing K

- K **must** be a power of 2
- K **must** be >= M (max distance)
- Recommended: K = next power of 2 >= M (e.g., M=63 -> K=64)
- Non-power-of-2 K values that don't evenly divide num_slots will cause batch alignment overflow

## Known Issues: Batch Alignment Fix

When K does not evenly divide `num_slots`, the batch calculation `(pos / K) * K` can produce a range `[start, start+K)` that exceeds `total_slots` (= num_slots + max_distance). This causes RDMA operations to go out of the Memory Region boundary, putting the QP in Error state and causing all subsequent operations to fail.

The fix (already applied in this codebase) clamps `actual_count` in both `read_slots()` and `write_slots()`:

```cpp
if (start_slot_idx + actual_count > cli->total_slots) {
    actual_count = cli->total_slots - start_slot_idx;
}
```

This ensures RDMA operations never exceed the registered Memory Region, regardless of K value.
