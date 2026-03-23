#!/bin/bash
# RBF Lookup Performance Test Script
# 20-round batch insert + interleaved lookup (Test A: TP, Test B: FP)
#
# This script automates the lookup test on two RDMA nodes.
# Output is saved to log files to survive SSH disconnections.

# ============ EDIT THESE PARAMETERS ============
SERVER="user@server-address"           # SSH address of server node
CLIENT="user@client-address"           # SSH address of client node (from server)
KEY="-i ~/.ssh/your_key"               # SSH key for server -> client
BUILD_DIR="exp1/build"                 # Build directory on both nodes
# ===============================================

SRV_LOG="/tmp/rbf_lookup_srv.log"
CLI_LOG="/tmp/rbf_lookup_cli.log"

echo "============================================"
echo "  RBF Lookup Performance Test"
echo "  20-Round Batch Insert + Interleaved Lookup"
echo "  M=63, K=64, Slots=8M"
echo "============================================"
echo ""

# Step 1: Kill any existing test processes
echo "[1/5] Cleaning up old processes..."
ssh $KEY $SERVER "pkill -f rbf_lookup_srv_test 2>/dev/null"
ssh $KEY $SERVER "ssh $KEY $CLIENT 'pkill -f rbf_lookup_cli_test 2>/dev/null'" 2>/dev/null
sleep 2

# Step 2: Start server (nohup to survive SSH disconnect)
echo "[2/5] Starting server..."
ssh $KEY $SERVER "cd $BUILD_DIR && nohup ./test_lookup/rbf_lookup_srv_test > $SRV_LOG 2>&1 &"
sleep 3

# Verify server is running
SRV_PID=$(ssh $KEY $SERVER "pgrep -f rbf_lookup_srv_test" 2>/dev/null)
if [ -z "$SRV_PID" ]; then
    echo "ERROR: Server failed to start. Check $SRV_LOG on server."
    exit 1
fi
echo "  Server started (PID: $SRV_PID)"

# Step 3: Start client (nohup to survive SSH disconnect)
echo "[3/5] Starting client (20 rounds, may take 30+ minutes)..."
ssh $KEY $SERVER "ssh $KEY $CLIENT 'cd $BUILD_DIR && nohup ./test_lookup/rbf_lookup_cli_test > $CLI_LOG 2>&1 &'"
sleep 3

CLI_PID=$(ssh $KEY $SERVER "ssh $KEY $CLIENT 'pgrep -f rbf_lookup_cli_test'" 2>/dev/null)
if [ -z "$CLI_PID" ]; then
    echo "ERROR: Client failed to start. Check $CLI_LOG on client."
    ssh $KEY $SERVER "pkill -f rbf_lookup_srv_test 2>/dev/null"
    exit 1
fi
echo "  Client started (PID: $CLI_PID)"

# Step 4: Poll for completion
echo "[4/5] Waiting for test completion..."
echo "  (Monitor progress: ssh $CLIENT 'tail -f $CLI_LOG')"
echo ""

while true; do
    # Check if client is still running
    CLI_ALIVE=$(ssh $KEY $SERVER "ssh $KEY $CLIENT 'pgrep -f rbf_lookup_cli_test'" 2>/dev/null)
    if [ -z "$CLI_ALIVE" ]; then
        echo "  Client process finished."
        break
    fi

    # Show latest round progress
    LATEST=$(ssh $KEY $SERVER "ssh $KEY $CLIENT 'grep \"ROUND\" $CLI_LOG | tail -1'" 2>/dev/null)
    echo "  $(date '+%H:%M:%S') - $LATEST"
    sleep 30
done

# Step 5: Retrieve results
echo ""
echo "[5/5] Retrieving results..."
echo ""
echo "============================================"
echo "  RESULTS"
echo "============================================"

# Print summary table
ssh $KEY $SERVER "ssh $KEY $CLIENT 'cat $CLI_LOG'" 2>/dev/null

# Save results locally
LOCAL_RESULTS="/tmp/rbf_lookup_results.txt"
ssh $KEY $SERVER "ssh $KEY $CLIENT 'cat $CLI_LOG'" > $LOCAL_RESULTS 2>/dev/null

echo ""
echo "============================================"
echo "  Test complete. Full output saved to:"
echo "  Local:  $LOCAL_RESULTS"
echo "  Client: $CLI_LOG"
echo "  Server: $SRV_LOG"
echo "============================================"

# Cleanup server process
ssh $KEY $SERVER "pkill -f rbf_lookup_srv_test 2>/dev/null"
