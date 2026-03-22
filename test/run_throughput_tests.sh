#!/bin/bash
# RBF 吞吐量测试脚本：M=63 K=64，9组不同插入数量
# 每组从空表开始，测量平均插入吞吐量

SERVER="shuchen@128.110.217.61"
CLIENT_VIA_SERVER="ssh -i ~/.ssh/cloudlab_simple shuchen@ms1029.utah.cloudlab.us"
KEY="-i ~/.ssh/cloudlab_simple"

# 9组插入数量: 0.1*8M ~ 0.9*8M
COUNTS=(838861 1677722 2516582 3355443 4194304 5033165 5872025 6710886 7549747)
LABELS=("0.1" "0.2" "0.3" "0.4" "0.5" "0.6" "0.7" "0.8" "0.9")

echo "============================================"
echo "  RBF Throughput Test Suite (M=63 K=64)"
echo "  Slots: 8388608, 9 test rounds"
echo "============================================"
echo ""

RESULTS_FILE="/tmp/rbf_throughput_results.txt"
echo "ratio,insert_count,throughput_ops,time_ms,success,fail" > $RESULTS_FILE

for i in "${!COUNTS[@]}"; do
    COUNT=${COUNTS[$i]}
    LABEL=${LABELS[$i]}
    LOG="/tmp/rbf_cli_tp_${LABEL}.log"
    SRV_LOG="/tmp/rbf_srv_tp_${LABEL}.log"
    
    echo "=== Round $((i+1))/9: ${LABEL}*8M = ${COUNT} inserts ==="
    
    # 杀掉旧进程
    ssh $KEY $SERVER "pkill -f rbf_srv_test 2>/dev/null; $CLIENT_VIA_SERVER 'pkill -f rbf_cli_test 2>/dev/null'" 2>/dev/null
    sleep 2
    
    # 启动服务端
    ssh $KEY $SERVER "cd exp1/build && nohup ./test/rbf_srv_test > $SRV_LOG 2>&1 &"
    sleep 3
    
    # 启动客户端（nolookup 模式跳过 lookup，只测插入）
    ssh $KEY $SERVER "$CLIENT_VIA_SERVER 'cd exp1/build && ./test/rbf_cli_test ${COUNT} nolookup > ${LOG} 2>&1'"
    
    # 提取结果
    THROUGHPUT=$(ssh $KEY $SERVER "$CLIENT_VIA_SERVER 'grep \"Throughput\" ${LOG} | head -1'" 2>/dev/null | grep -oP '[0-9]+\.[0-9]+' | head -1)
    TIME_MS=$(ssh $KEY $SERVER "$CLIENT_VIA_SERVER 'grep \"Time(ms)\" ${LOG} | head -1'" 2>/dev/null | grep -oP '[0-9]+' | tail -1)
    SUCCESS=$(ssh $KEY $SERVER "$CLIENT_VIA_SERVER 'grep \"Inserted success\" ${LOG}'" 2>/dev/null | grep -oP '[0-9]+' | tail -1)
    FAIL=$(ssh $KEY $SERVER "$CLIENT_VIA_SERVER 'grep \"Insert failed\" ${LOG}'" 2>/dev/null | grep -oP '[0-9]+' | tail -1)
    
    echo "  Throughput: ${THROUGHPUT} op/s, Time: ${TIME_MS} ms, Success: ${SUCCESS}, Fail: ${FAIL}"
    echo "${LABEL},${COUNT},${THROUGHPUT},${TIME_MS},${SUCCESS},${FAIL}" >> $RESULTS_FILE
    
    # 停掉服务端
    ssh $KEY $SERVER "pkill -f rbf_srv_test 2>/dev/null"
    sleep 1
done

echo ""
echo "============================================"
echo "  All tests completed. Results:"
echo "============================================"
cat $RESULTS_FILE
