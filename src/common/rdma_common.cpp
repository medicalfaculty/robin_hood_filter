#include "rdma_common.h"
#include "utils.h"
#include <cstdio>
#include <cstring>
#include <string>

#define COUNT_RETRY_CAS_MAX 1000

ibv_qp *create_rc_qp(ibv_pd *pd, ibv_cq *cq) {
    ibv_qp_init_attr qp_init_attr = {};
    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.cap.max_send_wr = 10;
    qp_init_attr.cap.max_recv_wr = 10;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    ibv_qp *qp = ibv_create_qp(pd, &qp_init_attr);
    assert_else(qp != nullptr, "Failed to create QP");
    return qp;
}

// flag: remote_write, remote_read, remote_atomic
int modify_init_qp(ibv_qp *qp, uint8_t port) {
    ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_INIT;
    // 网卡端口 1 默认走的是控制网络，2 才是数据网络
    attr.port_num = port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;
    return ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
}

int modify_rtr_qp(ibv_qp* qp, uint32_t remote_qpn, uint32_t remote_psn, const union ibv_gid& remote_gid, uint8_t sgid_index, uint8_t port) {
    ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = remote_psn;
    attr.max_dest_rd_atomic = 1;    // 典型值 1，足够处理对端读/原子操作
    attr.min_rnr_timer = 12;        // 典型值 12，约 0.5ms RNR NAK 重试间隔
    
    // 填充地址向量（Address Vector）
    attr.ah_attr.is_global = 1;
    attr.ah_attr.grh.dgid = remote_gid;
    attr.ah_attr.grh.sgid_index = sgid_index;
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.port_num = port;
    
    int mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    
    return ibv_modify_qp(qp, &attr, mask);
}

int modify_rts_qp(ibv_qp* qp, uint32_t sq_psn) {
    ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;               // 典型值 14，约 8ms 超时
    attr.retry_cnt = 7;              // 典型值 7，最大重试次数
    attr.rnr_retry = 7;              // 典型值 7，最大 RNR NAK 重试次数
    attr.sq_psn = sq_psn;            // 本地发送队列初始 PSN
    attr.max_rd_atomic = 1;          // 典型值 1，足够处理本地读/原子操作响应

    int mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    return ibv_modify_qp(qp, &attr, mask);
}

ibv_sge *create_sge(ibv_mr* mr) {
    ibv_sge *sge = (ibv_sge *)malloc(sizeof(ibv_sge));
    sge->addr = (uintptr_t)mr->addr;
    sge->length = mr->length;
    sge->lkey = mr->lkey;
    return sge;
}

rdma_conn_info *create_local_info(ibv_context *ctx, uint8_t port, uint8_t gid_index) {
    rdma_conn_info *local_info = (rdma_conn_info *)malloc(sizeof(rdma_conn_info));
    local_info->psn = lrand48() & 0xffffff;

    ibv_gid tmp_gid;
    ibv_query_gid(ctx, port, gid_index, &tmp_gid);
    memcpy(&local_info->gid, &tmp_gid, sizeof(ibv_gid));

    return local_info;
}

int rdma_one_side(ibv_qp *qp, int wr_id, ibv_sge *sge, uint64_t remote_addr, uint32_t rkey, ibv_wr_opcode opcode) {
    ibv_send_wr wr = {};
    wr.wr_id = wr_id;
    wr.opcode = opcode;
    wr.sg_list = sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    ibv_send_wr *bad_wr;
    int ret = ibv_post_send(qp, &wr, &bad_wr);
    if (ret != 0) {
        const char *op_name = (opcode == IBV_WR_RDMA_READ) ? "READ" :
                              (opcode == IBV_WR_RDMA_WRITE) ? "WRITE" : "UNKNOWN";
        std::cerr << "[RDMA_ERROR] ibv_post_send failed:"
                  << " op=" << op_name
                  << " wr_id=" << wr_id
                  << " remote_addr=0x" << std::hex << remote_addr << std::dec
                  << " rkey=0x" << std::hex << rkey << std::dec
                  << " sge.addr=0x" << std::hex << sge->addr << std::dec
                  << " sge.length=" << sge->length
                  << " sge.lkey=0x" << std::hex << sge->lkey << std::dec
                  << " errno=" << ret << " (" << strerror(ret) << ")"
                  << std::endl;
        assert_else(false, "ibv_post_send failed for RDMA " + std::string(op_name));
        return 0;
    }
    return 1;
}

int check_cq(ibv_cq *cq, int wr_id) {
    ibv_wc wc;
    int poll_count = 0;
    while (ibv_poll_cq(cq, 1, &wc) < 1) {
        poll_count++;
        if (poll_count % 1000000 == 0) {
            std::cout << "[RDMA_DIAG] check_cq: still polling for wr_id=" << wr_id
                      << " poll_count=" << poll_count << std::endl;
        }
    }
    if (wc.status != IBV_WC_SUCCESS) {
        std::cerr << "[RDMA_ERROR] CQ completion failed:"
                  << " wr_id=" << wc.wr_id << " (expected=" << wr_id << ")"
                  << " status=" << wc.status
                  << " (" << ibv_wc_status_str(wc.status) << ")"
                  << " vendor_err=0x" << std::hex << wc.vendor_err << std::dec
                  << " opcode=" << wc.opcode
                  << " byte_len=" << wc.byte_len
                  << " qp_num=" << wc.qp_num
                  << " src_qp=" << wc.src_qp
                  << std::endl;
        assert_else(false, "RDMA operation failed: " + std::string(ibv_wc_status_str(wc.status)));
        return 0;
    }
    if (wc.wr_id != (uint64_t)wr_id) {
        std::cerr << "[RDMA_ERROR] WR ID mismatch:"
                  << " got=" << wc.wr_id << " expected=" << wr_id
                  << " status=" << wc.status
                  << " opcode=" << wc.opcode
                  << std::endl;
        assert_else(false, "Mismatch wr_id: " + std::to_string(wc.wr_id) + " expected wr_id: " + std::to_string(wr_id));
        return 0;
    }
    return 1;
}

// compare_add: 旧期望值, swap: 新交换值
int rdma_atomic_cas(ibv_qp *qp, int wr_id, ibv_sge *sge, ibv_cq *cq, uint64_t remote_addr, uint32_t rkey, uint64_t compare_add, uint64_t swap) {
    ibv_send_wr wr = {};
    wr.wr_id = wr_id;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.sg_list = sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = remote_addr;
    wr.wr.atomic.rkey = rkey;
    wr.wr.atomic.compare_add = compare_add;
    wr.wr.atomic.swap = swap;

    *((uint64_t *)(sge->addr)) = compare_add + 1;

    ibv_send_wr *bad_wr;
    ibv_wc wc;
    // 自旋直到获取锁
    int retry = 0;
    while (retry++ < COUNT_RETRY_CAS_MAX) {
        if (ibv_post_send(qp, &wr, &bad_wr)) {
            assert_else(false, "ibv_post_send (CAS lock) failed");
            return 0;
        }
        while (ibv_poll_cq(cq, 1, &wc) < 1);
        if (wc.status != IBV_WC_SUCCESS) {
            assert_else(false, "CAS lock failed: " + std::string(ibv_wc_status_str(wc.status)));
            return 0;
        }
        if (wc.wr_id != (uint64_t)wr_id) {
            assert_else(false, "Mismatch wr_id in CAS: " + std::to_string(wc.wr_id) + " expected wr_id: " + std::to_string(wr_id));
            return 0;
        }
        // 如果返回值为 compare_add，说明成功获取锁
        if (*((uint64_t *)(sge->addr)) == compare_add) break;
        else usleep(200 + rand() % 300);
    }
    return retry < COUNT_RETRY_CAS_MAX ? 1 : 0;
}

ibv_context *open_rdma_ctx(const char* name_dev) {
    int num_devices = 0, index_device = 0;
    ibv_device **dev_list = ibv_get_device_list(&num_devices);
    if (num_devices <= 0) {
        assert_else(false, "No RDMA devices found");
        exit(1);
    }
    for (; index_device < num_devices; ++index_device) {
        ibv_device *dev = dev_list[index_device];
        if (strcmp(dev->name, name_dev) == 0) break;
    }
    if (index_device == num_devices) {
        assert_else(false, "Selected RDMA device not found");
        index_device = 0; // 默认使用第一个设备
    }
    ibv_context *ctx = ibv_open_device(dev_list[index_device]);
    ibv_free_device_list(dev_list);
    return ctx;
}
