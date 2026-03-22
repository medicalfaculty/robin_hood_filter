#include <random>
#include <algorithm>
#include <functional>
// #include <openssl/rand.h>
#include <cstring>
#include <stdexcept>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <errno.h>
#include <ctime>
#include <iostream>
#include "utils.h"

std::vector<uint64_t> GenerateRandom64Cuckoo(std::size_t count) {
  std::vector<uint64_t> result(count);
  std::random_device random;
  // To generate random keys to lookup, this uses ::std::random_device which is slower but
  // stronger than some other pseudo-random alternatives. The reason is that some of these
  // alternatives (like libstdc++'s ::std::default_random, which is a linear congruential
  // generator) behave non-randomly under some hash families like Dietzfelbinger's
  // multiply-shift.
  auto genrand = [&random]() {
    return random() + (static_cast<uint64_t>(random()) << 32);
  };
  std::generate(result.begin(), result.end(), std::ref(genrand));
  return result;
}

// Deepseek + Kimi
std::vector<uint64_t> GenerateRandom64(std::size_t count) {
    std::mt19937_64 generator(std::random_device{}());
    
    std::vector<uint64_t> result(count);
    std::generate(result.begin(), result.end(), std::ref(generator));
    return result;
}

// std::vector<uint64_t> GenerateRandomOpenSSL(std::size_t count) {
//     std::vector<uint64_t> vals(count);
//     if (RAND_bytes((unsigned char *)vals.data(), sizeof(uint64_t) * count) != 1) {
//         throw std::runtime_error("OpenSSL RAND_bytes failed");
//     }
//     return vals;
// }

int reliable_send(int sockfd, const void *data, uint32_t length) {
    if (0 == length) return 0;
    uint32_t total_sent = 0;
    const char *buffer = static_cast<const char *>(data);

    while (total_sent < length) {
        uint32_t sent = send(sockfd, buffer + total_sent, length - total_sent, 0);
        if (sent < 0) {
            if (errno == EINTR) continue; // 被信号中断，重试
            std::cerr << "Failed to send data over socket. errno: " << errno << std::endl;
            return -1;
        }
        else if (sent == 0) {
            std::cerr << "Connection closed by peer during send" << std::endl;
            return -1;
        }
        total_sent += sent;
    }
    return total_sent;
}

int reliable_recv(int sockfd, void *data, uint32_t length) {
    if (0 == length) return 0;
    uint32_t total_received = 0;
    char *buffer = static_cast<char *>(data);

    while (total_received < length) {
        uint32_t received = recv(sockfd, buffer + total_received, length - total_received, 0);
        if (received < 0) {
            if (errno == EINTR) continue; // 被信号中断，重试
            std::cerr << "Failed to receive data over socket. errno: " << errno << std::endl;
            return -1;
        }
        else if (received == 0) {
            std::cerr << "Connection closed by peer during recv" << std::endl;
            return -1;
        }
        total_received += received;
    }
    return total_received;
}

void assert_else(bool condition, const std::string& message, bool exit_on_failure) {
    if (!condition) std::cerr << "[Assert Failure] " << message << std::endl;
    if (!condition && exit_on_failure) exit(-1);
}

void alloc_aligned_64(void** ptr, std::size_t size) {
    int ret = posix_memalign(ptr, 64, size);
    assert_else(ret == 0, "posix_memalign failed");
    memset(*ptr, 0, size);
    return;
}

void sync_client(int const &sockfd) {
    char cmd[6];
    reliable_send(sockfd, "READY", 6);
    reliable_recv(sockfd, cmd, 3);
    std::cout << "[Client] " << get_current_time_string() << " Sync point reached." << std::endl;
    return;
}

void sync_server(std::vector<int> const &list_sockfd) {
    char cmd[6];
    for (auto const &sockfd : list_sockfd) {
        reliable_recv(sockfd, cmd, 6);
    }
    for (auto const &sockfd : list_sockfd) {
        reliable_send(sockfd, "GO", 3);
    }
    std::cout << "[Server] " << get_current_time_string() << " Sync point reached." << std::endl;
    return;
}

std::string get_current_time_string() {
    std::time_t now = std::time(nullptr);
    char buf[100];
    std::strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S]", std::localtime(&now));
    return std::string(buf);
}

// 2MB per hugepage
void hugepage_alloc(void** ptr, uint64_t const size) {
    assert_else(size % (1 << 21) == 0, "Hugepage size must be multiple of 2MB");
    
    // 使用进程ID和时间戳创建唯一文件名，避免多进程冲突
    char filename[256];
    snprintf(filename, sizeof(filename), "/mnt/huge/rdma_filter_hugepage_%d_%ld", getpid(), time(NULL));
    
    // 先删除可能存在的旧文件
    unlink(filename);
    
    int fd = open(filename, O_CREAT | O_RDWR | O_EXCL, 0600);
    if (fd < 0) {
        std::cerr << "Failed to open hugepage file: " << filename << ", errno: " << errno << " (" << strerror(errno) << ")" << std::endl;
        assert_else(false, "Failed to open hugepage file");
    }
    
    if (ftruncate(fd, size) != 0) {
        std::cerr << "ftruncate failed for hugepage file, errno: " << errno << " (" << strerror(errno) << ")" << std::endl;
        close(fd);
        unlink(filename);
        assert_else(false, "ftruncate failed for hugepage file");
    }

    *ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (*ptr == MAP_FAILED) {
        std::cerr << "mmap failed for hugepage file, errno: " << errno << " (" << strerror(errno) << ")" << std::endl;
        std::cerr << "Requested size: " << size << " bytes (" << (size >> 21) << " hugepages)" << std::endl;
        close(fd);
        unlink(filename);
        assert_else(false, "mmap failed for hugepage file");
    }
    
    memset(*ptr, 0, size);
    close(fd);
    
    // 删除文件，但内存映射仍然有效
    unlink(filename);
    
    std::cout << "[Hugepage] Successfully allocated " << (size >> 21) << " hugepages (" << (size >> 20) << " MB)" << std::endl;
    return;
}
