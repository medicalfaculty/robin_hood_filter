add_executable(rbf_srv_test "rbf_srv_test.cpp")
target_link_libraries(rbf_srv_test PRIVATE
    header
    murmur
    rdma_common
    utils
    ibverbs
    rdmarbf
)
target_compile_options(rbf_srv_test PRIVATE -Ofast)

add_executable(rbf_cli_test "rbf_cli_test.cpp")
target_link_libraries(rbf_cli_test PRIVATE
    header
    murmur
    rdma_common
    utils
    ibverbs
    rdmarbf
)
target_compile_options(rbf_cli_test PRIVATE -Ofast)
