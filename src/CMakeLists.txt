add_library(header INTERFACE)
target_include_directories(header INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/common
    ${CMAKE_CURRENT_SOURCE_DIR}/rdma_rbf
)

add_library(murmur STATIC "common/murmur3.cpp")
target_link_libraries(murmur PUBLIC header)
target_compile_options(murmur PRIVATE -Ofast)

add_library(utils STATIC "common/utils.cpp")
target_link_libraries(utils PUBLIC header)
target_compile_options(utils PRIVATE -Ofast)

add_library(rdma_common STATIC "common/rdma_common.cpp")
target_link_libraries(rdma_common PUBLIC header utils)
target_compile_options(rdma_common PRIVATE -Ofast)

add_library(rdmarbf STATIC "rdma_rbf/rdma_rbf.cpp")
target_link_libraries(rdmarbf PUBLIC header murmur utils rdma_common ibverbs)
target_compile_options(rdmarbf PRIVATE -Ofast)

if(TOGGLE_LOCK_FREE)
    target_compile_definitions(rdma_common PUBLIC TOGGLE_LOCK_FREE)
    target_compile_definitions(rdmarbf PUBLIC TOGGLE_LOCK_FREE)
endif()

if(TOGGLE_HUGEPAGE)
    target_compile_definitions(rdmarbf PUBLIC TOGGLE_HUGEPAGE)
endif()
