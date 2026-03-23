cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 11)

project(RBF_TEST)

option(TOGGLE_LOCK_FREE "enable lock-free" OFF)
option(TOGGLE_HUGEPAGE "enable hugepage allocation" OFF)

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(test_lookup)
