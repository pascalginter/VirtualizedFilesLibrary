cmake_minimum_required(VERSION 3.12)
project(VirtualizedFilesLibrary VERSION 0.0.1)

find_package(GTest REQUIRED)

# -------------------------------------------------------------------------------
# C++ build options
# -------------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}-g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -g")
add_compile_options(-march=native -Wall -Wextra)

add_executable(Test test/test.cpp)
target_link_libraries(Test PRIVATE arrow parquet GTest::gtest_main)
