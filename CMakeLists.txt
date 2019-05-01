cmake_minimum_required(VERSION 3.12)
project(OSEX3)

set(CMAKE_CXX_STANDARD 14)

add_executable(OSEX3 MapReduceFramework.cpp MapReduceFramework.h Barrier.cpp Barrier.h SampleClient.cpp)