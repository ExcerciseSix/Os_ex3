cmake_minimum_required(VERSION 3.12)
project(ex3)

set(CMAKE_CXX_STANDARD 14)

add_executable(ex3 MapReduceFramework.h MapREduceFramework.cpp Barrier.cpp Barrier.h SampleClient.cpp )
SET(CMAKE_CXX_FLAGS -pthread)
