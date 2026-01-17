cmake_minimum_required(VERSION 3.10)
project(WebSocketSharedMemory)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required libraries
find_package(Threads REQUIRED)

# For WebSocket, choose one:
# Option 1: libwebsockets (lightweight, C-based)
# find_package(LibWebSockets REQUIRED)

# Option 2: Boost.Beast (C++, part of Boost)
find_package(Boost REQUIRED COMPONENTS system json url)

# Option 3: uWebSockets (very fast, C++)
# You'll need to include uWebSockets manually

# Include directories
include_directories(${Boost_INCLUDE_DIRS})

# Source files
set(SOURCES
    btcturk_websocket_shared_memory.cpp
)

set(BINANCE_SOURCES
    binance_websocket_shared_memory.cpp
)

# Create shared library
add_library(websocket_shm SHARED ${SOURCES})

# Create Binance shared library
add_library(binance_websocket_shm SHARED ${BINANCE_SOURCES})

# Create executable client
add_executable(websocket_shm_client
    btcturk_websocket_client_main.cpp
    btcturk_websocket_shared_memory.cpp
)

# Create Binance executable client
add_executable(binance_websocket_shm_client
    binance_websocket_client_main.cpp
    binance_websocket_shared_memory.cpp
)

# Link libraries
target_link_libraries(websocket_shm
    ${Boost_LIBRARIES}
    Threads::Threads
    rt  # For shared memory (shm_open)
)

# Link executable
target_link_libraries(websocket_shm_client
    ${Boost_LIBRARIES}
    Threads::Threads
    rt  # For shared memory (shm_open)
)

# Link Binance libraries
target_link_libraries(binance_websocket_shm
    ${Boost_LIBRARIES}
    Threads::Threads
    rt
)

target_link_libraries(binance_websocket_shm_client
    ${Boost_LIBRARIES}
    Threads::Threads
    rt
    ssl
    crypto
)

# Python binding (optional - using pybind11)
find_package(pybind11 QUIET)
if(pybind11_FOUND)
    pybind11_add_module(websocket_shm_py
        python_binding.cpp
    )
    target_link_libraries(websocket_shm_py PRIVATE websocket_shm)
endif()

# Installation
install(TARGETS websocket_shm
    LIBRARY DESTINATION lib
)

