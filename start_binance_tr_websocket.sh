#!/bin/bash

# Script to build and run the Binance TR C++ WebSocket client
# This client connects to Binance TR WebSocket and logs top of book data

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Binance TR C++ WebSocket Client Builder"
echo "=========================================="

# Check for required tools
if ! command -v g++ &> /dev/null; then
    echo "Error: g++ is not installed. Please install it with: sudo apt-get install g++"
    exit 1
fi

# Check for OpenSSL development libraries
if ! command -v pkg-config &> /dev/null; then
    echo "Warning: pkg-config not found. Installing pkg-config and libssl-dev..."
    sudo apt-get update
    sudo apt-get install -y pkg-config libssl-dev
elif ! pkg-config --exists openssl 2>/dev/null; then
    echo "Warning: OpenSSL development libraries may not be installed."
    echo "Installing libssl-dev..."
    sudo apt-get update
    sudo apt-get install -y libssl-dev
fi

# Check if instruments file exists
INSTRUMENTS_FILE="common_symbol_info.json"
if [ ! -f "$INSTRUMENTS_FILE" ]; then
    echo "Warning: $INSTRUMENTS_FILE not found."
    echo "The client will use default stream: btctry@depth5@100ms"
    echo ""
fi

# Generate symbol-to-index mapping for shared memory
echo "Generating symbol-to-index mapping..."
if command -v python3 &> /dev/null; then
    if [ -f "create_symbol_mappings.py" ]; then
        python3 create_symbol_mappings.py
        if [ $? -ne 0 ]; then
            echo "Warning: Failed to generate symbol mappings, continuing without shared memory"
        fi
    else
        echo "Warning: create_symbol_mappings.py not found, skipping symbol mapping generation"
    fi
else
    echo "Warning: python3 not found, skipping symbol mapping generation"
fi

# Build the executable
echo "Compiling EXCHANGE_ws_client.cpp..."
g++ -std=c++17 -O3 -march=native -mtune=native \
    -o binance_tr_ws_client \
    EXCHANGE_ws_client.cpp \
    -lssl -lcrypto -lpthread -lrt \
    -Wall -Wextra

if [ $? -ne 0 ]; then
    echo "Error: Compilation failed!"
    exit 1
fi

echo ""
echo "=========================================="
echo "Build complete!"
echo "=========================================="
echo ""
echo "Starting Binance TR WebSocket client..."
echo "Instruments file: $INSTRUMENTS_FILE"
echo ""
echo "Press Ctrl+C to stop gracefully"
echo ""

# Trap SIGINT to ensure clean exit (though the C++ client handles it)
cleanup() {
    echo ""
    echo "Shutdown signal received. Waiting for client to exit gracefully..."
    exit 0
}

trap cleanup SIGINT SIGTERM

# Run the client (it will handle Ctrl+C internally)
./binance_tr_ws_client

echo ""
echo "Client has exited."
