#!/bin/bash

# Script to build and run the Binance/CS C++ WebSocket client
# This client connects to CS WebSocket and logs orderbook data

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Binance CS C++ WebSocket Client Builder"
echo "=========================================="

# Check for required tools
if ! command -v g++ &> /dev/null; then
    echo "Error: g++ is not installed. Please install it with: sudo apt-get install g++"
    exit 1
fi

# Check for Boost libraries (needed for JSON parsing)
if ! dpkg -s libboost-all-dev &> /dev/null; then
    echo "Warning: libboost-all-dev may not be installed. Installing..."
    sudo apt-get update
    sudo apt-get install -y libboost-all-dev
fi

# Check if common_symbol_info.json exists (required)
INSTRUMENTS_FILE="common_symbol_info.json"
if [ ! -f "$INSTRUMENTS_FILE" ]; then
    echo "Error: $INSTRUMENTS_FILE not found."
    echo "This file is required for the Binance CS client to work."
    echo "Please make sure common_symbol_info.json exists in the current directory."
    exit 1
fi

echo "Using instruments file: $INSTRUMENTS_FILE"

# Read hosts from common_symbol_info.json (if available)
HOSTS=()
if command -v python3 &> /dev/null; then
    HOSTS_JSON=$(python3 -c "
import json
try:
    with open('$INSTRUMENTS_FILE') as f:
        d = json.load(f)
        hosts = d.get('hosts', [])
        if hosts:
            print(','.join([f\"{h['host']}:{h.get('port', 10000)}\" if isinstance(h, dict) else str(h) for h in hosts]))
except:
    pass
" 2>/dev/null || echo "")
    if [ -n "$HOSTS_JSON" ]; then
        IFS=',' read -ra HOSTS <<< "$HOSTS_JSON"
    fi
fi

# If no hosts found, use defaults
if [ ${#HOSTS[@]} -eq 0 ]; then
    HOSTS=("63.180.84.140:10000" "63.180.141.87:10000")
    echo "Warning: No hosts found in instruments file, using defaults: ${HOSTS[@]}"
fi

# Use first 2 hosts (or all if less than 2) for redundancy
HOSTS_TO_USE=("${HOSTS[@]:0:2}")

# Generate symbol-to-index mapping for shared memory (after creating instruments file)
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

# Build the executable (always rebuild to ensure latest code)
echo "Compiling binance_cs_ws_client.cpp..."
# Remove old binary first to force rebuild
rm -f binance_cs_ws_client
g++ -std=c++17 -O3 -march=native -mtune=native \
    -o binance_cs_ws_client \
    binance_cs_ws_client.cpp \
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
# Check if executable was built successfully
if [ ! -f "./binance_cs_ws_client" ]; then
    echo ""
    echo "=========================================="
    echo "Binance CS C++ client not available"
    echo "=========================================="
    echo ""
    echo "The binance_cs_ws_client executable was not built."
    echo "This is expected if the shared memory header is not available."
    echo ""
    echo "The system will continue without the Binance CS C++ client."
    echo "You can still use Python WebSocket clients for Binance CS."
    echo ""
    echo "Press any key to exit..."
    read -n 1
    exit 0
fi

echo ""
echo "Starting Binance CS C++ WebSocket client..."
echo "Instruments file: $INSTRUMENTS_FILE"
echo ""
echo "The client will automatically:"
echo "  - Read hosts from $INSTRUMENTS_FILE"
echo "  - Spawn 4 cores (2 hosts × 2 cores each) for redundancy"
echo "  - Subscribe to all symbols from $INSTRUMENTS_FILE"
echo ""
echo "Press Ctrl+C to stop all clients"
echo ""

# Launch the C++ client (it will spawn 4 cores internally)
# The client reads hosts and symbols from common_symbol_info.json
./binance_cs_ws_client &
CLIENT_PID=$!

echo "Binance CS C++ client started (PID: $CLIENT_PID)"
echo "  - Parent process will spawn 4 child cores"
echo "  - Each core handles a subset of symbols"
echo "  - Both hosts will be used for redundancy"
echo ""
echo "Waiting for client to finish (or Ctrl+C to stop)..."
echo ""

# Trap SIGINT (Ctrl+C) to gracefully shutdown all clients
cleanup() {
    echo ""
    echo "Received Ctrl+C. Shutting down all clients gracefully..."
    
    # Kill parent process and all children
    if kill -0 "$CLIENT_PID" 2>/dev/null; then
        echo "  Sending SIGTERM to client PID: $CLIENT_PID"
        kill -TERM "$CLIENT_PID" 2>/dev/null || true
    fi
    
    # Also kill any child processes
    pkill -P "$CLIENT_PID" binance_cs_ws_client 2>/dev/null || true
    
    # Wait a bit for graceful shutdown
    sleep 2
    
    # Force kill if still running
    if kill -0 "$CLIENT_PID" 2>/dev/null; then
        echo "  Force killing client PID: $CLIENT_PID"
        kill -KILL "$CLIENT_PID" 2>/dev/null || true
    fi
    
    # Force kill any remaining child processes
    pkill -9 -P "$CLIENT_PID" binance_cs_ws_client 2>/dev/null || true
    
    echo "All clients stopped."
    exit 0
}

trap cleanup SIGINT SIGTERM

# Wait for client
wait "$CLIENT_PID" 2>/dev/null || true

echo ""
echo "All clients have exited."

