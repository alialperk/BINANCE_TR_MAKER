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

# Check if instruments file exists
INSTRUMENTS_FILE="binance_websocket_instruments.json"
if [ ! -f "$INSTRUMENTS_FILE" ]; then
    echo "Warning: $INSTRUMENTS_FILE not found."
    echo "The client will start but may not subscribe to any instruments."
    echo ""
fi

# Wait for instruments file to appear
echo "Waiting for instruments file: $INSTRUMENTS_FILE"
MAX_WAIT=30
WAIT_COUNT=0

while [ ! -f "$INSTRUMENTS_FILE" ] && [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    echo "  Waiting... ($WAIT_COUNT/$MAX_WAIT seconds)"
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ ! -f "$INSTRUMENTS_FILE" ]; then
    echo "Warning: Instruments file not found after $MAX_WAIT seconds."
    echo "The C++ client will start but won't subscribe to any instruments."
    echo "Make sure Python has written the instruments file."
    echo ""
fi

# Generate instruments file from CS_common_instruments.json
if [ -f "create_binance_instruments_from_common.py" ]; then
    echo "Generating instruments file from CS_common_instruments.json..."
    python3 create_binance_instruments_from_common.py
    echo ""
elif [ -f "create_binance_instruments_file_all.py" ]; then
    echo "Creating/updating instruments file with ALL instruments..."
    python3 create_binance_instruments_file_all.py
    echo ""
fi

# Read hosts from instruments file (should contain healthy hosts selected by GUI)
HOSTS=()
if [ -f "$INSTRUMENTS_FILE" ]; then
    # Try to extract hosts from JSON
    if command -v python3 &> /dev/null; then
        HOSTS_JSON=$(python3 -c "import json; f=open('$INSTRUMENTS_FILE'); d=json.load(f); print(','.join(d.get('hosts', [])))" 2>/dev/null || echo "")
        if [ -n "$HOSTS_JSON" ]; then
            IFS=',' read -ra HOSTS <<< "$HOSTS_JSON"
        fi
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
    python3 create_symbol_mappings.py
    if [ $? -ne 0 ]; then
        echo "Warning: Failed to generate symbol mappings, continuing without shared memory"
    fi
else
    echo "Warning: python3 not found, skipping symbol mapping generation"
fi

# Build the executable
echo "Compiling binance_ws_client.cpp..."
g++ -std=c++17 -O3 -march=native -mtune=native \
    -o binance_ws_client \
    binance_ws_client.cpp \
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
if [ ! -f "./binance_ws_client" ]; then
    echo ""
    echo "=========================================="
    echo "Binance CS C++ client not available"
    echo "=========================================="
    echo ""
    echo "The binance_ws_client executable was not built."
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
echo "Starting Binance CS C++ WebSocket clients for ${#HOSTS_TO_USE[@]} host(s)..."
echo "Hosts: ${HOSTS_TO_USE[@]}"
echo "Instruments file: $INSTRUMENTS_FILE"
echo ""
echo "Press Ctrl+C to stop all clients"
echo ""

# Launch a C++ client for each host
# Each client connects to a different host to ensure availability
# Each client runs on a different CPU core for better performance
PIDS=()
HOST_INDEX=0
CPU_CORES=(1 2)  # CPU cores for Binance clients

for HOST_STR in "${HOSTS_TO_USE[@]}"; do
    if [[ "$HOST_STR" == *":"* ]]; then
        HOST="${HOST_STR%%:*}"
        PORT="${HOST_STR##*:}"
    else
        HOST="$HOST_STR"
        PORT="10000"
    fi
    
    # Get CPU core for this client (Core 1 for first, Core 2 for second)
    CPU_CORE=${CPU_CORES[$HOST_INDEX]}
    
    echo "Starting Binance CS C++ client $HOST_INDEX for host: $HOST:$PORT"
    echo "  CPU core: $CPU_CORE"
    echo "  All symbols will be subscribed"
    
    # Launch client in background
    # Arguments: host port instruments_file cpu_core
    ./binance_ws_client "$HOST" "$PORT" "$INSTRUMENTS_FILE" "$CPU_CORE" &
    CLIENT_PID=$!
    PIDS+=($CLIENT_PID)
    echo "  Client PID: $CLIENT_PID"
    
    HOST_INDEX=$((HOST_INDEX + 1))
    sleep 1  # Small delay between client starts to avoid initialization race
done

echo ""
echo "All ${#PIDS[@]} C++ clients started. PIDs: ${PIDS[@]}"
echo "Waiting for clients to finish (or Ctrl+C to stop)..."
echo ""

# Trap SIGINT (Ctrl+C) to gracefully shutdown all clients
cleanup() {
    echo ""
    echo "Received Ctrl+C. Shutting down all clients gracefully..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "  Sending SIGTERM to client PID: $pid"
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    
    # Wait a bit for graceful shutdown
    sleep 2
    
    # Force kill if still running
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "  Force killing client PID: $pid"
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done
    
    echo "All clients stopped."
    exit 0
}

trap cleanup SIGINT SIGTERM

# Wait for all clients
wait ${PIDS[@]} 2>/dev/null || true

echo ""
echo "All clients have exited."

