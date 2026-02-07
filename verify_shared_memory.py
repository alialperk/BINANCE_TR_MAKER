#!/usr/bin/env python3
"""
Verification script to test shared memory writing and reading between C++ client and Python.
This script checks:
1. If shared memory exists and is initialized
2. If C++ client is writing data (num_symbols > 0)
3. If Python can read the data correctly
4. Sample prices to verify data integrity
"""

import sys
import time
import os
import struct
from ctypes import Structure, c_uint32, c_double, c_int64, c_char, sizeof
from mmap import mmap, ACCESS_READ

# Shared memory structure matching C++ header
MAX_SYMBOLS = 300
SHM_MAGIC = 0x42494E41  # "BINA"

class OrderbookEntry(Structure):
    _fields_ = [
        ("ask_price", c_double),
        ("ask_qty", c_double),
        ("bid_price", c_double),
        ("bid_qty", c_double),
        ("timestamp", c_int64),
        ("time_diff", c_int64),
        ("symbol", c_char * 16),
        ("padding", c_char * 12),
    ]

class BinanceTROrderbookSharedMemory(Structure):
    _fields_ = [
        ("magic", c_uint32),
        ("version", c_uint32),
        ("num_symbols", c_uint32),
        ("reserved", c_uint32),
        ("entries", OrderbookEntry * MAX_SYMBOLS),
    ]

def verify_shared_memory():
    """Verify shared memory is working correctly."""
    shm_name = "/binance_tr_orderbook_shm"
    
    print("=" * 60)
    print("Shared Memory Verification Script")
    print("=" * 60)
    print()
    
    # Try to open shared memory using shm_open (same method as Python reader)
    try:
        import fcntl
        # Open shared memory using shm_open
        shm_fd = os.open(shm_name, os.O_RDONLY)
        if shm_fd < 0:
            raise FileNotFoundError(f"Failed to open shared memory: {shm_name}")
        
        # Map shared memory
        mm = mmap(shm_fd, sizeof(BinanceTROrderbookSharedMemory), access=ACCESS_READ)
        shm_data = BinanceTROrderbookSharedMemory.from_buffer(mm)
        
        # Keep fd open for later cleanup
        _shm_fd = shm_fd
    except FileNotFoundError as e:
        print(f"ERROR: Shared memory '{shm_name}' not found.")
        print("       Make sure the C++ client (binance_tr_ws_client) is running.")
        print(f"       Error: {e}")
        return False
    except Exception as e:
        print(f"ERROR: Failed to open shared memory: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Check magic number
    print(f"✓ Shared memory opened successfully")
    print(f"  Magic: 0x{shm_data.magic:08X} (expected: 0x{SHM_MAGIC:08X})")
    
    if shm_data.magic != SHM_MAGIC:
        print(f"  ✗ WARNING: Magic number mismatch! Shared memory may be corrupted.")
        return False
    else:
        print(f"  ✓ Magic number is correct")
    
    print(f"  Version: {shm_data.version}")
    print(f"  num_symbols: {shm_data.num_symbols}")
    print()
    
    # Check if C++ client is writing data
    if shm_data.num_symbols == 0:
        print("✗ PROBLEM: num_symbols is 0")
        print("  The C++ client has initialized shared memory but is not writing data.")
        print("  Possible causes:")
        print("    1. C++ client is not receiving WebSocket messages")
        print("    2. C++ client failed to connect to Binance TR")
        print("    3. Symbol mapping mismatch")
        print()
        return False
    else:
        print(f"✓ C++ client is writing data: {shm_data.num_symbols} symbols")
    
    # Read sample entries
    print()
    print("Sample entries from shared memory:")
    print("-" * 60)
    
    valid_entries = 0
    sample_count = 0
    max_samples = 10
    
    for i in range(min(shm_data.num_symbols, MAX_SYMBOLS)):
        entry = shm_data.entries[i]
        
        # Skip if timestamp is 0 (not initialized)
        if entry.timestamp == 0:
            continue
        
        valid_entries += 1
        
        # Show first few samples
        if sample_count < max_samples:
            symbol_str = entry.symbol.decode('utf-8', errors='ignore').rstrip('\x00')
            print(f"  [{i:3d}] {symbol_str:16s} | "
                  f"Ask: {entry.ask_price:12.8f} | "
                  f"Bid: {entry.bid_price:12.8f} | "
                  f"Time: {entry.timestamp}")
            sample_count += 1
    
    print("-" * 60)
    print(f"Total valid entries: {valid_entries} / {shm_data.num_symbols}")
    print()
    
    if valid_entries == 0:
        print("✗ PROBLEM: No valid entries found (all timestamps are 0)")
        print("  The C++ client initialized shared memory but hasn't written any data yet.")
        print("  This could mean:")
        print("    1. C++ client just started and hasn't received data yet")
        print("    2. C++ client is not receiving WebSocket messages")
        return False
    else:
        print(f"✓ Found {valid_entries} valid entries with non-zero timestamps")
    
    # Test reading multiple times to verify updates
    print()
    print("Testing continuous reading (5 seconds)...")
    print("-" * 60)
    
    last_timestamp = {}
    update_count = 0
    
    for iteration in range(5):
        time.sleep(1)
        
        # Re-read the memory
        mm.seek(0)
        shm_data = BinanceTROrderbookSharedMemory.from_buffer(mm)
        
        for i in range(min(shm_data.num_symbols, MAX_SYMBOLS)):
            entry = shm_data.entries[i]
            if entry.timestamp == 0:
                continue
            
            symbol_str = entry.symbol.decode('utf-8', errors='ignore').rstrip('\x00')
            
            if symbol_str in last_timestamp:
                if entry.timestamp != last_timestamp[symbol_str]:
                    update_count += 1
                    if update_count <= 3:  # Show first 3 updates
                        print(f"  Update detected: {symbol_str} timestamp changed "
                              f"({last_timestamp[symbol_str]} -> {entry.timestamp})")
            
            last_timestamp[symbol_str] = entry.timestamp
        
        if iteration == 0:
            print(f"  Iteration {iteration + 1}: {len(last_timestamp)} symbols with data")
        else:
            print(f"  Iteration {iteration + 1}: {update_count} updates detected so far")
    
    print("-" * 60)
    
    if update_count > 0:
        print(f"✓ Shared memory is being updated: {update_count} updates detected")
    else:
        print("⚠ WARNING: No updates detected in 5 seconds")
        print("  This could mean the C++ client is not receiving new market data")
    
    print()
    print("=" * 60)
    print("Verification Summary:")
    print("=" * 60)
    print(f"  Shared memory exists: ✓")
    print(f"  Magic number correct: {'✓' if shm_data.magic == SHM_MAGIC else '✗'}")
    print(f"  C++ client writing data: {'✓' if shm_data.num_symbols > 0 else '✗'}")
    print(f"  Valid entries found: {'✓' if valid_entries > 0 else '✗'} ({valid_entries})")
    print(f"  Data is updating: {'✓' if update_count > 0 else '⚠'}")
    print()
    
    # Cleanup
    mm.close()
    os.close(_shm_fd)
    
    return shm_data.num_symbols > 0 and valid_entries > 0

if __name__ == "__main__":
    success = verify_shared_memory()
    sys.exit(0 if success else 1)
