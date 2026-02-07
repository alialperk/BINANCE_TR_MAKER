# Shared Memory Write/Read Verification

## Overview
This document verifies the shared memory communication between the C++ WebSocket client (`EXCHANGE_ws_client`/`binance_tr_ws_client`) and the Python arbitrage core (`arbit_core_maker_BTR_1.py`).

## Architecture

### C++ Client (Writer)
- **File**: `EXCHANGE_ws_client.cpp`
- **Shared Memory Manager**: `BinanceTROrderbookSharedMemoryManager` (in `EXCHANGE_orderbook_shared_memory.h`)
- **Write Location**: Line 621 in `EXCHANGE_ws_client.cpp`
  ```cpp
  g_shm_manager->update_orderbook(msg_symbol, top_ask_price, 0.0,
                                 top_bid_price, 0.0, timestamp);
  ```
- **Shared Memory Name**: `/binance_tr_orderbook_shm`
- **Structure**: `BinanceTROrderbookSharedMemory` with `OrderbookEntry` array (MAX_SYMBOLS=300)

### Python Reader
- **File**: `python_EXCHANGE_shared_memory_reader.py`
- **Class**: `EXCHANGESharedMemoryReader`
- **Read Method**: `read_updates()` (line 239)
- **Update Loop**: `run_bintr_shared_memory_reader()` (line 421)

## Data Flow

1. **C++ Client receives WebSocket message** → Parses orderbook data
2. **C++ Client calls `update_orderbook()`** → Writes to shared memory:
   - Looks up symbol in `symbol_to_index` map
   - Writes to `shm_ptr->entries[idx]`:
     - `ask_price`, `bid_price`
     - `timestamp`
     - `symbol` string
   - Updates `shm_ptr->num_symbols` if needed
3. **Python reader polls shared memory** (every 1ms by default)
4. **Python reader reads entries** → Updates `arbitrage_table_np`:
   - Maps global index to local index using `global_to_local_index`
   - Writes prices to arbitrage table columns

## Verification Points

### 1. Shared Memory Initialization
- **C++**: `g_shm_manager->initialize(symbol_to_index)` must succeed
- **Python**: `reader.connect()` must succeed
- **Check**: `shm_data.magic == 0x42494E41` and `shm_data.num_symbols > 0`

### 2. Symbol Format Matching
- **C++ writes**: `msg_symbol` from WebSocket (e.g., "ETHTRY", "XRPTRY")
- **Mapping file**: `binance_tr_symbol_mapping.json` has symbols like "ETH_TRY", "XRP_TRY"
- **Issue**: Symbol format mismatch can cause writes to fail silently
- **Solution**: C++ client normalizes symbols (removes underscores) before lookup

### 3. Index Mapping
- **Global Index**: Index in shared memory (0-289 for 290 symbols)
- **Local Index**: Index in arbitrage table (0-28 for 29 script symbols)
- **Mapping**: `EXCHANGE_global_to_local_index[global_idx] = local_idx`
- **Verification**: Python reader uses this mapping to write to correct table rows

### 4. Data Integrity
- **Write Verification**: C++ client writes prices to `entry.ask_price` and `entry.bid_price`
- **Read Verification**: Python reader compares shared memory values with table values
- **Logging**: Enhanced logging in `read_updates()` shows:
  - Sample prices from shared memory
  - Corresponding values in arbitrage table
  - Match status (✓ or ✗)

## Diagnostic Logging

### Python Reader Logs
- **First successful read**: Shows sample prices and verification
- **Every 100 reads**: Periodic verification of data integrity
- **Warning if `num_symbols=0`**: C++ client not writing data

### C++ Client Logs
- **Socket creation errors**: "ERROR: Failed to create socket"
- **Connection status**: "Connecting to Binance TR..." / "SSL connected"
- **Shared memory init**: "Shared memory initialized with N symbol(s)"

## Common Issues

### Issue 1: `num_symbols=0`
**Symptoms**: Python reader reports `num_symbols=0`
**Causes**:
- C++ client not running
- C++ client failed to connect to Binance TR
- C++ client not receiving WebSocket messages
- Symbol mapping mismatch

**Solution**: 
- Check if `binance_tr_ws_client` process is running
- Check C++ client logs for connection errors
- Verify `binance_tr_symbol_mapping.json` exists and is correct

### Issue 2: All Prices Zero
**Symptoms**: Prices in arbitrage table are all 0.0000
**Causes**:
- C++ client not writing data (see Issue 1)
- Symbol format mismatch (C++ can't find symbol in mapping)
- Index mapping incorrect

**Solution**:
- Verify symbol format in WebSocket messages matches mapping file
- Check C++ client logs for successful `update_orderbook()` calls
- Verify `global_to_local_index` mapping is correct

### Issue 3: Symbol Format Mismatch
**Symptoms**: Some symbols update, others don't
**Causes**:
- WebSocket messages use "ETHTRY" but mapping has "ETH_TRY"
- C++ client normalization not working correctly

**Solution**:
- C++ client normalizes symbols (removes underscores) before lookup
- Verify normalization logic in `EXCHANGE_ws_client.cpp` (lines 470-502)

## Testing

### Manual Verification
1. Start C++ client: `./start_binance_tr_websocket.sh`
2. Start Python arbitrage core: `python3 arbit_core_maker_BTR_1.py`
3. Check logs for:
   - "✓ Binance TR shared memory initialized: num_symbols=N"
   - "✓ Binance TR shared memory read: num_symbols=N, updates_processed=M"
   - Sample prices showing correct values

### Automated Verification
Run: `python3 verify_shared_memory.py`
- Checks shared memory exists
- Verifies `num_symbols > 0`
- Shows sample entries
- Tests continuous updates

## Code Locations

### C++ Write Path
- `EXCHANGE_ws_client.cpp:621` - `update_orderbook()` call
- `EXCHANGE_orderbook_shared_memory.h:100` - `update_orderbook()` implementation
- `EXCHANGE_orderbook_shared_memory.h:116` - Direct write to `entry.ask_price`, `entry.bid_price`

### Python Read Path
- `python_EXCHANGE_shared_memory_reader.py:239` - `read_updates()` method
- `python_EXCHANGE_shared_memory_reader.py:326` - Loop through entries
- `python_EXCHANGE_shared_memory_reader.py:356` - Write to arbitrage table
- `arbit_core_maker_BTR_1.py:2749` - Task creation for reader

## Status
✅ **Code Structure**: Verified correct
✅ **Write Path**: C++ client writes to shared memory correctly
✅ **Read Path**: Python reader reads from shared memory correctly
✅ **Index Mapping**: `global_to_local_index` mapping implemented
✅ **Diagnostic Logging**: Enhanced logging added for verification

⚠️ **Runtime Status**: Requires C++ client to be running and connected to Binance TR
