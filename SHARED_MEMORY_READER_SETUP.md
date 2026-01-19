# Shared Memory Reader Setup

## Overview
This system implements a distributed orderbook reader architecture for 290 common symbols:
- **2 C++ WebSocket clients** write orderbook data to shared memory
- **10 Python reader scripts** read assigned symbols (29 per script) and calculate margins
- **GUI** displays margins from Redis

## Architecture

### 1. C++ WebSocket Clients (Writers)
- `start_binance_tr_websocket.sh` → Writes to `/binance_tr_orderbook_shm`
- `start_binance_websocket.sh` → Writes to `/binance_cs_orderbook_shm`

### 2. Python Reader Scripts (Readers)
- `orderbook_reader_1.py` through `orderbook_reader_10.py`
- Each script handles 29 symbols (290 / 10 = 29)
- Reads best bid/ask prices from shared memory
- Calculates margins and writes to Redis

### 3. Symbol Distribution
- Script 1: Symbols 0-28 (29 symbols)
- Script 2: Symbols 29-57 (29 symbols)
- Script 3: Symbols 58-86 (29 symbols)
- Script 4: Symbols 87-115 (29 symbols)
- Script 5: Symbols 116-144 (29 symbols)
- Script 6: Symbols 145-173 (29 symbols)
- Script 7: Symbols 174-202 (29 symbols)
- Script 8: Symbols 203-231 (29 symbols)
- Script 9: Symbols 232-260 (29 symbols)
- Script 10: Symbols 261-289 (29 symbols)

## Files Created

1. **binance_orderbook_shared_memory.h** - Updated MAX_SYMBOLS to 300
2. **python_binance_orderbook_reader.py** - Core reader module
3. **orderbook_reader_template.py** - Template for reader scripts
4. **orderbook_reader_1.py** through **orderbook_reader_10.py** - 10 reader scripts
5. **generate_readers.py** - Script generator utility

## Usage

### Starting the System
1. GUI: Press "START" button
   - Starts 2 C++ WebSocket clients
   - Starts 10 Python reader scripts
   
### Reading Margins
- Margins are written to Redis keys:
  - `margins_script_X` - All margins for script X
  - `margin_{base_symbol}` - Individual symbol margin
- GUI reads from Redis and displays margins

### Stopping the System
- GUI: Press "STOP" button
   - Stops C++ clients
   - Stops Python readers

## Data Flow

```
C++ Clients → Shared Memory → Python Readers → Redis → GUI
```

1. C++ clients fetch orderbook data from WebSocket
2. C++ clients write best prices to shared memory
3. Python readers read from shared memory (100ms interval)
4. Python readers calculate margins and write to Redis
5. GUI reads from Redis and displays margins

## Margin Calculation

For each symbol:
- **Buy TR, Sell CS**: `(CS_bid_try - TR_ask) / TR_ask * 100`
- **Buy CS, Sell TR**: `(TR_bid - CS_ask_try) / CS_ask_try * 100`
- **Best Margin**: `max(buy_tr_sell_cs_margin, buy_cs_sell_tr_margin)`

Where:
- `CS_bid_try = CS_bid * USDTTRY_rate`
- `CS_ask_try = CS_ask * USDTTRY_rate`

## Next Steps

1. ✅ Update shared memory header (MAX_SYMBOLS = 300)
2. ✅ Create Python reader module
3. ✅ Create 10 reader scripts
4. ⏳ Update GUI to start/stop readers
5. ⏳ Add margin display to GUI
