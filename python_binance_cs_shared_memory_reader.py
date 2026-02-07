"""
Python module to read Binance CS (Futures) orderbook data from shared memory (written by C++ WebSocket client).
This is optimized for ultra-low latency by using direct memory access.
Reads from /binance_cs_orderbook_shm shared memory.
"""

import mmap
import ctypes
import struct
import numpy as np
import asyncio
from typing import Optional, Dict
import time
import logging

# Try to import Cython-optimized function (optional optimization)
# Note: CS-specific optimized reader not yet implemented, will use Python implementation
USE_CYTHON_OPTIMIZATION = False
read_updates_optimized = None

# C structure definitions (must match binance_cs_orderbook_shared_memory.h exactly)
class OrderbookEntry(ctypes.Structure):
    _fields_ = [
        ("ask_price", ctypes.c_double),
        ("ask_qty", ctypes.c_double),
        ("bid_price", ctypes.c_double),
        ("bid_qty", ctypes.c_double),
        ("timestamp", ctypes.c_int64),
        ("time_diff", ctypes.c_int64),
        ("instrument_id", ctypes.c_uint32),
        ("symbol", ctypes.c_char * 16),
        ("padding", ctypes.c_uint8 * 4),
    ]

class BinanceCSOrderbookSharedMemory(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_uint32),
        ("version", ctypes.c_uint32),
        ("num_symbols", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("entries", OrderbookEntry * 300),  # MAX_SYMBOLS = 300
    ]

class BinanceCSSharedMemoryReader:
    """Reads Binance CS (Futures) orderbook data from shared memory."""
    
    def __init__(self, shm_name: str = "/binance_cs_orderbook_shm", shm_size: int = 1024 * 1024):
        self.shm_name = shm_name
        self.shm_size = shm_size
        self.shm_fd = None
        self.shm_mmap = None
        self.shm_data = None
        self._use_direct_mmap = True
        self._buffer_copy = None
    
    @staticmethod
    def create_multi_host_readers(hosts: list) -> list:
        """
        Create BinanceCSSharedMemoryReader instance.
        Both hosts write to the same shared memory for redundancy.
        
        Args:
            hosts: List of host strings (e.g., ["63.180.84.140:10000", "63.180.141.87:10000"])
        
        Returns:
            List with single BinanceCSSharedMemoryReader instance
        """
        readers = []
        # Both hosts write to the same shared memory for redundancy
        # Use single shared memory name that matches C++ client
        reader = BinanceCSSharedMemoryReader(shm_name="/binance_cs_orderbook_shm")
        readers.append(reader)
        return readers
    
    def connect(self) -> bool:
        """Connect to shared memory."""
        try:
            # Try to use posix_ipc if available (cleaner API)
            # Note: posix_ipc is optional - if not installed, falls back to ctypes
            try:
                import posix_ipc  # type: ignore  # noqa: F401
                # Open existing shared memory
                self.shm = posix_ipc.SharedMemory(self.shm_name, 
                                                 flags=posix_ipc.O_CREAT,
                                                 size=self.shm_size)
                # Map shared memory (read-only)
                self.shm_mmap = mmap.mmap(self.shm.fd, self.shm_size, 
                                      access=mmap.ACCESS_READ)
            except ImportError:
                # Fallback to direct POSIX shared memory using ctypes
                import os
                import ctypes.util
                import stat
                
                # Use shm_open via ctypes
                librt = ctypes.CDLL(ctypes.util.find_library('rt'))
                librt.shm_open.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_int]
                librt.shm_open.restype = ctypes.c_int
                librt.ftruncate.argtypes = [ctypes.c_int, ctypes.c_ssize_t]
                librt.ftruncate.restype = ctypes.c_int
                librt.fstat.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.Structure)]
                librt.fstat.restype = ctypes.c_int
                
                # Open shared memory (O_RDONLY = 0) - do NOT create it, C++ client should create it
                shm_name_bytes = self.shm_name.encode('utf-8')
                O_RDONLY = 0
                fd = librt.shm_open(shm_name_bytes, O_RDONLY, 0o666)
                
                if fd < 0:
                    # Shared memory doesn't exist yet - this is expected if C++ client hasn't started
                    errno_val = ctypes.get_errno()
                    raise OSError(f"Shared memory '{self.shm_name}' does not exist yet. "
                                f"Make sure the C++ WebSocket client is running. Error: {os.strerror(errno_val)}")
                
                # Check the actual size of the shared memory file
                class Stat(ctypes.Structure):
                    _fields_ = [
                        ("st_dev", ctypes.c_ulonglong),
                        ("st_ino", ctypes.c_ulonglong),
                        ("st_mode", ctypes.c_uint),
                        ("st_nlink", ctypes.c_ulonglong),
                        ("st_uid", ctypes.c_uint),
                        ("st_gid", ctypes.c_uint),
                        ("st_rdev", ctypes.c_ulonglong),
                        ("st_size", ctypes.c_longlong),  # This is what we need
                        ("st_blksize", ctypes.c_longlong),
                        ("st_blocks", ctypes.c_longlong),
                        ("st_atime", ctypes.c_longlong),
                        ("st_atime_nsec", ctypes.c_longlong),
                        ("st_mtime", ctypes.c_longlong),
                        ("st_mtime_nsec", ctypes.c_longlong),
                        ("st_ctime", ctypes.c_longlong),
                        ("st_ctime_nsec", ctypes.c_longlong),
                    ]
                
                stat_buf = Stat()
                if librt.fstat(fd, ctypes.byref(stat_buf)) == 0:
                    actual_size = stat_buf.st_size
                    if actual_size == 0:
                        raise OSError(f"Shared memory '{self.shm_name}' exists but has size 0. "
                                    f"C++ client may not have initialized it yet. Please wait and retry.")
                    if actual_size < self.shm_size:
                        logging.warning(f"Shared memory size ({actual_size}) is smaller than expected ({self.shm_size}). "
                                      f"Using actual size: {actual_size}")
                        self.shm_size = actual_size
                
                # Map shared memory (read-only) - use actual size
                try:
                    self.shm_mmap = mmap.mmap(fd, self.shm_size, access=mmap.ACCESS_READ)
                except ValueError as e:
                    # If mmap fails due to size mismatch, try to get the actual size again
                    if "mmap length is greater than file size" in str(e):
                        if librt.fstat(fd, ctypes.byref(stat_buf)) == 0:
                            actual_size = stat_buf.st_size
                            if actual_size > 0:
                                logging.warning(f"Retrying mmap with actual size: {actual_size}")
                                self.shm_size = actual_size
                                self.shm_mmap = mmap.mmap(fd, self.shm_size, access=mmap.ACCESS_READ)
                            else:
                                raise OSError(f"Shared memory '{self.shm_name}' has size 0. "
                                            f"C++ client may not have initialized it yet.")
                        else:
                            raise
                    else:
                        raise
                
                self.shm_fd = fd
            
            # Create ctypes structure from memory (common for both paths)
            # Handle read-only memory buffer issue
            try:
                self.shm_data = BinanceCSOrderbookSharedMemory.from_buffer(self.shm_mmap)
                self._use_direct_mmap = True
            except (TypeError, BufferError) as e:
                # Fallback: create a writable copy for the structure
                import ctypes
                buffer_copy = ctypes.create_string_buffer(self.shm_mmap[:])
                self.shm_data = BinanceCSOrderbookSharedMemory.from_buffer(buffer_copy)
                self._buffer_copy = buffer_copy
                self._use_direct_mmap = False
            
            # Verify shm_data was created
            if self.shm_data is None:
                raise RuntimeError("Failed to create shared memory data structure")
            
            # Verify magic number
            SHM_MAGIC = 0x42494E41  # "BINA" in ASCII
            if self.shm_data.magic != SHM_MAGIC:
                raise RuntimeError(f"Invalid magic number: expected {hex(SHM_MAGIC)}, got {hex(self.shm_data.magic)}")
            
            if self.shm_data:
                logging.info(f"✓ Binance CS shared memory initialized: magic={hex(self.shm_data.magic)}, version={self.shm_data.version}, num_symbols={self.shm_data.num_symbols}")
            else:
                logging.error("❌ shm_data is None after connect(). This is a bug.")
                return False
            
            return True
            
        except ImportError as e:
            # This should not happen if posix_ipc is installed, but if it does, 
            # the fallback code should handle it. If we get here, the fallback also failed.
            logging.error(f"posix_ipc module not available and fallback also failed: {e}")
            logging.error("Please install posix_ipc: pip install posix_ipc")
            import traceback
            logging.error(traceback.format_exc())
            return False
        except OSError as e:
            # Shared memory doesn't exist or size mismatch
            logging.error(f"Failed to connect to Binance CS shared memory: {e}")
            logging.error("Make sure the C++ WebSocket client is running and has initialized shared memory.")
            return False
        except Exception as e:
            logging.error(f"Failed to connect to Binance shared memory: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return False
    
    def disconnect(self):
        """Disconnect from shared memory."""
        # Clear any references to the buffer copy before closing
        if hasattr(self, '_buffer_copy'):
            del self._buffer_copy
        if hasattr(self, 'shm_data'):
            self.shm_data = None
        
        # Close mmap (handle potential buffer errors)
        if self.shm_mmap:
            try:
                self.shm_mmap.close()
            except (BufferError, ValueError) as e:
                # Buffer might still be referenced, try to force close
                logging.debug(f"Warning during mmap close: {e}")
                try:
                    del self.shm_mmap
                except:
                    pass
            self.shm_mmap = None
        
        if hasattr(self, 'shm'):
            try:
                self.shm.close_fd()
            except:
                pass
        
        if hasattr(self, 'shm_fd') and self.shm_fd is not None:
            import os
            try:
                os.close(self.shm_fd)
            except:
                pass
            self.shm_fd = None
    
    def read_updates(self, arbitrage_table_np: np.ndarray,
                    symbol_index_map: Dict[str, int],
                    col_time: int,
                    col_ask_price: int,
                    col_bid_price: int,
                    global_to_local_index: Optional[Dict[int, int]] = None,
                    col_time_diff: Optional[int] = None,
                    update_map: Optional[Dict[str, tuple]] = None) -> int:
        """
        Read updates from shared memory and apply to arbitrage table.
        
        Args:
            arbitrage_table_np: NumPy array for arbitrage table
            symbol_index_map: Local symbol to index mapping
            col_time: Column index for Binance timestamp
            col_ask_price: Column index for Binance ask price
            col_bid_price: Column index for Binance bid price
            global_to_local_index: Mapping from global index to local index
            col_time_diff: Optional column index for Binance time diff (calculated by C++ client)
            update_map: Optional pre-computed mapping from raw_symbol -> (local_index, price_multiplier, amount_multiplier)
                       If provided, uses optimized path that eliminates string processing in hot loop
        
        Returns:
            Number of updates processed
        """
        if not self.shm_data:
            return 0
        
        # Refresh buffer copy if needed
        if hasattr(self, '_buffer_copy') and not getattr(self, '_use_direct_mmap', False):
            mmap_view = memoryview(self.shm_mmap)
            self._buffer_copy[:len(mmap_view)] = mmap_view
            self.shm_data = BinanceCSOrderbookSharedMemory.from_buffer(self._buffer_copy)
        
        # Python implementation: Read from OrderbookEntry array
        # The C++ client writes directly to entries array indexed by symbol index
        updates_processed = 0
        sample_updates = []
        skipped_indices = []
        
        # Read all entries (up to num_symbols)
        num_symbols = min(self.shm_data.num_symbols, 300)  # MAX_SYMBOLS
        
        for i in range(num_symbols):
            entry = self.shm_data.entries[i]
            
            # Skip if timestamp is 0 (not initialized)
            if entry.timestamp == 0:
                continue
            
            # OPTIMIZED PATH: Use pre-computed update_map if available (eliminates string processing in hot loop)
            if update_map is not None:
                # Get symbol string once
                symbol_str = entry.symbol.decode('utf-8', errors='ignore').rstrip('\x00')
                if not symbol_str:
                    continue
                
                # Fast lookup in pre-computed map
                map_entry = update_map.get(symbol_str)
                if map_entry is None:
                    skipped_indices.append(i)
                    continue
                
                local_symbol_idx, price_mult, amount_mult = map_entry
                
                # Apply price multiplier (0.001 for 1000-prefix symbols, 1.0 otherwise)
                ask_price = entry.ask_price * price_mult
                bid_price = entry.bid_price * price_mult
            else:
                # FALLBACK PATH: Original logic with string processing (slower)
                # Get symbol string
                symbol_str = entry.symbol.decode('utf-8', errors='ignore').rstrip('\x00')
                if not symbol_str:
                    continue
                
                # Convert global index to local index
                local_symbol_idx = None
                if global_to_local_index is not None:
                    # Use global_to_local_index mapping (preferred method)
                    local_symbol_idx = global_to_local_index.get(i)
                    if local_symbol_idx is None:
                        skipped_indices.append(i)
                        continue
                else:
                    # Fallback: Try to find symbol in symbol_index_map
                    # The shared memory has symbols like "0GUSDT", but symbol_index_map has base symbols like "0G"
                    # So we need to strip "USDT" suffix if present and apply exceptional symbol normalization
                    base_symbol = symbol_str
                    if symbol_str.endswith("USDT"):
                        binance_cs_base_symbol = symbol_str[:-4]  # Remove "USDT" suffix (e.g., "1000SHIBUSDT" -> "1000SHIB")
                        # Normalize to EXCHANGE format for consistent mapping (e.g., "1000SHIB" -> "SHIB")
                        try:
                            import arbit_config_maker_BTR as arbit_config
                            base_symbol = arbit_config.normalize_base_symbol_from_binance_cs(binance_cs_base_symbol)
                        except (ImportError, AttributeError):
                            # Fallback if arbit_config not available
                            base_symbol = binance_cs_base_symbol
                    
                    if base_symbol in symbol_index_map:
                        local_symbol_idx = symbol_index_map[base_symbol]
                    else:
                        skipped_indices.append(i)
                        continue
                
                # CRITICAL: For "1000" prefix symbols (1000SHIB, 1000BONK, 1000FLOKI, 1000PEPE, 1000LUNC, 1000XEC),
                # multiply prices by 0.001 because the price shown is for 1000 units, not 1 unit
                ask_price = entry.ask_price
                bid_price = entry.bid_price
                
                try:
                    import arbit_config_maker_BTR as arbit_config
                    if arbit_config.is_1000_prefix_symbol(symbol_str):
                        # Price is for 1000 units, convert to price per 1 unit
                        ask_price = entry.ask_price * 0.001
                        bid_price = entry.bid_price * 0.001
                except (ImportError, AttributeError):
                    # Fallback: check manually if symbol starts with "1000"
                    base_symbol_check = symbol_str
                    if symbol_str.endswith("USDT"):
                        base_symbol_check = symbol_str[:-4]
                    if base_symbol_check.startswith("1000"):
                        ask_price = entry.ask_price * 0.001
                        bid_price = entry.bid_price * 0.001
            
            # Verify bounds and write to arbitrage table
            if local_symbol_idx is not None and local_symbol_idx < arbitrage_table_np.shape[0]:
                arbitrage_table_np[local_symbol_idx, col_time] = entry.timestamp
                arbitrage_table_np[local_symbol_idx, col_ask_price] = ask_price
                arbitrage_table_np[local_symbol_idx, col_bid_price] = bid_price
                
                # Read time diff if column is specified
                if col_time_diff is not None:
                    arbitrage_table_np[local_symbol_idx, col_time_diff] = entry.time_diff
                
                updates_processed += 1
                
                if len(sample_updates) < 3:
                    # Store adjusted prices (already multiplied by 0.001 if needed)
                    sample_updates.append((local_symbol_idx, ask_price, bid_price))
        
        # Debug logging (only once, on first successful read)
        if not hasattr(self, '_debug_logged_once'):
            self._debug_logged_once = False
        
        if not self._debug_logged_once and updates_processed > 0:
            # Build detailed debug message with prices (only once)
            debug_lines = [f"✓ Binance CS shared memory read: num_symbols={num_symbols}, updates_processed={updates_processed}"]
            if skipped_indices:
                debug_lines.append(f"  Skipped indices (not in this script's mapping): {skipped_indices[:10]}...")
            
            if sample_updates:
                debug_lines.append("  ✓ Sample prices from shared memory (verifying write/read cycle):")
                for local_symbol_idx, ask_price, bid_price in sample_updates[:5]:
                    symbol_name = "?"
                    try:
                        for sym, idx in symbol_index_map.items():
                            if idx == local_symbol_idx:
                                symbol_name = sym
                                break
                    except:
                        pass
                    # Also show prices from arbitrage table to verify they're being written correctly
                    try:
                        table_ask = arbitrage_table_np[local_symbol_idx, col_ask_price]
                        table_bid = arbitrage_table_np[local_symbol_idx, col_bid_price]
                        # Verify the write/read cycle is working
                        ask_match = abs(table_ask - ask_price) < 0.0001
                        bid_match = abs(table_bid - bid_price) < 0.0001
                        match_status = "✓" if (ask_match and bid_match) else "✗"
                        debug_lines.append(f"    {match_status} {symbol_name} (idx={local_symbol_idx}): "
                                          f"shm_ask={ask_price:.8f}, shm_bid={bid_price:.8f}, "
                                          f"table_ask={table_ask:.8f}, table_bid={table_bid:.8f}")
                    except:
                        debug_lines.append(f"    ? {symbol_name} (idx={local_symbol_idx}): ask={ask_price:.8f}, bid={bid_price:.8f}")
            
            logging.info("\n".join(debug_lines))
            self._debug_logged_once = True
        
        # Periodic verification logging (every 100 reads)
        if not hasattr(self, '_read_count'):
            self._read_count = 0
        self._read_count += 1
        
        if self._read_count % 100 == 0 and updates_processed > 0:
            # Verify data integrity by checking a few random entries
            verification_samples = []
            for i in range(min(num_symbols, 5)):
                entry = self.shm_data.entries[i]
                if entry.timestamp > 0:
                    symbol_str = entry.symbol.decode('utf-8', errors='ignore').rstrip('\x00')
                    
                    # Calculate adjusted prices (multiply by 0.001 for 1000 prefix symbols)
                    shm_ask_raw = entry.ask_price
                    shm_bid_raw = entry.bid_price
                    try:
                        import arbit_config_maker_BTR as arbit_config
                        if arbit_config.is_1000_prefix_symbol(symbol_str):
                            shm_ask = shm_ask_raw * 0.001
                            shm_bid = shm_bid_raw * 0.001
                        else:
                            shm_ask = shm_ask_raw
                            shm_bid = shm_bid_raw
                    except (ImportError, AttributeError):
                        # Fallback: check manually
                        base_symbol_check = symbol_str
                        if symbol_str.endswith("USDT"):
                            base_symbol_check = symbol_str[:-4]
                        if base_symbol_check.startswith("1000"):
                            shm_ask = shm_ask_raw * 0.001
                            shm_bid = shm_bid_raw * 0.001
                        else:
                            shm_ask = shm_ask_raw
                            shm_bid = shm_bid_raw
                    
                    if symbol_str in symbol_index_map:
                        local_idx = symbol_index_map[symbol_str]
                        if local_idx < arbitrage_table_np.shape[0]:
                            table_ask = arbitrage_table_np[local_idx, col_ask_price]
                            table_bid = arbitrage_table_np[local_idx, col_bid_price]
                            ask_match = abs(table_ask - shm_ask) < 0.0001
                            bid_match = abs(table_bid - shm_bid) < 0.0001
                            verification_samples.append((symbol_str, ask_match and bid_match))
                    elif global_to_local_index is not None:
                        # Try using global_to_local_index mapping
                        local_idx = global_to_local_index.get(i)
                        if local_idx is not None and local_idx < arbitrage_table_np.shape[0]:
                            table_ask = arbitrage_table_np[local_idx, col_ask_price]
                            table_bid = arbitrage_table_np[local_idx, col_bid_price]
                            ask_match = abs(table_ask - shm_ask) < 0.0001
                            bid_match = abs(table_bid - shm_bid) < 0.0001
                            verification_samples.append((symbol_str, ask_match and bid_match))
            
            if verification_samples:
                all_match = all(match for _, match in verification_samples)
                status = "✓" if all_match else "⚠"
                logging.info(f"{status} Binance CS shared memory verification (read #{self._read_count}): "
                           f"{sum(1 for _, m in verification_samples if m)}/{len(verification_samples)} samples match "
                           f"(C++ write → Python read cycle working)")
        
        return updates_processed
    
    def get_stats(self) -> Optional[Dict]:
        """Get statistics about shared memory reads."""
        if not self.shm_data:
            return None
        
        return {
            'magic': hex(self.shm_data.magic),
            'version': self.shm_data.version,
            'num_symbols': self.shm_data.num_symbols,
        }
    
    def get_usdttry_rate(self) -> float:
        """Get current USDTTRY rate (not stored in CS shared memory, return default)."""
        # CS shared memory doesn't store USDTTRY rate
        return 1.0  # Default fallback

# Backward compatibility alias
BinanceSharedMemoryReader = BinanceCSSharedMemoryReader

# Global variable to store the active reader (for accessing USDTTRY rate)
_global_binance_reader = None

def get_binance_usdttry_rate() -> float:
    """Get current USDTTRY rate (not stored in CS shared memory, return default)."""
    # CS shared memory doesn't store USDTTRY rate
    return 1.0  # Default fallback

async def run_binance_shared_memory_reader(arbitrage_table_np: np.ndarray,
                                          symbol_index_map: Dict[str, int],
                                          col_time: int,
                                          col_ask_price: int,
                                          col_bid_price: int,
                                          update_interval: float = 0.001,
                                          set_connected_flag: Optional[callable] = None,
                                          global_to_local_index: Optional[Dict[int, int]] = None,
                                          hosts: Optional[list] = None,
                                          col_time_diff: Optional[int] = None,
                                          update_map: Optional[Dict[str, tuple]] = None):
    """
    Async function to continuously read from Binance shared memory and update arbitrage table.
    Supports reading from multiple hosts (one C++ client per host, each subscribing to all symbols).
    
    Args:
        arbitrage_table_np: NumPy array for arbitrage table
        symbol_index_map: Local symbol to index mapping
        col_time: Column index for Binance timestamp
        col_ask_price: Column index for Binance ask price
        col_bid_price: Column index for Binance bid price
        update_interval: Polling interval in seconds (default: 1ms)
        set_connected_flag: Callback to set connection flag
        global_to_local_index: Mapping from global index to local index
        hosts: List of host strings (e.g., ["63.180.84.140:10000", "63.180.141.87:10000"])
               If provided, reads from multiple shared memory sources (one per host)
        col_time_diff: Optional column index for Binance time diff (calculated by C++ client)
                       If provided, reads time_diff from shared memory and writes to arbitrage table
        update_map: Optional pre-computed mapping from raw_symbol -> (local_index, price_multiplier, amount_multiplier)
                    If provided, eliminates string processing in hot loop for better performance
    """
    # Create reader
    # IMPORTANT: Both C++ clients write to the SAME shared memory (/binance_cs_orderbook_shm)
    # for redundancy, so we use a single reader regardless of number of hosts
    if hosts and len(hosts) > 0:
        # Use single shared memory reader (both C++ clients write to same shm)
        readers = BinanceCSSharedMemoryReader.create_multi_host_readers(hosts)
        logging.info(f"Created single Binance CS shared memory reader for {len(hosts)} host(s) (C++ clients write to same shared memory)")
    else:
        # Single reader (backward compatibility)
        readers = [BinanceCSSharedMemoryReader(shm_name="/binance_cs_orderbook_shm")]
        logging.info("Created single Binance CS shared memory reader (default)")
    
    # Connect all readers
    connected_readers = []
    for i, reader in enumerate(readers):
        if reader.connect():
            connected_readers.append(reader)
            host_info = f"host {i}" if hosts and i < len(hosts) else "default"
            logging.info(f"✓ Binance CS shared memory reader {i} connected ({host_info})")
        else:
            host_info = f"host {i}" if hosts and i < len(hosts) else "default"
            logging.warning(f"✗ Failed to connect Binance CS shared memory reader {i} ({host_info})")
    
    if not connected_readers:
        logging.error("Failed to connect to any Binance CS shared memory. Make sure C++ WebSocket clients are running.")
        if set_connected_flag:
            set_connected_flag(False)
        return
    
    if set_connected_flag:
        set_connected_flag(True)
    
    logging.info(f"Binance CS shared memory reader(s) connected and running ({len(connected_readers)} reader(s))")
    
    # Store the first reader globally (for potential future use)
    global _global_binance_reader
    _global_binance_reader = connected_readers[0] if connected_readers else None
    
    stats_count = 0
    last_log_time = time.time()
    
    try:
        while True:
            total_updates = 0
            
            # Read from all connected readers and merge updates
            # Each reader may have different data, we take the latest for each symbol
            # Prices are already in TRY (converted by C++ client), Python just reads them
            for reader in connected_readers:
                updates = reader.read_updates(
                    arbitrage_table_np,
                    symbol_index_map,
                    col_time,
                    col_ask_price,
                    col_bid_price,
                    global_to_local_index,
                    col_time_diff=col_time_diff,  # Pass time diff column index
                    update_map=update_map  # Pass pre-computed update map for optimization
                )
                total_updates += updates
            
            await asyncio.sleep(update_interval)
            
            # Log statistics every 30 seconds
            stats_count += 1
            current_time = time.time()
            if current_time - last_log_time >= 30.0:
                # Get stats from first reader (for logging)
                if connected_readers:
                    stats = connected_readers[0].get_stats()
                    if stats:
                        logging.info(f"Binance CS shared memory reader stats: num_symbols={stats.get('num_symbols', 0)}, "
                                    f"updates_last_cycle={total_updates}, "
                                    f"readers={len(connected_readers)}")
                last_log_time = current_time
    except asyncio.CancelledError:
        logging.info("Binance CS shared memory reader cancelled")
    except Exception as e:
        logging.error(f"Error in Binance CS shared memory reader: {e}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        for reader in connected_readers:
            reader.disconnect()
        if set_connected_flag:
            set_connected_flag(False)

