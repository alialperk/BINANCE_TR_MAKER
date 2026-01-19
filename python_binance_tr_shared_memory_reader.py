"""
Python module to read Binance TR orderbook data from shared memory (written by C++ WebSocket client).
This is optimized for ultra-low latency by using direct memory access.
Reads from /binance_tr_orderbook_shm shared memory.
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
try:
    from binance_tr_shared_memory_reader_optimized import read_updates_optimized  # type: ignore
    USE_CYTHON_OPTIMIZATION = True
except ImportError:
    USE_CYTHON_OPTIMIZATION = False
    read_updates_optimized = None

# C structure definitions (must match binance_tr_orderbook_shared_memory.h exactly)
class OrderbookEntry(ctypes.Structure):
    _fields_ = [
        ("ask_price", ctypes.c_double),
        ("ask_qty", ctypes.c_double),
        ("bid_price", ctypes.c_double),
        ("bid_qty", ctypes.c_double),
        ("timestamp", ctypes.c_int64),
        ("time_diff", ctypes.c_int64),
        ("symbol", ctypes.c_char * 16),
        ("padding", ctypes.c_uint8 * 12),
    ]

class BinanceTROrderbookSharedMemory(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_uint32),
        ("version", ctypes.c_uint32),
        ("num_symbols", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("entries", OrderbookEntry * 300),  # MAX_SYMBOLS = 300
    ]

class BinanceTRSharedMemoryReader:
    """Reads Binance TR orderbook data from shared memory."""
    
    def __init__(self, shm_name: str = "/binance_tr_orderbook_shm", shm_size: int = 1024 * 1024):
        self.shm_name = shm_name
        self.shm_size = shm_size
        self.shm_fd = None
        self.shm_mmap = None
        self.shm_data = None
        self._use_direct_mmap = True
        self._buffer_copy = None
    
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
                self.shm_data = BinanceTROrderbookSharedMemory.from_buffer(self.shm_mmap)
                self._use_direct_mmap = True
            except (TypeError, BufferError) as e:
                # Fallback: create a writable copy for the structure
                import ctypes
                buffer_copy = ctypes.create_string_buffer(self.shm_mmap[:])
                self.shm_data = BinanceTROrderbookSharedMemory.from_buffer(buffer_copy)
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
                logging.info(f"✓ Binance TR shared memory initialized: magic={hex(self.shm_data.magic)}, version={self.shm_data.version}, num_symbols={self.shm_data.num_symbols}")
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
            logging.error(f"Failed to connect to Binance TR shared memory: {e}")
            logging.error("Make sure the C++ WebSocket client is running and has initialized shared memory.")
            return False
        except Exception as e:
            logging.error(f"Failed to connect to Binance TR shared memory: {e}")
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
                    col_time_diff: Optional[int] = None) -> int:
        """
        Read updates from shared memory and apply to arbitrage table.
        
        Args:
            arbitrage_table_np: NumPy array for arbitrage table
            symbol_index_map: Local symbol to index mapping
            col_time: Column index for Binance TR timestamp
            col_ask_price: Column index for Binance TR ask price
            col_bid_price: Column index for Binance TR bid price
            global_to_local_index: Mapping from global index to local index
            col_time_diff: Optional column index for Binance TR time diff
        
        Returns:
            Number of updates processed
        """
        if not self.shm_data:
            return 0
        
        # Refresh buffer copy if needed
        if hasattr(self, '_buffer_copy') and not getattr(self, '_use_direct_mmap', False):
            mmap_view = memoryview(self.shm_mmap)
            self._buffer_copy[:len(mmap_view)] = mmap_view
            self.shm_data = BinanceTROrderbookSharedMemory.from_buffer(self._buffer_copy)
        
        # Try to use Cython-optimized version if available (ultra-low latency)
        if USE_CYTHON_OPTIMIZATION and read_updates_optimized:
            try:
                # Ensure array is a NumPy array
                if not isinstance(arbitrage_table_np, np.ndarray):
                    raise TypeError(f"Expected numpy.ndarray, got {type(arbitrage_table_np)}")
                
                # Check if array dtype is compatible with Cython (must be float64)
                if arbitrage_table_np.dtype != np.float64:
                    raise ValueError(f"Array dtype is {arbitrage_table_np.dtype}, Cython requires float64. Falling back to Python implementation.")
                
                # Ensure array is contiguous
                if not arbitrage_table_np.flags['C_CONTIGUOUS']:
                    raise ValueError("Array must be C-contiguous for Cython optimization")
                
                col_time_diff_val = col_time_diff if col_time_diff is not None else -1
                updates_processed = read_updates_optimized(
                    self.shm_data,
                    arbitrage_table_np,
                    global_to_local_index,
                    col_time,
                    col_ask_price,
                    col_bid_price,
                    col_time_diff_val
                )
                
                # Minimal debug logging (only once)
                if not hasattr(self, '_cython_optimized_logged'):
                    logging.info("✓ Using Cython-optimized Binance TR shared memory reader (ultra-low latency)")
                    self._cython_optimized_logged = True
                
                return updates_processed
            except (ValueError, TypeError) as e:
                # Fall back to Python implementation if array is incompatible with Cython
                if not hasattr(self, '_cython_fallback_logged'):
                    logging.debug(f"Cython optimization not available (array dtype incompatible), using Python: {e}")
                    self._cython_fallback_logged = True
            except Exception as e:
                # Fall back to Python implementation if Cython fails for other reasons
                logging.warning(f"Cython-optimized read failed, falling back to Python: {e}")
        
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
            
            # Get symbol string
            symbol_str = entry.symbol.decode('utf-8', errors='ignore').rstrip('\x00')
            if not symbol_str:
                continue
            
            # Convert global index to local index
            local_symbol_idx = None
            if global_to_local_index is not None:
                local_symbol_idx = global_to_local_index.get(i)
                if local_symbol_idx is None:
                    skipped_indices.append(i)
                    continue
            else:
                # Try to find symbol in symbol_index_map
                if symbol_str in symbol_index_map:
                    local_symbol_idx = symbol_index_map[symbol_str]
                else:
                    skipped_indices.append(i)
                    continue
            
            # Verify bounds
            if local_symbol_idx is not None and local_symbol_idx < arbitrage_table_np.shape[0]:
                # Read prices (only prices, quantities are 0.0 as per C++ client)
                arbitrage_table_np[local_symbol_idx, col_time] = entry.timestamp
                arbitrage_table_np[local_symbol_idx, col_ask_price] = entry.ask_price
                arbitrage_table_np[local_symbol_idx, col_bid_price] = entry.bid_price
                
                # Read time diff if column is specified
                if col_time_diff is not None:
                    arbitrage_table_np[local_symbol_idx, col_time_diff] = entry.time_diff
                
                updates_processed += 1
                
                if len(sample_updates) < 3:
                    sample_updates.append((local_symbol_idx, entry.ask_price, entry.bid_price))
        
        # Debug logging (only once, on first successful read)
        if not hasattr(self, '_debug_logged_once'):
            self._debug_logged_once = False
        
        if not self._debug_logged_once and updates_processed > 0:
            # Build detailed debug message with prices (only once)
            debug_lines = [f"Binance TR shared memory read: num_symbols={num_symbols}, updates_processed={updates_processed}"]
            if skipped_indices:
                debug_lines.append(f"  Skipped indices (not in this script's mapping): {skipped_indices[:10]}...")
            
            if sample_updates:
                debug_lines.append("  Sample prices from shared memory:")
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
                        debug_lines.append(f"    {symbol_name} (idx={local_symbol_idx}): shm_ask={ask_price:.8f}, shm_bid={bid_price:.8f}, table_ask={table_ask:.8f}, table_bid={table_bid:.8f}")
                    except:
                        debug_lines.append(f"    {symbol_name} (idx={local_symbol_idx}): ask={ask_price:.8f}, bid={bid_price:.8f}")
            
            logging.info("\n".join(debug_lines))
            self._debug_logged_once = True
        
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

# Backward compatibility alias
BinTROrderbookReader = BinanceTRSharedMemoryReader

# Global variable to store the active reader
_global_binance_tr_reader = None

async def run_bintr_shared_memory_reader(arbitrage_table_np: np.ndarray,
                                         symbol_index_map: Dict[str, int],
                                         col_time: int,
                                         col_ask_price: int,
                                         col_bid_price: int,
                                         update_interval: float = 0.001,
                                         set_connected_flag: Optional[callable] = None,
                                         global_to_local_index: Optional[Dict[int, int]] = None,
                                         col_time_diff: Optional[int] = None):
    """
    Async function to continuously read from Binance TR shared memory and update arbitrage table.
    
    Args:
        arbitrage_table_np: NumPy array for arbitrage table
        symbol_index_map: Local symbol to index mapping
        col_time: Column index for Binance TR timestamp
        col_ask_price: Column index for Binance TR ask price
        col_bid_price: Column index for Binance TR bid price
        update_interval: Polling interval in seconds (default: 1ms)
        set_connected_flag: Callback to set connection flag
        global_to_local_index: Mapping from global index to local index
        col_time_diff: Optional column index for Binance TR time diff
    """
    # Create reader
    reader = BinanceTRSharedMemoryReader(shm_name="/binance_tr_orderbook_shm")
    logging.info("Created Binance TR shared memory reader")
    
    # Connect
    if not reader.connect():
        logging.error("Failed to connect to Binance TR shared memory. Make sure C++ WebSocket client is running.")
        if set_connected_flag:
            set_connected_flag(False)
        return
    
    if set_connected_flag:
        set_connected_flag(True)
    
    logging.info("Binance TR shared memory reader connected and running")
    
    # Store the reader globally
    global _global_binance_tr_reader
    _global_binance_tr_reader = reader
    
    stats_count = 0
    last_log_time = time.time()
    
    try:
        while True:
            total_updates = reader.read_updates(
                arbitrage_table_np,
                symbol_index_map,
                col_time,
                col_ask_price,
                col_bid_price,
                global_to_local_index,
                col_time_diff=col_time_diff
            )
            
            await asyncio.sleep(update_interval)
            
            # Log statistics every 30 seconds
            stats_count += 1
            current_time = time.time()
            if current_time - last_log_time >= 30.0:
                stats = reader.get_stats()
                if stats:
                    logging.info(f"Binance TR shared memory reader stats: num_symbols={stats.get('num_symbols', 0)}, "
                                f"updates_last_cycle={total_updates}")
                last_log_time = current_time
    except asyncio.CancelledError:
        logging.info("Binance TR shared memory reader cancelled")
    except Exception as e:
        logging.error(f"Error in Binance TR shared memory reader: {e}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        reader.disconnect()
        if set_connected_flag:
            set_connected_flag(False)
