# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False

"""
Cython-optimized shared memory reader for Binance orderbook updates (ultra-low latency HFT).

This module optimizes the critical hot path of reading updates from shared memory
and applying them to the NumPy arbitrage table.
"""

import cython
import numpy as np
cimport numpy as np
# Required for NumPy 2.x compatibility
np.import_array()
from libc.stdint cimport uint32_t, uint64_t, int64_t, intptr_t
from libc.stdlib cimport malloc, free
import ctypes

# Type definitions
ctypedef np.float64_t DTYPE_t
ctypedef np.int64_t INT_t

# C structure matching BinanceOrderbookUpdate (must match C++ struct)
# Note: Using unsigned char for bool to match C++ bool (1 byte)
cdef packed struct BinanceOrderbookUpdate:
    int64_t timestamp
    double ask_price
    double bid_price
    int64_t time_diff  # Time difference: current_time_ms - binance_timestamp_ms
    uint32_t symbol_index
    unsigned char valid  # C++ bool is 1 byte
    char padding[3]

# C structure matching SharedBinanceOrderbookData (must match C++ struct)
cdef packed struct SharedBinanceOrderbookData:
    uint32_t num_symbols
    uint64_t sequence
    double usdttry_rate  # USDT/TRY exchange rate
    BinanceOrderbookUpdate updates[200]  # Max 200 symbols
    char padding[56]

@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline int _read_updates_fast(
    void* shm_data_ptr,
    DTYPE_t[:, :] arbitrage_table_np,  # Use memoryview instead of ndarray for nogil
    int* global_to_local_map,  # C array: global_idx -> local_idx, -1 means not found
    int max_global_idx,  # Maximum global index (size of map)
    int col_time,
    int col_ask_price,
    int col_bid_price,
    int col_time_diff,  # Column for time_diff (-1 if not used)
    int table_size,
    uint64_t* last_sequence
) nogil:
    """
    Ultra-fast C-level function to read updates from shared memory.
    
    This function operates at C speed with no Python overhead.
    """
    cdef SharedBinanceOrderbookData* shm_data = <SharedBinanceOrderbookData*>shm_data_ptr
    cdef uint64_t current_sequence = shm_data.sequence
    
    # Fast check: no new updates
    if current_sequence == last_sequence[0]:
        return 0
    
    cdef int updates_processed = 0
    cdef int i
    cdef BinanceOrderbookUpdate* update
    cdef uint32_t global_symbol_idx
    cdef int local_symbol_idx
    
    # Fast loop through all slots (C-level, no Python overhead)
    for i in range(200):
        update = &shm_data.updates[i]
        
        # Check if valid (unsigned char: 0 = False, non-zero = True)
        if update.valid != 0:
            global_symbol_idx = update.symbol_index
            
            # Fast array lookup instead of dictionary
            if global_symbol_idx < max_global_idx:
                local_symbol_idx = global_to_local_map[global_symbol_idx]
                
                # Skip if not in mapping (-1 means not found)
                if local_symbol_idx >= 0 and local_symbol_idx < table_size:
                    # Direct memory write to NumPy array (very fast)
                    arbitrage_table_np[local_symbol_idx, col_time] = <DTYPE_t>update.timestamp
                    arbitrage_table_np[local_symbol_idx, col_ask_price] = update.ask_price
                    arbitrage_table_np[local_symbol_idx, col_bid_price] = update.bid_price
                    
                    # Write time_diff if column is specified
                    if col_time_diff >= 0:
                        arbitrage_table_np[local_symbol_idx, col_time_diff] = <DTYPE_t>update.time_diff
                    
                    updates_processed += 1
    
    # Update last sequence
    last_sequence[0] = current_sequence
    
    return updates_processed

@cython.boundscheck(False)
@cython.wraparound(False)
def read_updates_optimized(
    object shm_data,  # ctypes SharedBinanceOrderbookData structure
    np.ndarray[DTYPE_t, ndim=2] arbitrage_table_np,
    object global_to_local_index,  # Python dict or None: {global_idx: local_idx}
    int col_time,
    int col_ask_price,
    int col_bid_price,
    object last_sequence_ref,  # List with one element: [last_sequence] for mutable reference
    int col_time_diff = -1  # Column index for time_diff (-1 if not used)
):
    """
    Optimized Python-callable function to read updates from shared memory.
    
    This function converts the Python dict to a C array for fast lookups,
    then calls the ultra-fast C-level function.
    
    Parameters:
    -----------
    shm_data : ctypes.Structure
        SharedBinanceOrderbookData structure from ctypes
    arbitrage_table_np : np.ndarray
        NumPy array to update
    global_to_local_index : dict or None
        Mapping from global index to local index (None means direct mapping)
    col_* : int
        Column indices
    last_sequence_ref : list
        List with one element containing last sequence number (will be updated)
    col_time_diff : int
        Column index for time_diff (-1 if not used)
    
    Returns:
    --------
    int : Number of updates processed
    """
    # Get pointer to shared memory structure using ctypes.addressof
    # This requires GIL (which we have in this function)
    # ctypes.addressof returns a Python int, convert to intptr_t then to void*
    cdef intptr_t addr = <intptr_t>ctypes.addressof(shm_data)
    cdef void* shm_data_ptr = <void*>addr
    
    # Fast check: no new updates (before expensive dict->array conversion)
    cdef uint64_t current_sequence = shm_data.sequence
    if current_sequence == last_sequence_ref[0]:
        return 0
    
    # Convert Python dict to C array for fast lookups
    # If None, use direct mapping (global_idx == local_idx)
    cdef int max_global_idx = 200  # Max symbols in shared memory
    cdef int* global_to_local_map = <int*>malloc(max_global_idx * sizeof(int))
    if global_to_local_map == NULL:
        raise MemoryError("Failed to allocate memory for index mapping")
    
    cdef int i
    if global_to_local_index is not None:
        # Initialize to -1 (not found)
        for i in range(max_global_idx):
            global_to_local_map[i] = -1
        
        # Fill array from dict
        for global_idx, local_idx in global_to_local_index.items():
            if global_idx < max_global_idx:
                global_to_local_map[global_idx] = local_idx
    else:
        # Direct mapping: global_idx == local_idx (identity mapping)
        for i in range(max_global_idx):
            global_to_local_map[i] = i
    
    # Get table size and create memoryview
    cdef int table_size = arbitrage_table_np.shape[0]
    cdef DTYPE_t[:, :] table_view = arbitrage_table_np
    
    # Call ultra-fast C-level function
    cdef uint64_t last_seq = last_sequence_ref[0]
    cdef int updates = _read_updates_fast(
        shm_data_ptr,
        table_view,
        global_to_local_map,
        max_global_idx,
        col_time,
        col_ask_price,
        col_bid_price,
        col_time_diff,
        table_size,
        &last_seq
    )
    
    # Update last sequence
    last_sequence_ref[0] = last_seq
    
    # Free allocated memory
    free(global_to_local_map)
    
    return updates

