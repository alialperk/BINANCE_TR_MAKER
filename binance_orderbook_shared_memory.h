#ifndef BINANCE_ORDERBOOK_SHARED_MEMORY_H
#define BINANCE_ORDERBOOK_SHARED_MEMORY_H

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <unordered_map>

// Maximum number of symbols supported
#define MAX_SYMBOLS 200

// Shared memory structure for orderbook data
// This structure is shared between C++ clients and Python readers
struct OrderbookEntry {
    double ask_price;      // Best ask price
    double ask_qty;         // Best ask quantity
    double bid_price;       // Best bid price
    double bid_qty;         // Best bid quantity
    int64_t timestamp;      // Timestamp in milliseconds
    int64_t time_diff;      // Time difference (current_time - timestamp)
    uint32_t instrument_id; // Instrument ID (for CS) or 0 (for TR)
    char symbol[16];        // Symbol string (e.g., "ETHTRY", "BTCUSDT")
    uint8_t padding[8];     // Padding for alignment
};

// Shared memory structure
struct BinanceOrderbookSharedMemory {
    uint32_t magic;                    // Magic number for validation (0x42494E41 = "BINA")
    uint32_t version;                  // Version number
    uint32_t num_symbols;              // Number of active symbols
    uint32_t reserved;                 // Reserved for future use
    OrderbookEntry entries[MAX_SYMBOLS]; // Array of orderbook entries
    char padding[4096 - sizeof(uint32_t) * 4 - sizeof(OrderbookEntry) * MAX_SYMBOLS]; // Padding to 4KB
};

#define SHM_MAGIC 0x42494E41  // "BINA" in ASCII
#define SHM_VERSION 1
#define SHM_NAME_BINANCE_TR "/binance_tr_orderbook_shm"
#define SHM_NAME_BINANCE_CS "/binance_cs_orderbook_shm"

// Helper class to manage shared memory
class BinanceOrderbookSharedMemoryManager {
private:
    const char* shm_name;
    int shm_fd;
    BinanceOrderbookSharedMemory* shm_ptr;
    std::unordered_map<std::string, size_t> symbol_to_index;
    size_t next_index;
    bool initialized;

public:
    BinanceOrderbookSharedMemoryManager(const char* name) 
        : shm_name(name), shm_fd(-1), shm_ptr(nullptr), next_index(0), initialized(false) {
    }

    ~BinanceOrderbookSharedMemoryManager() {
        cleanup();
    }

    bool initialize(const std::unordered_map<std::string, size_t>& symbol_map) {
        if (initialized) {
            return true;
        }

        symbol_to_index = symbol_map;

        // Create or open shared memory
        shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
        if (shm_fd == -1) {
            return false;
        }

        // Set size
        if (ftruncate(shm_fd, sizeof(BinanceOrderbookSharedMemory)) == -1) {
            close(shm_fd);
            return false;
        }

        // Map shared memory
        shm_ptr = (BinanceOrderbookSharedMemory*)mmap(nullptr, sizeof(BinanceOrderbookSharedMemory),
                                                       PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        if (shm_ptr == MAP_FAILED) {
            close(shm_fd);
            return false;
        }

        // Initialize if new
        if (shm_ptr->magic != SHM_MAGIC) {
            memset(shm_ptr, 0, sizeof(BinanceOrderbookSharedMemory));
            shm_ptr->magic = SHM_MAGIC;
            shm_ptr->version = SHM_VERSION;
            shm_ptr->num_symbols = 0;
        }

        initialized = true;
        return true;
    }

    bool update_orderbook(const std::string& symbol, double ask_price, double ask_qty,
                         double bid_price, double bid_qty, int64_t timestamp) {
        if (!initialized || !shm_ptr) {
            return false;
        }

        auto it = symbol_to_index.find(symbol);
        if (it == symbol_to_index.end()) {
            return false;
        }

        size_t idx = it->second;
        if (idx >= MAX_SYMBOLS) {
            return false;
        }

        OrderbookEntry& entry = shm_ptr->entries[idx];
        entry.ask_price = ask_price;
        entry.ask_qty = ask_qty;
        entry.bid_price = bid_price;
        entry.bid_qty = bid_qty;
        entry.timestamp = timestamp;
        
        // Calculate time difference
        int64_t current_time = timestamp; // Use timestamp as current time for now
        entry.time_diff = 0; // Will be calculated by Python reader
        
        // Copy symbol string
        strncpy(entry.symbol, symbol.c_str(), sizeof(entry.symbol) - 1);
        entry.symbol[sizeof(entry.symbol) - 1] = '\0';

        // Update count if this is a new symbol
        if (idx >= shm_ptr->num_symbols) {
            shm_ptr->num_symbols = idx + 1;
        }

        return true;
    }

    bool update_orderbook_cs(uint32_t instrument_id, const std::string& symbol,
                            double ask_price, double ask_qty, double bid_price, double bid_qty,
                            int64_t timestamp) {
        if (!initialized || !shm_ptr) {
            return false;
        }

        auto it = symbol_to_index.find(symbol);
        if (it == symbol_to_index.end()) {
            return false;
        }

        size_t idx = it->second;
        if (idx >= MAX_SYMBOLS) {
            return false;
        }

        OrderbookEntry& entry = shm_ptr->entries[idx];
        entry.ask_price = ask_price;
        entry.ask_qty = ask_qty;
        entry.bid_price = bid_price;
        entry.bid_qty = bid_qty;
        entry.timestamp = timestamp;
        entry.instrument_id = instrument_id;
        
        // Calculate time difference
        entry.time_diff = 0; // Will be calculated by Python reader
        
        // Copy symbol string
        strncpy(entry.symbol, symbol.c_str(), sizeof(entry.symbol) - 1);
        entry.symbol[sizeof(entry.symbol) - 1] = '\0';

        // Update count if this is a new symbol
        if (idx >= shm_ptr->num_symbols) {
            shm_ptr->num_symbols = idx + 1;
        }

        return true;
    }

    void cleanup() {
        if (shm_ptr && shm_ptr != MAP_FAILED) {
            munmap(shm_ptr, sizeof(BinanceOrderbookSharedMemory));
            shm_ptr = nullptr;
        }
        if (shm_fd >= 0) {
            close(shm_fd);
            shm_fd = -1;
        }
        initialized = false;
    }
};

#endif // BINANCE_ORDERBOOK_SHARED_MEMORY_H
