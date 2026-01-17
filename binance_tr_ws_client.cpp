#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <chrono>
#include <iomanip>
#include <x86intrin.h>  // For RDTSC
#include <sched.h>      // For CPU affinity
#include <sys/syscall.h>
#include <fstream>
#include <vector>
#include <sstream>
#include <map>
#include <csignal>
#include <atomic>
#include <unordered_map>
#include "binance_orderbook_shared_memory.h"

// HFT OPTIMIZATION: RDTSC for ultra-fast timestamps (CPU cycle counter)
inline uint64_t rdtsc() {
    unsigned int lo, hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((uint64_t)hi << 32) | lo;
}

// Calibrate RDTSC to nanoseconds (call once at startup)
static uint64_t rdtsc_freq = 0;
inline void calibrate_rdtsc() {
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t start_tsc = rdtsc();
    usleep(100000); // 100ms
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t end_tsc = rdtsc();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    rdtsc_freq = (end_tsc - start_tsc) * 1000000000ULL / duration;
}

inline uint64_t get_timestamp_ns() {
    if (rdtsc_freq == 0) {
        return std::chrono::high_resolution_clock::now().time_since_epoch().count();
    }
    return (rdtsc() * 1000000000ULL) / rdtsc_freq;
}

// HFT OPTIMIZATION: Set CPU affinity to specific core (reduces cache misses)
inline void set_cpu_affinity(int cpu) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
        perror("sched_setaffinity");
    }
}

// HFT OPTIMIZATION: Fast string search using memmem (glibc optimized)
inline const char* fast_strstr(const char* haystack, size_t haystack_len, const char* needle, size_t needle_len) {
    if (needle_len == 0) return haystack;
    if (haystack_len < needle_len) return nullptr;
    return (const char*)memmem(haystack, haystack_len, needle, needle_len);
}

// HFT OPTIMIZATION: Ultra-fast double parsing (no std::stod overhead)
inline double fast_atof(const char* str, size_t len) {
    double result = 0.0;
    double fraction = 0.1;
    bool has_dot = false;
    bool negative = false;
    
    if (len == 0) return 0.0;
    if (*str == '-') {
        negative = true;
        str++;
        len--;
    }
    
    for (size_t i = 0; i < len && str[i] != '\0'; i++) {
        if (str[i] == '.') {
            has_dot = true;
            continue;
        }
        if (str[i] >= '0' && str[i] <= '9') {
            if (!has_dot) {
                result = result * 10.0 + (str[i] - '0');
            } else {
                result += (str[i] - '0') * fraction;
                fraction *= 0.1;
            }
        } else {
            break;
        }
    }
    
    return negative ? -result : result;
}

// Minimal logging for HFT (disabled in hot path by default)
static bool enable_logging = true;
inline void log_fast(const char* msg) {
    if (!enable_logging) return;
    auto ns = get_timestamp_ns();
    std::cout << "[" << ns << "] " << msg << std::endl;
}

// Global flag for graceful shutdown
static std::atomic<bool> g_running(true);

// Signal handler for Ctrl+C (SIGINT)
void signal_handler(int signal) {
    if (signal == SIGINT) {
        enable_logging = true;
        log_fast("\nReceived SIGINT (Ctrl+C), shutting down gracefully...");
        g_running = false;
    }
}

// Parse WebSocket frame - ULTRA OPTIMIZED with branch prediction hints
__attribute__((always_inline))
inline bool parse_ws_frame(const unsigned char* data, size_t len, const char** payload, size_t* payload_len, size_t* frame_size) {
    if (__builtin_expect(len < 2, 0)) return false;
    
    bool fin = (data[0] & 0x80) != 0;
    unsigned char opcode = data[0] & 0x0F;
    bool masked = (data[1] & 0x80) != 0;
    size_t plen = data[1] & 0x7F;
    size_t offset = 2;
    
    if (__builtin_expect(plen == 126, 0)) {
        if (__builtin_expect(len < 4, 0)) return false;
        plen = (data[2] << 8) | data[3];
        offset = 4;
    } else if (__builtin_expect(plen == 127, 0)) {
        if (__builtin_expect(len < 10, 0)) return false;
        // Optimized 64-bit read
        plen = __builtin_bswap64(*(uint64_t*)(data + 2));
        offset = 10;
    }
    
    size_t mask_offset = masked ? 4 : 0;
    size_t total_size = offset + mask_offset + plen;
    
    if (__builtin_expect(len < total_size, 0)) return false;
    
    // Server frames are NOT masked (common case)
    if (__builtin_expect(masked, 0)) {
        // mask is at data + offset, but we don't need to unmask server frames
        *payload = (const char*)(data + offset + 4);
        *payload_len = plen;
    } else {
        *payload = (const char*)(data + offset);
        *payload_len = plen;
    }
    
    *frame_size = total_size;
    
    // Handle ping (0x09) - respond with pong (required by Binance TR)
    if (__builtin_expect(opcode == 0x09, 0)) {
        // Return ping frame info so we can respond with pong
        *frame_size = total_size;
        return false; // Don't process as data, but frame_size is set
    }
    
    // Handle close (0x08)
    if (__builtin_expect(opcode == 0x08, 0)) {
        return false;
    }
    
    return fin && (opcode == 0x01 || opcode == 0x02); // TEXT or BINARY
}

// Load symbol-to-index mapping from JSON file for shared memory
// Format: {"symbol_to_index": {"ETHTRY": 0, "XRPTRY": 1, ...}}
std::unordered_map<std::string, size_t> load_symbol_to_index_mapping(const std::string& filename) {
    std::unordered_map<std::string, size_t> mapping;
    std::ifstream file(filename);
    if (!file.is_open()) {
        return mapping;
    }
    
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();
    
    // Simple JSON parsing: look for "symbol_to_index" field
    size_t pos = content.find("\"symbol_to_index\"");
    if (pos == std::string::npos) return mapping;
    
    pos = content.find('{', pos);
    if (pos == std::string::npos) return mapping;
    pos++;
    
    // Parse key-value pairs
    while (pos < content.length()) {
        // Skip whitespace
        while (pos < content.length() && (content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r' || content[pos] == '\t' || content[pos] == ',')) pos++;
        if (pos >= content.length() || content[pos] == '}') break;
        
        // Parse key (symbol string)
        if (content[pos] != '"') break;
        pos++;
        const char* key_start = content.c_str() + pos;
        while (pos < content.length() && content[pos] != '"') pos++;
        if (pos >= content.length()) break;
        std::string symbol(key_start, content.c_str() + pos - key_start);
        pos++; // Skip closing quote
        
        // Skip colon and whitespace
        while (pos < content.length() && (content[pos] == ' ' || content[pos] == ':')) pos++;
        
        // Parse value (index number)
        const char* val_start = content.c_str() + pos;
        char* val_end = nullptr;
        size_t index = static_cast<size_t>(strtoul(val_start, &val_end, 10));
        if (val_end == val_start) break;
        mapping[symbol] = index;
        pos = val_end - content.c_str();
    }
    
    return mapping;
}

// Simple JSON parser to extract streams from CS_common_instruments.json
// Format: [{"base_symbol":"ETH","binance_tr_symbol":"ETHTRY","binance_symbol":"ETHUSDT","binance_tr_stream":"ethtry@depth5@100ms"},...]
std::vector<std::string> load_instruments_from_json(const std::string& filename) {
    std::vector<std::string> streams;
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::string error_msg = "ERROR: Could not open " + filename;
        log_fast(error_msg.c_str());
        return streams;
    }
    
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();
    
    // Simple JSON parsing for new format: look for "binance_tr_stream" field
    size_t pos = 0;
    while ((pos = content.find("\"binance_tr_stream\"", pos)) != std::string::npos) {
        pos += 19; // Skip "binance_tr_stream"
        // Find the colon
        while (pos < content.length() && (content[pos] == ' ' || content[pos] == ':')) pos++;
        if (pos >= content.length() || content[pos] != '"') continue;
        pos++; // Skip opening quote
        size_t stream_start = pos;
        // Find closing quote
        while (pos < content.length() && content[pos] != '"') pos++;
        if (pos < content.length()) {
            std::string stream = content.substr(stream_start, pos - stream_start);
            streams.push_back(stream);
        }
        pos++;
    }
    
    return streams;
}

// Structure to store symbol data for table display
struct SymbolData {
    double ask_price;
    double ask_qty;
    double bid_price;
    double bid_qty;
    double spread;
    double spread_pct;
};

// Global symbol data map (shared between process_depth and main)
static std::map<std::string, SymbolData> g_symbol_data;

// Shared memory manager
static BinanceOrderbookSharedMemoryManager* g_shm_manager = nullptr;

// Extract symbol from depth message - Binance TR format
// Combined stream format: {"stream":"ethtry@depth5@100ms","data":{"e":"depthUpdate","s":"ETHTRY",...}}
// Raw stream format: {"e":"depthUpdate","s":"ETHTRY",...}
std::string extract_symbol(const char* data, size_t len) {
    // First try to find "stream" field at top level (for combined streams)
    static const char stream_marker[] = "\"stream\":\"";
    static const size_t stream_marker_len = sizeof(stream_marker) - 1;
    const char* stream_start = fast_strstr(data, len, stream_marker, stream_marker_len);
    std::string symbol_from_stream;
    
    if (stream_start) {
        stream_start += stream_marker_len;
        const char* data_end = data + len;
        const char* stream_end = (const char*)memchr(stream_start, '"', data_end - stream_start);
        if (stream_end) {
            std::string stream = std::string(stream_start, stream_end - stream_start);
            // Extract symbol from stream name (e.g., "ethtry@depth5@100ms" -> "ETHTRY")
            size_t at_pos = stream.find('@');
            if (at_pos != std::string::npos) {
                symbol_from_stream = stream.substr(0, at_pos);
                // Convert to uppercase
                for (char& c : symbol_from_stream) {
                    if (c >= 'a' && c <= 'z') c = c - 'a' + 'A';
                }
            }
        }
    }
    
    // Try to find "data" field and extract symbol from it (for combined streams)
    // Or find "s" field directly (for raw streams)
    const char* data_field_start = nullptr;
    if (stream_start) {
        // Combined stream: look for "data":{...}
        static const char data_marker[] = "\"data\":";
        static const size_t data_marker_len = sizeof(data_marker) - 1;
        data_field_start = fast_strstr(data, len, data_marker, data_marker_len);
        if (data_field_start) {
            data_field_start += data_marker_len;
            // Skip whitespace and {
            while (data_field_start < data + len && 
                   (*data_field_start == ' ' || *data_field_start == '\t' || *data_field_start == '{')) {
                data_field_start++;
            }
        }
    } else {
        // Raw stream: use data directly
        data_field_start = data;
    }
    
    // Look for "s":"SYMBOL" pattern in data field
    if (data_field_start) {
        size_t remaining_len = len - (data_field_start - data);
        static const char symbol_marker[] = "\"s\":\"";
        static const size_t marker_len = sizeof(symbol_marker) - 1;
        const char* symbol_start = fast_strstr(data_field_start, remaining_len, symbol_marker, marker_len);
        if (symbol_start) {
            symbol_start += marker_len;
            const char* data_end = data + len;
            const char* symbol_end = (const char*)memchr(symbol_start, '"', data_end - symbol_start);
            if (symbol_end) {
                std::string symbol = std::string(symbol_start, symbol_end - symbol_start);
                // Prefer symbol from stream name if available (more reliable)
                return symbol_from_stream.empty() ? symbol : symbol_from_stream;
            }
        }
    }
    
    // Fallback: use symbol from stream name if we found it
    if (!symbol_from_stream.empty()) {
        return symbol_from_stream;
    }
    
    return "UNKNOWN";
}

// Process depth5 orderbook - ZERO COPY, ZERO ALLOCATION for HFT
__attribute__((always_inline))
inline void process_depth(const char* data, size_t len, uint64_t /* recv_time */, const std::string& symbol = "") {
    // For combined streams, extract the actual data payload
    const char* actual_data = data;
    size_t actual_len = len;
    
    // Check if this is a combined stream format: {"stream":"...","data":{...}}
    static const char data_marker[] = "\"data\":";
    static const size_t data_marker_len = sizeof(data_marker) - 1;
    const char* data_field = fast_strstr(data, len, data_marker, data_marker_len);
    
    if (data_field) {
        // Combined stream format - extract data field
        data_field += data_marker_len;
        // Skip whitespace and opening brace
        while (data_field < data + len && 
               (*data_field == ' ' || *data_field == '\t' || *data_field == '{')) {
            data_field++;
        }
        // Find the matching closing brace (simple approach: find last })
        const char* data_end = data + len;
        const char* last_brace = (const char*)memrchr(data_field, '}', data_end - data_field);
        if (last_brace) {
            actual_data = data_field;
            actual_len = last_brace - data_field + 1;
        }
    }
    
    // Check if it's a snapshot or depthUpdate in the actual data
    static const char depth_marker[] = "\"e\":\"depthUpdate\"";
    static const char snapshot_marker[] = "\"lastUpdateId\"";
    static const size_t depth_marker_len = sizeof(depth_marker) - 1;
    static const size_t snapshot_marker_len = sizeof(snapshot_marker) - 1;
    
    bool is_snapshot = (fast_strstr(actual_data, actual_len, snapshot_marker, snapshot_marker_len) != nullptr);
    bool is_update = (fast_strstr(actual_data, actual_len, depth_marker, depth_marker_len) != nullptr);
    
    if (!is_snapshot && !is_update) {
        return; // Not a depth message, skip
    }
    
    // Extract symbol from message if not provided (use original data for symbol extraction)
    std::string msg_symbol = symbol.empty() ? extract_symbol(data, len) : symbol;
    
    // Parse bids array: "bids":[[price,qty],[price,qty],...]
    // Find "bids":[[ or "b":[[ (use actual_data for combined streams)
    static const char bid_marker[] = "\"bids\":[[";
    static const size_t bid_marker_len = sizeof(bid_marker) - 1;
    const char* bid_array_start = fast_strstr(actual_data, actual_len, bid_marker, bid_marker_len);
    if (!bid_array_start) {
        // Try alternative format "b":[[
        static const char bid_marker_alt[] = "\"b\":[[";
        bid_array_start = fast_strstr(actual_data, actual_len, bid_marker_alt, sizeof(bid_marker_alt) - 1);
        if (!bid_array_start) return;
        bid_array_start += sizeof(bid_marker_alt) - 1; // Points to after [[
    } else {
        bid_array_start += bid_marker_len; // Points to after [[
    }
    
    // Parse asks array: "asks":[[price,qty],[price,qty],...]
    static const char ask_marker[] = "\"asks\":[[";
    static const size_t ask_marker_len = sizeof(ask_marker) - 1;
    const char* ask_array_start = fast_strstr(actual_data, actual_len, ask_marker, ask_marker_len);
    if (!ask_array_start) {
        // Try alternative format "a":[[
        static const char ask_marker_alt[] = "\"a\":[[";
        ask_array_start = fast_strstr(actual_data, actual_len, ask_marker_alt, sizeof(ask_marker_alt) - 1);
        if (!ask_array_start) return;
        ask_array_start += sizeof(ask_marker_alt) - 1; // Points to after [[
    } else {
        ask_array_start += ask_marker_len; // Points to after [[
    }
    
    // Parse up to 5 levels of bids and asks
    struct Level {
        double price;
        double qty;
        Level() : price(0.0), qty(0.0) {}
    };
    Level bids[5];
    Level asks[5];
    int bid_count = 0;
    int ask_count = 0;
    
    // Parse bids (up to 5 levels)
    // After "bids":[[, we're at position right after [[, which should be " for first price
    const char* pos = bid_array_start;
    const char* data_end = actual_data + actual_len;
    while (bid_count < 5 && pos < data_end) {
        // Skip whitespace
        while (pos < data_end && (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r')) pos++;
        if (pos >= data_end) break;
        if (*pos == ']') break; // End of array
        
        // Each entry is ["price","qty"] - we need to find the opening [
        if (*pos != '[') {
            // Look for next [
            const char* next_bracket = (const char*)memchr(pos, '[', data_end - pos);
            if (!next_bracket) break;
            pos = next_bracket;
        }
        pos++; // Skip [
        
        // Skip whitespace
        while (pos < data_end && (*pos == ' ' || *pos == '\t')) pos++;
        if (pos >= data_end || *pos != '"') break;
        pos++; // Skip quote
        
        // Parse price
        const char* price_start = pos;
        const char* price_end = (const char*)memchr(price_start, '"', data_end - price_start);
        if (!price_end) break;
        bids[bid_count].price = fast_atof(price_start, price_end - price_start);
        pos = price_end + 1;
        
        // Skip whitespace, comma
        while (pos < data_end && (*pos == ' ' || *pos == ',' || *pos == '\t')) pos++;
        if (pos >= data_end || *pos != '"') break;
        pos++; // Skip quote
        
        // Parse quantity
        const char* qty_start = pos;
        const char* qty_end = (const char*)memchr(qty_start, '"', data_end - qty_start);
        if (!qty_end) break;
        bids[bid_count].qty = fast_atof(qty_start, qty_end - qty_start);
        pos = qty_end + 1;
        
        bid_count++;
        
        // Skip ], whitespace to next entry
        while (pos < data_end && (*pos == ' ' || *pos == ']' || *pos == ',' || *pos == '\t' || *pos == '\n' || *pos == '\r')) pos++;
        if (pos >= data_end || *pos == ']') break;
    }
    
    // Parse asks (up to 5 levels) - same logic as bids
    pos = ask_array_start;
    data_end = actual_data + actual_len; // Reset for asks parsing
    while (ask_count < 5 && pos < data_end) {
        // Skip whitespace
        while (pos < data_end && (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r')) pos++;
        if (pos >= data_end) break;
        if (*pos == ']') break;
        
        // Each entry is ["price","qty"]
        if (*pos != '[') {
            const char* next_bracket = (const char*)memchr(pos, '[', data_end - pos);
            if (!next_bracket) break;
            pos = next_bracket;
        }
        pos++; // Skip [
        
        // Skip whitespace
        while (pos < data_end && (*pos == ' ' || *pos == '\t')) pos++;
        if (pos >= data_end || *pos != '"') break;
        pos++; // Skip quote
        
        // Parse price
        const char* price_start = pos;
        const char* price_end = (const char*)memchr(price_start, '"', data_end - price_start);
        if (!price_end) break;
        asks[ask_count].price = fast_atof(price_start, price_end - price_start);
        pos = price_end + 1;
        
        // Skip whitespace, comma
        while (pos < data_end && (*pos == ' ' || *pos == ',' || *pos == '\t')) pos++;
        if (pos >= data_end || *pos != '"') break;
        pos++; // Skip quote
        
        // Parse quantity
        const char* qty_start = pos;
        const char* qty_end = (const char*)memchr(qty_start, '"', data_end - qty_start);
        if (!qty_end) break;
        asks[ask_count].qty = fast_atof(qty_start, qty_end - qty_start);
        pos = qty_end + 1;
        
        ask_count++;
        
        // Skip ], whitespace to next entry
        while (pos < data_end && (*pos == ' ' || *pos == ']' || *pos == ',' || *pos == '\t' || *pos == '\n' || *pos == '\r')) pos++;
        if (pos >= data_end || *pos == ']') break;
    }
    
    // Only log if we successfully parsed at least one level
    if (bid_count == 0 && ask_count == 0) {
        return; // Parsing failed, skip
    }
    
    // Calculate spread
    double spread = 0.0;
    double spread_pct = 0.0;
    if (bid_count > 0 && ask_count > 0) {
        spread = asks[0].price - bids[0].price;
        spread_pct = (spread / bids[0].price) * 100.0;
    }
    
    // Update data for this symbol in global map
    g_symbol_data[msg_symbol] = {
        ask_count > 0 ? asks[0].price : 0.0,
        ask_count > 0 ? asks[0].qty : 0.0,
        bid_count > 0 ? bids[0].price : 0.0,
        bid_count > 0 ? bids[0].qty : 0.0,
        spread,
        spread_pct
    };
    
    // Update shared memory if available
    if (g_shm_manager && bid_count > 0 && ask_count > 0) {
        int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        g_shm_manager->update_orderbook(msg_symbol, asks[0].price, asks[0].qty,
                                       bids[0].price, bids[0].qty, timestamp);
    }
    
    // Don't print here - will be printed every 5 seconds in main loop
}

// Function to print the table (called every 5 seconds)
void print_table(const std::map<std::string, SymbolData>& symbol_data, int update_count) {
    char output[2048];
    int written = snprintf(output, sizeof(output), 
        "\033[2J\033[H"  // Clear screen and move cursor to top
        "═══════════════════════════════════════════════════════════════════════════════\n"
        "  BINANCE TR - TOP OF BOOK (Update #%d)\n"
        "═══════════════════════════════════════════════════════════════════════════════\n"
        "  Symbol    │  Best Ask Price  │  Best Ask Qty   │  Best Bid Price  │  Best Bid Qty   │  Spread      │  Spread %%\n"
        "────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────┼──────────────┼───────────\n",
        update_count);
    ssize_t result = write(STDOUT_FILENO, output, written);
    (void)result; // Explicitly ignore return value
    
    // Print all symbols in table
    for (const auto& pair : symbol_data) {
        written = snprintf(output, sizeof(output),
            "  %-10s │  %15.2f │  %15.8f │  %15.2f │  %15.8f │  %12.2f │  %8.4f%%\n",
            pair.first.c_str(),
            pair.second.ask_price,
            pair.second.ask_qty,
            pair.second.bid_price,
            pair.second.bid_qty,
            pair.second.spread,
            pair.second.spread_pct);
        result = write(STDOUT_FILENO, output, written);
        (void)result; // Explicitly ignore return value
    }
    
    written = snprintf(output, sizeof(output), 
        "═══════════════════════════════════════════════════════════════════════════════\n");
    result = write(STDOUT_FILENO, output, written);
    (void)result; // Explicitly ignore return value
}

int main() {
    // Register signal handler for Ctrl+C
    signal(SIGINT, signal_handler);
    
    // HFT OPTIMIZATION: Pin to CPU core 0 (adjust based on your system)
    set_cpu_affinity(0);
    
    // HFT OPTIMIZATION: Calibrate RDTSC for fast timestamps
    calibrate_rdtsc();
    
    log_fast("=== ULTRA-LOW LATENCY HFT CLIENT (OPTIMIZED) ===");
    log_fast("Press Ctrl+C to stop gracefully");
    
    // Initialize OpenSSL
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();
    
    SSL_CTX* ssl_ctx = SSL_CTX_new(TLS_client_method());
    if (!ssl_ctx) {
        log_fast("ERROR: Failed to create SSL context");
        return 1;
    }
    
    // Create socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        log_fast("ERROR: Failed to create socket");
        return 1;
    }
    
    // HFT OPTIMIZATION: Set TCP_NODELAY for minimum latency
    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    
    // HFT OPTIMIZATION: Disable TCP delayed ACK (reduces latency)
    int quickack = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
    
    // HFT OPTIMIZATION: Set socket buffer sizes (tune for your system)
    int bufsize = 1024 * 1024; // 1MB
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
    
    // Resolve Binance TR hostname
    struct hostent* host = gethostbyname("stream-cloud.binance.tr");
    if (!host) {
        log_fast("ERROR: Failed to resolve hostname");
        return 1;
    }
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(443);
    addr.sin_addr = *((struct in_addr*)host->h_addr);
    
    log_fast("Connecting to Binance TR...");
    
    // Connect
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        log_fast("ERROR: Connection failed");
        return 1;
    }
    
    log_fast("TCP connected");
    
    // SSL handshake
    SSL* ssl = SSL_new(ssl_ctx);
    SSL_set_fd(ssl, sock);
    
    if (SSL_connect(ssl) <= 0) {
        log_fast("ERROR: SSL handshake failed");
        ERR_print_errors_fp(stderr);
        return 1;
    }
    
    log_fast("SSL connected");
    
    // HFT OPTIMIZATION: Set socket to non-blocking AFTER connection is established
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
    
    // Load symbol-to-index mapping for shared memory
    std::unordered_map<std::string, size_t> symbol_to_index = load_symbol_to_index_mapping("binance_tr_symbol_mapping.json");
    if (!symbol_to_index.empty()) {
        g_shm_manager = new BinanceOrderbookSharedMemoryManager(SHM_NAME_BINANCE_TR);
        if (g_shm_manager->initialize(symbol_to_index)) {
            char log_msg[512];
            snprintf(log_msg, sizeof(log_msg), "Shared memory initialized with %zu symbol(s)", symbol_to_index.size());
            log_fast(log_msg);
        } else {
            log_fast("WARNING: Failed to initialize shared memory, continuing without it");
            delete g_shm_manager;
            g_shm_manager = nullptr;
        }
    } else {
        log_fast("WARNING: No symbol-to-index mapping found, shared memory disabled");
    }
    
    // Load instruments from JSON file
    std::vector<std::string> streams = load_instruments_from_json("CS_common_instruments.json");
    if (streams.empty()) {
        log_fast("ERROR: No streams found in CS_common_instruments.json");
        log_fast("Falling back to default: btctry@depth5@100ms");
        streams.push_back("btctry@depth5@100ms");
    } else {
        char log_msg[512];
        snprintf(log_msg, sizeof(log_msg), "Loaded %zu stream(s) from CS_common_instruments.json", streams.size());
        log_fast(log_msg);
    }
    
    // Build WebSocket URL with all streams using combined stream format
    // Format: /stream?streams=stream1/stream2/stream3
    std::stringstream ws_path;
    if (streams.size() == 1) {
        // Single stream: use raw stream format
        ws_path << "/ws/" << streams[0];
    } else {
        // Multiple streams: use combined stream format
        ws_path << "/stream?streams=";
        for (size_t i = 0; i < streams.size(); i++) {
            if (i > 0) ws_path << "/";
            ws_path << streams[i];
        }
    }
    
    // Build handshake request
    std::stringstream handshake_ss;
    handshake_ss << "GET " << ws_path.str() << " HTTP/1.1\r\n"
                 << "Host: stream-cloud.binance.tr\r\n"
                 << "Upgrade: websocket\r\n"
                 << "Connection: Upgrade\r\n"
                 << "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==\r\n"
                 << "Sec-WebSocket-Version: 13\r\n"
                 << "\r\n";
    
    std::string handshake = handshake_ss.str();
    // Store handshake string for reconnection
    std::string handshake_str = handshake;
    
    SSL_write(ssl, handshake.c_str(), handshake.length());
    
    char log_msg[512];
    snprintf(log_msg, sizeof(log_msg), "WebSocket handshake sent for %zu stream(s) using %s format", 
             streams.size(), streams.size() == 1 ? "raw" : "combined");
    log_fast(log_msg);
    
    // Read HTTP response (handle non-blocking properly)
    char http_buf[4096];
    int http_len = 0;
    int total_read = 0;
    
    // Read until we get the full HTTP response
    while (total_read < (int)sizeof(http_buf) - 1) {
        int n = SSL_read(ssl, http_buf + total_read, sizeof(http_buf) - total_read - 1);
        if (n > 0) {
            total_read += n;
            http_buf[total_read] = '\0';
            // Check if we have complete headers (ends with \r\n\r\n)
            if (strstr(http_buf, "\r\n\r\n")) {
                break;
            }
        } else {
            int err = SSL_get_error(ssl, n);
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                usleep(1000); // Small delay for non-blocking
                continue;
            } else {
                break;
            }
        }
    }
    
    http_len = total_read;
    if (http_len > 0) {
        http_buf[http_len] = '\0';
        if (strstr(http_buf, "101") && strstr(http_buf, "Upgrade")) {
            log_fast("✓ WebSocket connected!");
        } else {
            log_fast("ERROR: Handshake failed");
            std::cout << "HTTP Response:\n" << http_buf << std::endl;
            return 1;
        }
    } else {
        log_fast("ERROR: No HTTP response received");
        return 1;
    }
    
    log_fast("=== STREAMING MARKET DATA (HFT OPTIMIZED) ===");
    
    // HFT OPTIMIZATION: Disable logging in hot path for maximum performance
    enable_logging = false;
    
    // HFT OPTIMIZATION: Pre-allocated buffer on stack (no heap allocation)
    alignas(64) unsigned char buffer[65536];  // Cache-line aligned
    size_t buffer_pos = 0;
    
    // Table printing variables
    int update_count = 0;
    uint64_t last_print_time = get_timestamp_ns();
    const uint64_t PRINT_INTERVAL_NS = 5000000000ULL; // 5 seconds in nanoseconds
    
    // HFT OPTIMIZATION: Main loop - ULTRA FAST with branch prediction
    // Note: Server sends ping every 3 minutes, we respond with pong (no need to send ping ourselves)
    while (g_running) {
        
        // Read data
        int n = SSL_read(ssl, buffer + buffer_pos, sizeof(buffer) - buffer_pos);
        
        if (__builtin_expect(n <= 0, 0)) {
            int err = SSL_get_error(ssl, n);
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                usleep(1000); // Small delay for non-blocking
                continue;
            }
            // Check if we should exit
            if (!g_running) {
                break;
            }
            
            enable_logging = true;
            char error_msg[256];
            snprintf(error_msg, sizeof(error_msg), "Connection closed (error: %d). Attempting to reconnect...", err);
            log_fast(error_msg);
            
            // Try to reconnect
            SSL_shutdown(ssl);
            SSL_free(ssl);
            close(sock);
            
            // Wait 2 seconds before reconnecting, but check g_running periodically
            for (int i = 0; i < 2000 && g_running; i++) {
                usleep(1000); // 1ms increments
            }
            
            if (!g_running) {
                break;
            }
            
            // Reconnect
            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
                log_fast("ERROR: Failed to recreate socket");
                break;
            }
            
            int flag = 1;
            setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
            int quickack = 1;
            setsockopt(sock, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
            
            if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                log_fast("ERROR: Reconnection failed");
                break;
            }
            
            ssl = SSL_new(ssl_ctx);
            SSL_set_fd(ssl, sock);
            if (SSL_connect(ssl) <= 0) {
                log_fast("ERROR: SSL reconnection failed");
                break;
            }
            
            flags = fcntl(sock, F_GETFL, 0);
            fcntl(sock, F_SETFL, flags | O_NONBLOCK);
            
            // Resend handshake
            SSL_write(ssl, handshake_str.c_str(), handshake_str.length());
            log_fast("Reconnected, resending handshake...");
            
            // Read handshake response
            total_read = 0;
            while (total_read < (int)sizeof(http_buf) - 1) {
                int n = SSL_read(ssl, http_buf + total_read, sizeof(http_buf) - total_read - 1);
                if (n > 0) {
                    total_read += n;
                    http_buf[total_read] = '\0';
                    if (strstr(http_buf, "\r\n\r\n")) {
                        break;
                    }
                } else {
                    int err = SSL_get_error(ssl, n);
                    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                        usleep(1000);
                        continue;
                    } else {
                        break;
                    }
                }
            }
            
            if (strstr(http_buf, "101") && strstr(http_buf, "Upgrade")) {
                log_fast("✓ Reconnected successfully!");
                buffer_pos = 0;
                continue;
            } else {
                log_fast("ERROR: Reconnection handshake failed");
                break;
            }
        }
        
        // HFT: Timestamp immediately after receive (critical for latency measurement)
        uint64_t recv_time = get_timestamp_ns();
        
        buffer_pos += n;
        
        // HFT OPTIMIZATION: Parse frames with minimal branching
        size_t offset = 0;
        while (__builtin_expect(offset < buffer_pos, 1)) {
            const char* payload;
            size_t payload_len;
            size_t frame_size = 0;
            
            // Check opcode before parsing (for ping handling)
            unsigned char opcode = 0;
            if (buffer_pos - offset >= 2) {
                opcode = (buffer[offset] & 0x0F);
            }
            
            if (__builtin_expect(parse_ws_frame(buffer + offset, buffer_pos - offset, 
                                                &payload, &payload_len, &frame_size), 1)) {
                // Process depth data - ZERO COPY, ZERO ALLOCATION
                // This updates symbol_data internally
                process_depth(payload, payload_len, recv_time, "");
                
                offset += frame_size;
                
                // Check if it's time to print table (every 5 seconds)
                uint64_t now = get_timestamp_ns();
                if (now - last_print_time >= PRINT_INTERVAL_NS) {
                    update_count++;
                    print_table(g_symbol_data, update_count);
                    last_print_time = now;
                }
            } else {
                // Handle ping frame from server - respond with pong (required: respond within 10 minutes)
                if (opcode == 0x09 && frame_size > 0) {
                    // Extract ping payload if any
                    size_t ping_payload_len = 0;
                    const unsigned char* ping_payload = nullptr;
                    
                    // Parse ping frame to get payload
                    if (frame_size >= 2) {
                        unsigned char mask_bit = (buffer[offset + 1] & 0x80) != 0;
                        size_t payload_len = buffer[offset + 1] & 0x7F;
                        size_t header_len = 2;
                        
                        if (payload_len == 126) {
                            if (frame_size >= 4) {
                                payload_len = (buffer[offset + 2] << 8) | buffer[offset + 3];
                                header_len = 4;
                            }
                        } else if (payload_len == 127) {
                            if (frame_size >= 10) {
                                payload_len = __builtin_bswap64(*(uint64_t*)(buffer + offset + 2));
                                header_len = 10;
                            }
                        }
                        
                        if (mask_bit) header_len += 4;
                        
                        if (frame_size >= header_len + payload_len) {
                            ping_payload = buffer + offset + header_len;
                            ping_payload_len = payload_len;
                        }
                    }
                    
                    // Construct pong frame (same as ping but opcode 0x0A)
                    // Pong must echo the ping payload if present
                    if (ping_payload_len > 0 && ping_payload_len <= 125) {
                        // Pong with payload
                        unsigned char pong_frame[128];
                        pong_frame[0] = 0x8A; // FIN=1, opcode=0xA (pong)
                        pong_frame[1] = ping_payload_len; // No mask (client to server)
                        memcpy(pong_frame + 2, ping_payload, ping_payload_len);
                        SSL_write(ssl, pong_frame, 2 + ping_payload_len);
                    } else {
                        // Pong without payload
                        unsigned char pong_frame[2] = {0x8A, 0x00}; // Pong frame (opcode 0xA, FIN=1, no payload)
                        SSL_write(ssl, pong_frame, 2);
                    }
                    
                    enable_logging = true;
                    log_fast("Responded to server ping with pong");
                    enable_logging = false;
                }
                
                // Incomplete frame or non-data frame
                if (__builtin_expect(frame_size == 0, 0)) {
                    // Need more data - break to read more
                    break;
                } else {
                    // Skip this frame (ping, close, etc.)
                    offset += frame_size;
                    continue;
                }
            }
        }
        
        // HFT OPTIMIZATION: Efficient buffer management
        if (__builtin_expect(offset > 0, 1)) {
            if (__builtin_expect(offset < buffer_pos, 1)) {
                // Move remaining data to start (only if needed)
                memmove(buffer, buffer + offset, buffer_pos - offset);
                buffer_pos -= offset;
            } else {
                // Buffer fully consumed
                buffer_pos = 0;
            }
        }
        
        // Check if it's time to print table (every 5 seconds) - also check here for cases with no new data
        uint64_t now = get_timestamp_ns();
        if (now - last_print_time >= PRINT_INTERVAL_NS) {
            update_count++;
            print_table(g_symbol_data, update_count);
            last_print_time = now;
        }
    }
    
    // Cleanup on exit
    enable_logging = true;
    log_fast("\nShutting down gracefully...");
    
    // Cleanup shared memory
    if (g_shm_manager) {
        g_shm_manager->cleanup();
        delete g_shm_manager;
        g_shm_manager = nullptr;
    }
    
    SSL_shutdown(ssl);
    SSL_free(ssl);
    close(sock);
    SSL_CTX_free(ssl_ctx);
    
    log_fast("Cleanup complete. Returning to shell.");
    
    return 0;
}