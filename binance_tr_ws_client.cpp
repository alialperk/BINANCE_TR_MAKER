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
#include <ctime>
#include <x86intrin.h>  // For RDTSC
#include <sched.h>      // For CPU affinity
#include <sys/syscall.h>
#include <fstream>
#include <vector>
#include <sstream>
#include <map>
#include <set>
#include <csignal>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <sys/wait.h>
#include "binance_tr_orderbook_shared_memory.h"

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

// Format timestamp as HH:MM:SS.mmm
inline std::string format_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::tm* tm_info = std::localtime(&time_t);
    if (!tm_info) {
        return "[ERROR]";
    }
    
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d.%03lld",
                  tm_info->tm_hour, tm_info->tm_min, tm_info->tm_sec,
                  static_cast<long long>(ms.count()));
    return std::string(buffer);
}

inline void log_fast(const char* msg) {
    if (!enable_logging) return;
    std::string timestamp = format_timestamp();
    std::cout << "[" << timestamp << "] " << msg << std::endl;
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

// Simple JSON parser to extract streams from common_symbol_info.json
// Format: {"symbols":[{"binance_tr_symbol":"ETHTRY",...},...]}
// Constructs stream name as: {symbol_lowercase}@depth5@100ms
// Returns both streams and symbols for filtering
std::pair<std::vector<std::string>, std::vector<std::string>> load_instruments_from_json(const std::string& filename) {
    std::vector<std::string> streams;
    std::vector<std::string> symbols;
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::string error_msg = "ERROR: Could not open " + filename;
        log_fast(error_msg.c_str());
        return {streams, symbols};
    }
    
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();
    
    // Simple JSON parsing: look for "binance_tr_symbol" field
    size_t pos = 0;
    while ((pos = content.find("\"binance_tr_symbol\"", pos)) != std::string::npos) {
        pos += 19; // Skip "binance_tr_symbol"
        // Find the colon
        while (pos < content.length() && (content[pos] == ' ' || content[pos] == ':')) pos++;
        if (pos >= content.length() || content[pos] != '"') continue;
        pos++; // Skip opening quote
        size_t symbol_start = pos;
        // Find closing quote
        while (pos < content.length() && content[pos] != '"') pos++;
        if (pos < content.length()) {
            std::string symbol = content.substr(symbol_start, pos - symbol_start);
            symbols.push_back(symbol); // Store original symbol (e.g., "XRP_TRY")
            
            // Convert to lowercase, remove underscores, and append "@depth5@100ms" to create stream name
            // Binance TR uses format like "xrptry@depth5@100ms" (no underscores)
            std::string stream;
            stream.reserve(symbol.length());
            for (char c : symbol) {
                if (c >= 'A' && c <= 'Z') {
                    stream += (c - 'A' + 'a');
                } else if (c != '_') {
                    stream += c;
                }
                // Skip underscores
            }
            stream += "@depth5@100ms";
            streams.push_back(stream);
        }
        pos++;
    }
    
    return {streams, symbols};
}

// Structure to store symbol data for table display (only top bid/ask prices)
struct SymbolData {
    double ask_price;
    double bid_price;
};

// Global symbol data map (shared between process_depth and main)
// Use unordered_map for O(1) lookups instead of O(log n) for map
static std::unordered_map<std::string, SymbolData> g_symbol_data;

// Shared memory manager
static BinanceTROrderbookSharedMemoryManager* g_shm_manager = nullptr;

// Global stream name(s) for fallback symbol extraction
static std::vector<std::string> g_active_streams;

// Global symbol filter: only process symbols in assigned range
static std::unordered_set<std::string> g_assigned_symbols;
static int g_core_id = 0;

// Global message counter
static std::atomic<uint64_t> g_message_count(0);

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
                // Prefer symbol from "s" field (more accurate than stream name)
                // Stream name might be lowercase (e.g., "0g_try") but symbol is uppercase (e.g., "0G_TRY")
                return symbol;
            }
        }
    }
    
    // Fallback: use symbol from stream name if we found it
    if (!symbol_from_stream.empty()) {
        return symbol_from_stream;
    }
    
    // Final fallback: extract from global active streams (for raw stream format)
    if (!g_active_streams.empty()) {
        // Use first stream (for single stream subscriptions)
        std::string stream = g_active_streams[0];
        size_t at_pos = stream.find('@');
        if (at_pos != std::string::npos) {
            std::string fallback_symbol = stream.substr(0, at_pos);
            // Convert to uppercase
            for (char& c : fallback_symbol) {
                if (c >= 'a' && c <= 'z') c = c - 'a' + 'A';
            }
            return fallback_symbol;
        }
    }
    
    return "UNKNOWN";
}

// Process depth5 orderbook - ZERO COPY, ZERO ALLOCATION for HFT
__attribute__((always_inline))
inline void process_depth(const char* data, size_t len, uint64_t /* recv_time */, const std::string& symbol = "") {
    g_message_count++; // Increment message counter
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
    
    // Skip if symbol is UNKNOWN (don't store invalid data)
    if (msg_symbol == "UNKNOWN") {
        return;
    }
    
    // Normalize symbol for comparison (remove underscores to match JSON format)
    // JSON has "XRP_TRY" but messages have "XRPTRY", so we need to normalize
    std::string normalized_symbol = msg_symbol;
    std::string normalized_msg_symbol;
    normalized_msg_symbol.reserve(normalized_symbol.length());
    for (char c : normalized_symbol) {
        if (c != '_') {
            normalized_msg_symbol += c;
        }
    }
    
    // Check if this symbol (or its normalized version) is in assigned set
    bool is_assigned = false;
    if (!g_assigned_symbols.empty()) {
        // First try exact match
        if (g_assigned_symbols.find(msg_symbol) != g_assigned_symbols.end()) {
            is_assigned = true;
        } else {
            // Try normalized match - check if any assigned symbol matches when normalized
            for (const auto& assigned_sym : g_assigned_symbols) {
                std::string normalized_assigned;
                normalized_assigned.reserve(assigned_sym.length());
                for (char c : assigned_sym) {
                    if (c != '_') {
                        normalized_assigned += c;
                    }
                }
                if (normalized_assigned == normalized_msg_symbol) {
                    is_assigned = true;
                    break;
                }
            }
        }
    } else {
        // If no assigned symbols set, process all (backward compatibility)
        is_assigned = true;
    }
    
    if (!is_assigned) {
        return; // This symbol is handled by another core
    }
    
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
    
    // Parse only top bid and top ask prices (skip quantities)
    // Format: "bids":[["price","qty"],...] or "b":[["price","qty"],...]
    double top_bid_price = 0.0;
    double top_ask_price = 0.0;
    bool has_bid = false;
    bool has_ask = false;
    
    // Parse top bid price (first entry in bids array)
    const char* pos = bid_array_start;
    const char* data_end = actual_data + actual_len;
    
    // Skip whitespace
    while (pos < data_end && (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r')) pos++;
    if (pos < data_end && *pos != ']') {
        // Find opening [
        if (*pos != '[') {
            const char* next_bracket = (const char*)memchr(pos, '[', data_end - pos);
            if (next_bracket) pos = next_bracket;
        }
        if (pos < data_end && *pos == '[') {
            pos++; // Skip [
            // Skip whitespace
            while (pos < data_end && (*pos == ' ' || *pos == '\t')) pos++;
            if (pos < data_end && *pos == '"') {
                pos++; // Skip quote
                // Parse price
                const char* price_start = pos;
                const char* price_end = (const char*)memchr(price_start, '"', data_end - price_start);
                if (price_end) {
                    top_bid_price = fast_atof(price_start, price_end - price_start);
                    has_bid = true;
                }
            }
        }
    }
    
    // Parse top ask price (first entry in asks array)
    pos = ask_array_start;
    data_end = actual_data + actual_len; // Reset for asks
    
    // Skip whitespace
    while (pos < data_end && (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r')) pos++;
    if (pos < data_end && *pos != ']') {
        // Find opening [
        if (*pos != '[') {
            const char* next_bracket = (const char*)memchr(pos, '[', data_end - pos);
            if (next_bracket) pos = next_bracket;
        }
        if (pos < data_end && *pos == '[') {
            pos++; // Skip [
            // Skip whitespace
            while (pos < data_end && (*pos == ' ' || *pos == '\t')) pos++;
            if (pos < data_end && *pos == '"') {
                pos++; // Skip quote
                // Parse price
                const char* price_start = pos;
                const char* price_end = (const char*)memchr(price_start, '"', data_end - price_start);
                if (price_end) {
                    top_ask_price = fast_atof(price_start, price_end - price_start);
                    has_ask = true;
                }
            }
        }
    }
    
    // Only process if we have at least one price
    if (!has_bid && !has_ask) {
        return; // Parsing failed, skip
    }
    
    // Update data for this symbol in global map (only prices, no quantities or spread)
    g_symbol_data[msg_symbol] = {
        top_ask_price,
        top_bid_price
    };
    
    // Update shared memory if available (pass 0.0 for quantities since we don't need them)
    if (g_shm_manager && has_bid && has_ask) {
        int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        g_shm_manager->update_orderbook(msg_symbol, top_ask_price, 0.0,
                                       top_bid_price, 0.0, timestamp);
    }
    
    // Don't print here - will be printed every 5 seconds in main loop
}

// Function to log message count and first symbol TOB data (called every 5 seconds)
void log_core_stats(const std::unordered_map<std::string, SymbolData>& symbol_data, int update_count) {
    enable_logging = true;
    
    uint64_t msg_count = g_message_count.load();
    
    if (symbol_data.empty()) {
        char log_msg[256];
        snprintf(log_msg, sizeof(log_msg), 
            "[Core %d | Update #%d] Messages: %llu | No symbols received yet",
            g_core_id, update_count, (unsigned long long)msg_count);
        log_fast(log_msg);
    } else {
        // Get first symbol
        auto first_it = symbol_data.begin();
        char log_msg[512];
        snprintf(log_msg, sizeof(log_msg), 
            "[Core %d | Update #%d] Messages: %llu | First Symbol: %s (Ask: %.2f, Bid: %.2f) | Total Symbols: %zu",
            g_core_id, update_count, (unsigned long long)msg_count,
            first_it->first.c_str(),
            first_it->second.ask_price,
            first_it->second.bid_price,
            symbol_data.size());
        log_fast(log_msg);
    }
    
    enable_logging = false;
}

// Function to run a single core client
int run_core_client(int core_id, int start_index, int end_index);

int main(int argc, char* argv[]) {
    // If no arguments, spawn 10 processes (one per core)
    if (argc == 1) {
        // Generate symbol mapping if needed
        int sys_ret = system("python3 -c \"import json; d=json.load(open('common_symbol_info.json')); m={s.get('binance_tr_symbol'):i for i,s in enumerate(d.get('symbols',[])) if s.get('binance_tr_symbol')}; json.dump({'symbol_to_index':m}, open('binance_tr_symbol_mapping.json','w'), indent=2)\" 2>/dev/null");
        (void)sys_ret; // Ignore return value intentionally
        
        // Count total symbols
        int total_symbols = 0;
        FILE* fp = popen("python3 -c \"import json; d=json.load(open('common_symbol_info.json')); print(len([s for s in d.get('symbols',[]) if s.get('binance_tr_symbol')]))\" 2>/dev/null", "r");
        if (fp) {
            int scan_ret = fscanf(fp, "%d", &total_symbols);
            (void)scan_ret; // Ignore return value intentionally
            pclose(fp);
        }
        
        if (total_symbols == 0) {
            std::cerr << "ERROR: Could not count symbols. Make sure common_symbol_info.json exists." << std::endl;
            return 1;
        }
        
        int symbols_per_core = total_symbols / 10;
        int remainder = total_symbols % 10;
        
        std::vector<pid_t> child_pids;
        
        // Spawn 10 child processes
        for (int core_id = 0; core_id < 10; core_id++) {
            int start_idx = core_id * symbols_per_core;
            if (core_id < remainder) {
                start_idx += core_id;
            } else {
                start_idx += remainder;
            }
            int end_idx = start_idx + symbols_per_core - 1;
            if (core_id < remainder) {
                end_idx++;
            }
            
            pid_t pid = fork();
            if (pid == 0) {
                // Child process - run the core client
                return run_core_client(core_id, start_idx, end_idx);
            } else if (pid > 0) {
                // Parent process - store child PID
                child_pids.push_back(pid);
                std::cout << "[MAIN] Spawned Core " << core_id << " (PID: " << pid << ", symbols " << start_idx << "-" << end_idx << ")" << std::endl;
            } else {
                std::cerr << "ERROR: Failed to fork process for Core " << core_id << std::endl;
            }
            usleep(100000); // 100ms delay between spawns
        }
        
        // Parent process: wait for all children
        std::cout << "[MAIN] All 10 cores launched. Waiting for children..." << std::endl;
        std::cout << "[MAIN] Press Ctrl+C to stop all clients" << std::endl;
        
        // Set up signal handler to kill all children
        signal(SIGINT, [](int /* sig */) {
            std::cout << "\n[MAIN] Received SIGINT, killing all child processes..." << std::endl;
            int sys_ret = system("pkill -P $$ binance_tr_ws_client 2>/dev/null");
            (void)sys_ret; // Ignore return value intentionally
            exit(0);
        });
        
        // Wait for all children
        int status;
        for (pid_t pid : child_pids) {
            waitpid(pid, &status, 0);
        }
        
        return 0;
    }
    
    // Parse command-line arguments: core_id [start_index] [end_index]
    int core_id = 0;
    int start_index = -1;
    int end_index = -1;
    
    if (argc >= 2) {
        core_id = atoi(argv[1]);
        if (core_id < 0 || core_id > 9) {
            std::cerr << "ERROR: core_id must be between 0 and 9" << std::endl;
            return 1;
        }
    }
    if (argc >= 3) {
        start_index = atoi(argv[2]);
    }
    if (argc >= 4) {
        end_index = atoi(argv[3]);
    }
    
    return run_core_client(core_id, start_index, end_index);
}

// Function to run a single core client
int run_core_client(int core_id, int start_index, int end_index) {
    g_core_id = core_id;
    
    // Register signal handler for Ctrl+C
    signal(SIGINT, signal_handler);
    
    // HFT OPTIMIZATION: Pin to specified CPU core
    set_cpu_affinity(core_id);
    
    // HFT OPTIMIZATION: Calibrate RDTSC for fast timestamps
    calibrate_rdtsc();
    
    enable_logging = true;
    char init_msg[256];
    snprintf(init_msg, sizeof(init_msg), "=== ULTRA-LOW LATENCY HFT CLIENT (CORE %d) ===", core_id);
    log_fast(init_msg);
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
        g_shm_manager = new BinanceTROrderbookSharedMemoryManager(SHM_NAME_BINANCE_TR);
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
    auto [all_streams, all_symbols] = load_instruments_from_json("common_symbol_info.json");
    std::vector<std::string> streams;
    std::vector<std::string> assigned_symbols_list;
    
    if (all_streams.empty()) {
        log_fast("ERROR: No streams found in common_symbol_info.json");
        log_fast("Falling back to default: btctry@depth5@100ms");
        streams.push_back("btctry@depth5@100ms");
        assigned_symbols_list.push_back("BTC_TRY");
    } else {
        // Filter symbols based on start_index and end_index
        if (start_index >= 0 && end_index >= 0 && start_index <= end_index) {
            // Load only assigned range
            for (int i = start_index; i <= end_index && i < (int)all_symbols.size(); i++) {
                streams.push_back(all_streams[i]);
                assigned_symbols_list.push_back(all_symbols[i]);
                g_assigned_symbols.insert(all_symbols[i]);
            }
            char log_msg[512];
            snprintf(log_msg, sizeof(log_msg), 
                     "Core %d: Loaded %zu stream(s) from common_symbol_info.json (indices %d-%d)",
                     core_id, streams.size(), start_index, end_index);
            log_fast(log_msg);
        } else {
            // Load all symbols (backward compatibility)
            streams = all_streams;
            assigned_symbols_list = all_symbols;
            for (const auto& sym : all_symbols) {
                g_assigned_symbols.insert(sym);
            }
            char log_msg[512];
            snprintf(log_msg, sizeof(log_msg), "Loaded %zu stream(s) from common_symbol_info.json", streams.size());
            log_fast(log_msg);
        }
    }
    
    // Store active streams globally for fallback symbol extraction
    g_active_streams = streams;
    
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
                
                // Check if it's time to log core stats (every 5 seconds)
                uint64_t now = get_timestamp_ns();
                if (now - last_print_time >= PRINT_INTERVAL_NS) {
                    update_count++;
                    log_core_stats(g_symbol_data, update_count);
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
        
        // Check if it's time to log core stats (every 5 seconds) - also check here for cases with no new data
        uint64_t now = get_timestamp_ns();
        if (now - last_print_time >= PRINT_INTERVAL_NS) {
            update_count++;
            log_core_stats(g_symbol_data, update_count);
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