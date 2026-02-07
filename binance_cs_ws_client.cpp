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
#include <errno.h>
#include <chrono>
#include <iomanip>
#include <ios>
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
#include <sys/wait.h>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <pthread.h>
#include "binance_cs_orderbook_shared_memory.h"

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

// HFT OPTIMIZATION: Ultra-fast double parsing
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

// Global running flag for signal handling
static std::atomic<bool> g_running(true);

void signal_handler(int signal) {
    (void)signal; // Suppress unused parameter warning
    g_running = false;
}

// Structure to store symbol data (only prices for TOB)
struct SymbolData {
    double ask_price;
    double bid_price;
};

// Global symbol data map
static std::map<uint32_t, SymbolData> g_symbol_data;
static std::unordered_map<uint32_t, std::string> g_instrument_to_symbol;

// Global variables for multi-core support
static int g_core_id = 0;
static int g_host_id = 0;
static std::atomic<uint64_t> g_message_count(0);
static bool enable_logging = true;

// Format timestamp as HH:MM:SS.mmm
inline std::string format_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::tm bt = *std::localtime(&time_t);
    
    char timestamp_buf[32];
    snprintf(timestamp_buf, sizeof(timestamp_buf), "%02d:%02d:%02d.%03lld", 
             bt.tm_hour, bt.tm_min, bt.tm_sec, (long long)ms.count());
    return std::string(timestamp_buf);
}

// Fast logging function
inline void log_fast(const char* msg) {
    if (!enable_logging) return;
    std::string timestamp = format_timestamp();
    std::cout << "[" << timestamp << "] " << msg << std::endl;
}

// Shared memory manager
static BinanceCSOrderbookSharedMemoryManager* g_shm_manager = nullptr;

// USDTTRY rate cache (average of ask and bid from Binance Global)
static std::atomic<double> g_usdttry_rate(1.0);  // Default to 1.0 if not fetched yet
static std::atomic<int64_t> g_usdttry_last_update(0);  // Timestamp of last update

// Function to fetch USDTTRY from Binance Global REST API
// API: GET https://api.binance.com/api/v3/ticker/bookTicker?symbol=USDTTRY
// Uses curl for simplicity and reliability (handles HTTPS automatically)
bool fetch_usdttry_from_binance_global(double& usdttry_rate) {
    // Use curl to fetch the JSON response (handles HTTPS automatically)
    FILE* curl_pipe = popen("curl -s --max-time 5 'https://api.binance.com/api/v3/ticker/bookTicker?symbol=USDTTRY'", "r");
    if (!curl_pipe) {
        return false;
    }
    
    // Read response
    char response[1024];
    size_t total_read = 0;
    while (total_read < sizeof(response) - 1) {
        size_t n = fread(response + total_read, 1, sizeof(response) - total_read - 1, curl_pipe);
        if (n == 0) {
            break;
        }
        total_read += n;
    }
    response[total_read] = '\0';
    
    int curl_status = pclose(curl_pipe);
    if (curl_status != 0 || total_read == 0) {
        return false;
    }
    
    // Parse JSON response: {"symbol":"USDTTRY","bidPrice":"34.50000000","bidQty":"1000.00000000","askPrice":"34.51000000","askQty":"1000.00000000"}
    // Find bidPrice and askPrice
    const char* bid_price_start = strstr(response, "\"bidPrice\"");
    const char* ask_price_start = strstr(response, "\"askPrice\"");
    
    if (!bid_price_start || !ask_price_start) {
        return false;
    }
    
    // Extract bidPrice value
    bid_price_start = strchr(bid_price_start, ':');
    if (!bid_price_start) return false;
    bid_price_start++;  // Skip ':'
    while (*bid_price_start == ' ' || *bid_price_start == '"') bid_price_start++;
    
    const char* bid_price_end = bid_price_start;
    while (*bid_price_end && *bid_price_end != '"' && *bid_price_end != ',' && *bid_price_end != '}') {
        bid_price_end++;
    }
    
    // Extract askPrice value
    ask_price_start = strchr(ask_price_start, ':');
    if (!ask_price_start) return false;
    ask_price_start++;  // Skip ':'
    while (*ask_price_start == ' ' || *ask_price_start == '"') ask_price_start++;
    
    const char* ask_price_end = ask_price_start;
    while (*ask_price_end && *ask_price_end != '"' && *ask_price_end != ',' && *ask_price_end != '}') {
        ask_price_end++;
    }
    
    // Parse doubles
    std::string bid_str(bid_price_start, bid_price_end - bid_price_start);
    std::string ask_str(ask_price_start, ask_price_end - ask_price_start);
    
    try {
        double bid_price = std::stod(bid_str);
        double ask_price = std::stod(ask_str);
        
        // Calculate average
        usdttry_rate = (bid_price + ask_price) / 2.0;
        
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

// Thread function to periodically update USDTTRY rate (every 60 seconds)
void* usdttry_update_thread(void* arg) {
    (void)arg;  // Suppress unused parameter warning
    
    // Do immediate first fetch (don't wait - we need the rate ASAP)
        double usdttry_rate = 1.0;
        if (fetch_usdttry_from_binance_global(usdttry_rate)) {
            g_usdttry_rate.store(usdttry_rate);
            int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            g_usdttry_last_update.store(now);
            
            char log_msg[256];
            snprintf(log_msg, sizeof(log_msg), "[Host-%d Core-%d] [USDTTRY] Initial rate fetched: %.6f (avg of bid/ask) - all prices will be converted to TRY", 
                    g_host_id + 1, g_core_id, usdttry_rate);
            log_fast(log_msg);
        } else {
            char warn_msg[256];
            snprintf(warn_msg, sizeof(warn_msg), "[Host-%d Core-%d] [USDTTRY] WARNING: Initial fetch failed, using default rate 1.0. Prices will NOT be converted to TRY until fetch succeeds!", 
                    g_host_id + 1, g_core_id);
            log_fast(warn_msg);
        }
    
    while (g_running) {
        // Sleep for 60 seconds before next fetch
        for (int i = 0; i < 60 && g_running; i++) {
            sleep(1);
        }
        
        if (!g_running) break;
        
        double usdttry_rate = 1.0;
        if (fetch_usdttry_from_binance_global(usdttry_rate)) {
            g_usdttry_rate.store(usdttry_rate);
            int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            g_usdttry_last_update.store(now);
            
            char log_msg[256];
            snprintf(log_msg, sizeof(log_msg), "[Host-%d Core-%d] [USDTTRY] Rate updated: %.6f (avg of bid/ask)", 
                    g_host_id + 1, g_core_id, usdttry_rate);
            log_fast(log_msg);
        } else {
            char warn_msg[256];
            snprintf(warn_msg, sizeof(warn_msg), "[Host-%d Core-%d] [USDTTRY] WARNING: Failed to fetch, using cached rate: %.6f", 
                    g_host_id + 1, g_core_id, g_usdttry_rate.load());
            log_fast(warn_msg);
        }
    }
    
    return nullptr;
}

// Load instruments and symbol mapping from common_symbol_info.json
bool load_instruments_from_json(const std::string& filename,
                            std::vector<uint32_t>& instruments,
                                std::unordered_map<uint32_t, std::string>& instrument_to_symbol) {
        std::ifstream file(filename);
        if (!file.is_open()) {
        std::cerr << "ERROR: Could not open " << filename << std::endl;
            return false;
        }
        
        std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        file.close();
        
    // Parse common_symbol_info.json format:
    // {"symbols": [{"CS_instrument_id": 8614, "binance_futures_symbol": "0GUSDT", ...}, ...]}
    
    // Find "symbols" array
    size_t pos = content.find("\"symbols\"");
    if (pos == std::string::npos) {
        // Fallback: try old format with "instruments" array
        pos = content.find("\"instruments\"");
        if (pos == std::string::npos) return false;
    }
    
    pos = content.find('[', pos);
    if (pos == std::string::npos) return false;
    pos++;
    
    // Parse each symbol object
    while (pos < content.length()) {
        // Skip whitespace
        while (pos < content.length() && (content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r' || content[pos] == '\t')) pos++;
        if (pos >= content.length() || content[pos] == ']') break;
        if (content[pos] != '{') {
            // Skip to next object
            while (pos < content.length() && content[pos] != '{' && content[pos] != ']') pos++;
            if (pos >= content.length() || content[pos] == ']') break;
        }
        pos++; // Skip {
        
        // Find CS_instrument_id
        size_t inst_id_pos = content.find("\"CS_instrument_id\"", pos);
        if (inst_id_pos == std::string::npos || inst_id_pos > content.find('}', pos)) {
            // Skip this object
            while (pos < content.length() && content[pos] != '}' && content[pos] != ']') pos++;
            if (pos < content.length() && content[pos] == '}') pos++;
            while (pos < content.length() && (content[pos] == ',' || content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r')) pos++;
            continue;
        }
        
        inst_id_pos += 18; // Skip "CS_instrument_id"
        while (inst_id_pos < content.length() && (content[inst_id_pos] == ' ' || content[inst_id_pos] == ':')) inst_id_pos++;
        
        // Parse instrument ID
        const char* num_start = content.c_str() + inst_id_pos;
        char* num_end = nullptr;
        uint32_t inst_id = static_cast<uint32_t>(strtoul(num_start, &num_end, 10));
        if (num_end == num_start) {
            // Skip this object
            while (pos < content.length() && content[pos] != '}' && content[pos] != ']') pos++;
            if (pos < content.length() && content[pos] == '}') pos++;
            while (pos < content.length() && (content[pos] == ',' || content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r')) pos++;
            continue;
        }
        
        // Find binance_futures_symbol
        size_t symbol_pos = content.find("\"binance_futures_symbol\"", pos);
        if (symbol_pos == std::string::npos || symbol_pos > content.find('}', pos)) {
            // Skip this object
            while (pos < content.length() && content[pos] != '}' && content[pos] != ']') pos++;
            if (pos < content.length() && content[pos] == '}') pos++;
            while (pos < content.length() && (content[pos] == ',' || content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r')) pos++;
            continue;
        }
        
        symbol_pos += 24; // Skip "binance_futures_symbol"
        while (symbol_pos < content.length() && (content[symbol_pos] == ' ' || content[symbol_pos] == ':')) symbol_pos++;
        if (symbol_pos >= content.length() || content[symbol_pos] != '"') {
            // Skip this object
            while (pos < content.length() && content[pos] != '}' && content[pos] != ']') pos++;
            if (pos < content.length() && content[pos] == '}') pos++;
            while (pos < content.length() && (content[pos] == ',' || content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r')) pos++;
            continue;
        }
        
        symbol_pos++; // Skip opening quote
        const char* symbol_start = content.c_str() + symbol_pos;
        while (symbol_pos < content.length() && content[symbol_pos] != '"') symbol_pos++;
        if (symbol_pos >= content.length()) break;
        
        std::string symbol(symbol_start, content.c_str() + symbol_pos - symbol_start);
        
        // Store instrument and mapping
        instruments.push_back(inst_id);
        instrument_to_symbol[inst_id] = symbol;
        
        // Skip to next object
        while (pos < content.length() && content[pos] != '}' && content[pos] != ']') pos++;
        if (pos < content.length() && content[pos] == '}') pos++;
        while (pos < content.length() && (content[pos] == ',' || content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r')) pos++;
    }
    
    return !instruments.empty();
}

// Log core statistics (first symbol TOB data every 5 seconds)
void log_core_stats(const std::map<uint32_t, SymbolData>& symbol_data,
                    const std::unordered_map<uint32_t, std::string>& instrument_to_symbol,
                    int update_count) {
    enable_logging = true;
    
    uint64_t msg_count = g_message_count.load();
    
    // Determine which half of symbols this core handles for clearer logging
    const char* symbol_range = "";
    if (g_core_id == 10 || g_core_id == 12) {
        symbol_range = "first half (0-144)";
    } else if (g_core_id == 11 || g_core_id == 13) {
        symbol_range = "second half (145-289)";
    }
    
    if (symbol_data.empty()) {
        char log_msg[512];
        snprintf(log_msg, sizeof(log_msg), 
            "[Host-%d Core-%d (%s) | Update #%d] Messages: %llu | No symbols received yet",
            g_host_id + 1, g_core_id, symbol_range, update_count, (unsigned long long)msg_count); // host_id+1 to show Host-1, Host-2
        log_fast(log_msg);
    } else {
        // Get first symbol (lowest instrument ID in the map)
        auto first_it = symbol_data.begin();
        uint32_t inst_id = first_it->first;
        const SymbolData& data = first_it->second;
        std::string symbol = instrument_to_symbol.count(inst_id) ? 
                            instrument_to_symbol.at(inst_id) : std::to_string(inst_id);
        
        char log_msg[512];
        snprintf(log_msg, sizeof(log_msg), 
            "[Host-%d Core-%d (%s) | Update #%d] Messages: %llu | First Symbol: %s (Ask: %.8f, Bid: %.8f) | Total Symbols: %zu",
            g_host_id + 1, g_core_id, symbol_range, update_count, (unsigned long long)msg_count, // host_id+1 to show Host-1, Host-2
            symbol.c_str(), data.ask_price, data.bid_price, symbol_data.size());
        log_fast(log_msg);
    }
    
    enable_logging = false;
}

// Process CS TOB message: [msgType, instrument, prevEventId, eventId, adapterTimestamp, exchangeTimestamp, data]
// Data format: [[side, price, quantity, ordercount], [side, price, quantity, ordercount]]
// side: 0 (BID) or 1 (ASK)
// Full example: [6, 123456, "12345678-1", "12345678-2", 1580143531008103451, 1580143530031129024, [[0, "7098.25", "10", 1], [1, "7099.25", "20", 1]]]
void process_tob_message(const char* data, size_t len) {
    // Find TOB message type (6) - simple check for [6,
    // Also handle continuation frames that might start with the data directly
    const char* tob_marker = (const char*)memmem(data, len, "[6,", 3);
    if (!tob_marker) {
        // Check if this might be a continuation frame or different message format
        // Some messages might be batched: [[6,...],[6,...]]
        if (len > 0 && data[0] == '[' && memmem(data, len, "6,", 2)) {
            // Might be a batched message, try to find [6, in the array
            tob_marker = (const char*)memmem(data, len, "6,", 2);
            if (tob_marker && tob_marker > data && *(tob_marker - 1) == '[') {
                // Found [6, in batched format
            } else {
                return; // Not a TOB message
            }
        } else {
            return; // Not a TOB message
        }
    }
    
    // Parse instrument_id (second element)
    const char* inst_start = tob_marker + 3;
    while (inst_start < data + len && (*inst_start == ' ' || *inst_start == '\n' || *inst_start == '\r')) inst_start++;
    if (inst_start >= data + len) return;
    
    char* inst_end = nullptr;
    uint32_t instrument_id = static_cast<uint32_t>(strtoul(inst_start, &inst_end, 10));
    if (inst_end == inst_start || instrument_id == 0) return;
    
    // Debug logging removed for production
    
    // Find data array (7th element) by counting commas
    // Format: [6, instrument, "prevEventId", "eventId", timestamp, timestamp, [[...]]]
    // Starting from after instrument, we need to skip 5 commas to reach the data array
    const char* pos = inst_end;
    int comma_count = 0;
    
    // Skip to the 5th comma (after exchangeTimestamp, before data array)
    while (pos < data + len && comma_count < 5) {
        // Skip whitespace
        while (pos < data + len && (*pos == ' ' || *pos == '\n' || *pos == '\r' || *pos == '\t')) pos++;
        if (pos >= data + len) break;
        
        if (*pos == ',') {
            comma_count++;
            pos++;
        } else if (*pos == '"') {
            // Skip quoted string completely
            pos++;
            while (pos < data + len && *pos != '"') {
                if (*pos == '\\' && pos + 1 < data + len) pos++; // Skip escaped characters
                pos++;
            }
            if (pos < data + len) pos++; // Skip closing quote
        } else if ((*pos >= '0' && *pos <= '9') || *pos == '-' || *pos == '+') {
            // Skip number (including negative signs and scientific notation)
            while (pos < data + len && 
                   ((*pos >= '0' && *pos <= '9') || *pos == '.' || *pos == '-' || 
                    *pos == '+' || *pos == 'e' || *pos == 'E')) {
                pos++;
            }
        } else {
            pos++;
        }
    }
    
    // Skip whitespace after the comma
    while (pos < data + len && (*pos == ' ' || *pos == '\n' || *pos == '\r' || *pos == '\t')) pos++;
    
    
    // Now we should be at the start of the data array [[
        if (pos >= data + len || *pos != '[') {
        return;
    }
    const char* data_array_start = pos; // Save start of [[
    
    
    // Skip the opening [[ of the data array
    pos++; // Skip first [
    if (pos >= data + len || *pos != '[') {
        return;
    }
    pos++; // Skip second [, now should be at [ of first entry [0,...]
    
    // But actually, we might already be at the start of the entry content (the '0')
    // Let's check: if we're at '[', that's the entry's opening bracket, skip it
    // If we're at a digit, we're already at the entry content
    const char* data_start = pos;
    if (pos < data + len && *pos == '[') {
        // We're at the [ of the first entry, skip it
        pos++;
        data_start = pos;
    }
    // Otherwise, we're already at the entry content (the side digit)
    
    // Find the end of the data array ]]
    // Start from data_array_start (the [[) to properly track bracket depth
    const char* data_end = data_array_start;
    int bracket_depth = 0;
    bool in_string = false;
    while (data_end < data + len) {
        if (in_string) {
            if (*data_end == '\\' && data_end + 1 < data + len) {
                data_end += 2;
                continue;
            }
            if (*data_end == '"') {
                in_string = false;
            }
            data_end++;
        } else {
            if (*data_end == '"') {
                in_string = true;
                data_end++;
            } else if (*data_end == '[') {
                bracket_depth++;
                data_end++;
            } else if (*data_end == ']') {
                bracket_depth--;
                data_end++;
                if (bracket_depth == 0) {
                    // Found closing ]] - data_end now points after the final ]
                    break;
                }
            } else {
                data_end++;
            }
        }
    }
    
    // Parse entries: [side, price, quantity, ordercount]
    // Format: [[0,"0.358","18798",1],[1,"0.3581","58002",1]]
    // side: integer (0=BID, 1=ASK), price: string, quantity: string, ordercount: integer
    // Note: We only store prices, not quantities
    double bid_price = 0.0; // bid_qty not used - only prices stored
    double ask_price = 0.0; // ask_qty not used - only prices stored
    
    // The data array format is: [[entry1],[entry2],...]
    // We've already skipped the opening [[, so data_start points to the first character of the first entry
    // But each entry starts with [, so we need to check if we're already at [ or if we need to look for it
    const char* entry_pos = data_start;
    
    
    // Parse all entries in the data array
    // Format: [[0,"price","qty",1],[1,"price","qty",1]]
    // After skipping [[, we should be at [ of first entry, but we might be at the side digit directly
    int entry_count = 0;
    while (entry_pos < data_end) {
        // Skip whitespace and commas
        while (entry_pos < data_end && (*entry_pos == ' ' || *entry_pos == '\n' || *entry_pos == '\r' || *entry_pos == '\t' || *entry_pos == ',')) entry_pos++;
        if (entry_pos >= data_end) break;
        
        // Check if we've reached the end of the data array ]]
        if (entry_pos + 1 < data_end && *entry_pos == ']' && entry_pos[1] == ']') {
            break;
        }
        
        // Entry might start with [ or directly with the side digit (0 or 1)
        if (*entry_pos == '[') {
            entry_pos++; // Skip [
        } else if (*entry_pos != '0' && *entry_pos != '1') {
            // Not a valid entry start
            break;
        }
        entry_count++;
        
        // Parse side (integer: 0 or 1)
        while (entry_pos < data_end && (*entry_pos == ' ' || *entry_pos == '\n' || *entry_pos == '\r')) entry_pos++;
        if (entry_pos >= data_end) break;
        
        int side = -1;
        if (*entry_pos == '0') {
            side = 0;
            entry_pos++;
        } else if (*entry_pos == '1') {
            side = 1;
            entry_pos++;
        } else {
            // Invalid side, skip to next entry
            while (entry_pos < data_end && *entry_pos != ']') entry_pos++;
            if (entry_pos < data_end) entry_pos++; // Skip ]
            continue;
        }
        
        // Skip comma and whitespace
        while (entry_pos < data_end && (*entry_pos == ',' || *entry_pos == ' ' || *entry_pos == '\n' || *entry_pos == '\r')) entry_pos++;
        if (entry_pos >= data_end) break;
        
        // Parse price (string in quotes)
        if (*entry_pos != '"') {
            // Skip to next entry if no quote
            while (entry_pos < data_end && *entry_pos != ']') entry_pos++;
            if (entry_pos < data_end) entry_pos++; // Skip ]
            continue;
        }
        entry_pos++; // Skip opening quote
        const char* price_start = entry_pos;
        while (entry_pos < data_end && *entry_pos != '"') {
            if (*entry_pos == '\\' && entry_pos + 1 < data_end) entry_pos++; // Skip escaped characters
            entry_pos++;
        }
        if (entry_pos >= data_end) break;
        size_t price_len = entry_pos - price_start;
        double price = fast_atof(price_start, price_len);
        entry_pos++; // Skip closing quote
        
        // Skip comma and whitespace
        while (entry_pos < data_end && (*entry_pos == ',' || *entry_pos == ' ' || *entry_pos == '\n' || *entry_pos == '\r')) entry_pos++;
        if (entry_pos >= data_end) break;
        
        // Parse quantity (string in quotes)
        if (*entry_pos != '"') {
            // Skip to next entry if no quote
            while (entry_pos < data_end && *entry_pos != ']') entry_pos++;
            if (entry_pos < data_end) entry_pos++; // Skip ]
            continue;
        }
        entry_pos++; // Skip opening quote
        // Skip quantity field (not used - only prices stored)
        while (entry_pos < data_end && *entry_pos != '"') {
            if (*entry_pos == '\\' && entry_pos + 1 < data_end) entry_pos++; // Skip escaped characters
            entry_pos++;
        }
        if (entry_pos >= data_end) break;
        entry_pos++; // Skip closing quote
        
        // Skip comma and whitespace before ordercount
        while (entry_pos < data_end && (*entry_pos == ',' || *entry_pos == ' ' || *entry_pos == '\n' || *entry_pos == '\r')) entry_pos++;
        
        // Skip ordercount (integer, always 1, not used)
        while (entry_pos < data_end && *entry_pos >= '0' && *entry_pos <= '9') entry_pos++;
        
        // Skip to end of entry ]
        // Entry format is [side, price, quantity, ordercount], so we need to skip to the closing ]
        while (entry_pos < data_end && (*entry_pos == ' ' || *entry_pos == '\n' || *entry_pos == '\r')) entry_pos++;
        if (entry_pos < data_end && *entry_pos == ']') {
            entry_pos++; // Skip ]
        } else {
            // If no ], we might have reached the end of the data array or there's a parsing error
            // Try to find the next comma or ]
            while (entry_pos < data_end && *entry_pos != ',' && *entry_pos != ']') entry_pos++;
            if (entry_pos < data_end && *entry_pos == ']') entry_pos++;
        }
        
        // Store based on side (find best bid/ask)
        // side=0 means BID, side=1 means ASK
        // Note: We only store prices, not quantities
        if (side == 0) {  // BID - take highest price
            if (bid_price == 0.0 || price > bid_price) {
                bid_price = price;
                // bid_qty = qty; // Not used - only prices stored
            }
        } else if (side == 1) {  // ASK - take lowest price
            if (ask_price == 0.0 || price < ask_price) {
                ask_price = price;
                // ask_qty = qty; // Not used - only prices stored
            }
        }
        
        // Skip comma/whitespace to next entry
        while (entry_pos < data_end && (*entry_pos == ',' || *entry_pos == ' ' || *entry_pos == '\n' || *entry_pos == '\r')) entry_pos++;
    }
    
    // Update symbol data only if we have at least one valid price
    if (bid_price > 0.0 || ask_price > 0.0) {
        g_symbol_data[instrument_id] = {
            ask_price, bid_price
        };
        
        // Increment message counter
        g_message_count++;
        
        // Update shared memory if available (only prices, no quantities)
        // REDUNDANCY: All cores (Host-1 and Host-2) write to the SAME shared memory location
        // for the same symbol. The symbol-to-index mapping ensures that "GRTUSDT" from
        // Host-1 Core-10 and "GRTUSDT" from Host-2 Core-12 both write to entries[same_index].
        // This provides redundancy - if one host fails, the other continues updating the data.
        if (g_shm_manager && bid_price > 0.0 && ask_price > 0.0) {
            std::string symbol = g_instrument_to_symbol.count(instrument_id) ? 
                                 g_instrument_to_symbol[instrument_id] : "";
            if (!symbol.empty()) {
                int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                
                // CRITICAL: Convert ALL Binance CS prices from USDT to TRY
                // This applies to EVERY symbol, regardless of which core processes it (first half, second half, etc.)
                // All prices MUST be converted to TRY for comparison with EXCHANGE prices
                double usdttry_rate = g_usdttry_rate.load();
                
                // Log USDTTRY rate periodically (once per 1000 updates to avoid spam)
                static uint64_t conversion_log_count = 0;
                if (conversion_log_count++ % 1000 == 0) {
                    char rate_log[256];
                    snprintf(rate_log, sizeof(rate_log), "[Host-%d Core-%d] USDTTRY rate: %.6f (converting %s: USDT %.8f -> TRY %.8f)", 
                            g_host_id + 1, g_core_id, usdttry_rate, symbol.c_str(), ask_price, ask_price * usdttry_rate);
                    log_fast(rate_log);
                }
                
                // Multiply both ask and bid prices by USDTTRY rate to convert from USDT to TRY
                // This conversion happens for ALL symbols, no exceptions
                double ask_price_try = ask_price * usdttry_rate;
                double bid_price_try = bid_price * usdttry_rate;
                
                // Write prices in TRY (0.0 for quantities) - safe overwriting for redundancy
                // Multiple cores can safely overwrite the same location - last write wins
                // Note: Prices are now in TRY, matching EXCHANGE prices for arbitrage comparison
                // ALL symbols written to shared memory are in TRY, regardless of core assignment
                g_shm_manager->update_orderbook(instrument_id, symbol, ask_price_try, 0.0,
                                                bid_price_try, 0.0, timestamp);
            }
        }
    }
}

// Parse WebSocket frame
bool parse_ws_frame(unsigned char* buffer, size_t buffer_len, size_t* offset, size_t* frame_size) {
    if (buffer_len < 2) return false;
    
    size_t pos = *offset;
    if (pos + 2 > buffer_len) return false;
    
    bool fin = (buffer[pos] & 0x80) != 0;
    uint8_t opcode = buffer[pos] & 0x0F;
    bool masked = (buffer[pos + 1] & 0x80) != 0;
    uint64_t payload_len = buffer[pos + 1] & 0x7F;
    
    pos += 2;
    
    if (payload_len == 126) {
        if (pos + 2 > buffer_len) return false;
        payload_len = (buffer[pos] << 8) | buffer[pos + 1];
        pos += 2;
    } else if (payload_len == 127) {
        if (pos + 8 > buffer_len) return false;
        payload_len = 0;
        for (int i = 0; i < 8; i++) {
            payload_len = (payload_len << 8) | buffer[pos + i];
        }
        pos += 8;
    }
    
    if (masked) {
        if (pos + 4 > buffer_len) return false;
        pos += 4; // Skip masking key
    }
    
    size_t total_size = pos - *offset + payload_len;
    if (*offset + total_size > buffer_len) return false;
    
    // Handle ping (0x09) - respond with pong
    if (opcode == 0x09) {
        unsigned char pong_frame[14];
        size_t pong_size = (total_size < 14) ? total_size : 14;
        memcpy(pong_frame, buffer + *offset, pong_size);
        pong_frame[0] = (pong_frame[0] & 0xF0) | 0x0A; // Set opcode to pong (0x0A)
        // Note: Pong will be sent in main loop
        *frame_size = total_size;
        return false;
    }
    
    // Handle close (0x08)
    if (opcode == 0x08) {
        return false;
    }
    
    *frame_size = total_size;
    return fin && (opcode == 0x01 || opcode == 0x02); // TEXT or BINARY
}

// Function to get hosts from instruments file or use defaults
std::vector<std::pair<std::string, int>> get_hosts_from_file(const std::string& filename) {
    std::vector<std::pair<std::string, int>> hosts;
    std::ifstream file(filename);
    if (file.is_open()) {
        std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        file.close();
        
        // Look for "hosts" array
        size_t pos = content.find("\"hosts\"");
        if (pos != std::string::npos) {
            pos = content.find('[', pos);
            if (pos != std::string::npos) {
                pos++;
                while (pos < content.length()) {
                    while (pos < content.length() && (content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r' || content[pos] == '\t' || content[pos] == '"')) pos++;
                    if (pos >= content.length() || content[pos] == ']') break;
                    
                    const char* host_start = content.c_str() + pos;
                    while (pos < content.length() && content[pos] != '"' && content[pos] != ',' && content[pos] != ']' && content[pos] != ':') pos++;
                    
                    std::string host_str(host_start, content.c_str() + pos - host_start);
                    
                    // Parse host:port
                    size_t colon_pos = host_str.find(':');
                    std::string host = host_str.substr(0, colon_pos);
                    int port = 10000;
                    if (colon_pos != std::string::npos) {
                        try {
                            port = std::stoi(host_str.substr(colon_pos + 1));
                        } catch (...) {
                            port = 10000;
                        }
                    }
                    
                    hosts.push_back({host, port});
                    
                    while (pos < content.length() && content[pos] != ',' && content[pos] != ']') pos++;
                    if (pos < content.length() && content[pos] == ',') pos++;
                }
            }
        }
    }
    
    // Use defaults if no hosts found
    if (hosts.empty()) {
        hosts.push_back({"63.180.84.140", 10000});
        hosts.push_back({"63.180.141.87", 10000});
    }
    
    return hosts;
}

// Function to run a single core client
int run_core_client(int core_id, int host_id, const std::string& host, int port, 
                    int start_index, int end_index, const std::string& instruments_file);

int main(int argc, char* argv[]) {
    // If no arguments, spawn 4 processes (2 hosts × 2 cores each)
    if (argc == 1) {
        enable_logging = true;
        log_fast("Parent process: Spawning 4 child clients (2 hosts × 2 cores) for redundancy...");
        
        // Generate symbol mapping if needed (similar to binance_tr_ws_client)
        int sys_ret = system("python3 -c \"import json; d=json.load(open('common_symbol_info.json')); m={s.get('binance_futures_symbol'):i for i,s in enumerate(d.get('symbols',[])) if s.get('binance_futures_symbol')}; json.dump({'symbol_to_index':m}, open('binance_cs_symbol_mapping.json','w'), indent=2)\" 2>/dev/null");
        (void)sys_ret; // Ignore return value intentionally
        
        // Get hosts from instruments file or use defaults
        std::vector<std::pair<std::string, int>> hosts = get_hosts_from_file("common_symbol_info.json");
        if (hosts.size() < 2) {
            log_fast("WARNING: Less than 2 hosts found, using defaults");
            hosts.clear();
            hosts.push_back({"63.180.84.140", 10000});
            hosts.push_back({"63.180.141.87", 10000});
        }
        
        // Use first 2 hosts
        std::string host1 = hosts[0].first;
        int port1 = hosts[0].second;
        std::string host2 = hosts.size() > 1 ? hosts[1].first : hosts[0].first;
        int port2 = hosts.size() > 1 ? hosts[1].second : hosts[0].second;
        
        // Count total symbols from common_symbol_info.json
        int total_symbols = 0;
        FILE* fp = popen("python3 -c \"import json; d=json.load(open('common_symbol_info.json')); print(len([s for s in d.get('symbols',[]) if s.get('binance_futures_symbol')]))\" 2>/dev/null", "r");
        if (fp) {
            int scan_ret = fscanf(fp, "%d", &total_symbols);
            (void)scan_ret; // Ignore return value intentionally
            pclose(fp);
        }
        
        if (total_symbols == 0) {
            log_fast("ERROR: Could not count symbols. Make sure common_symbol_info.json exists.");
            return 1;
        }
        
        int symbols_per_core = total_symbols / 2; // 2 cores per host
        int remainder = total_symbols % 2;
        
        std::vector<pid_t> child_pids;
        
        // Spawn 4 child processes
        // Core 10: Host-1, symbols 0 to symbols_per_core-1
        // Core 11: Host-1, symbols symbols_per_core to total_symbols-1
        // Core 12: Host-2, symbols 0 to symbols_per_core-1 (same as Core 10, overwrites)
        // Core 13: Host-2, symbols symbols_per_core to total_symbols-1 (same as Core 11, overwrites)
        
        int core_ids[] = {10, 11, 12, 13}; // Host-1: cores 10-11, Host-2: cores 12-13
        
        for (int i = 0; i < 4; i++) {
            int host_id = (i < 2) ? 0 : 1; // Host-1 (host_id=0) for cores 10-11, Host-2 (host_id=1) for cores 12-13
            int core_id = core_ids[i];
            std::string host = (host_id == 0) ? host1 : host2;
            int port = (host_id == 0) ? port1 : port2;
            
            int start_idx, end_idx;
            if (i % 2 == 0) {
                // Core 0 or 2: first half
                start_idx = 0;
                end_idx = symbols_per_core - 1 + remainder;
            } else {
                // Core 1 or 3: second half
                start_idx = symbols_per_core + remainder;
                end_idx = total_symbols - 1;
            }
            
            pid_t pid = fork();
            if (pid == 0) {
                // Child process - run the core client
                return run_core_client(core_id, host_id, host, port, start_idx, end_idx, "common_symbol_info.json");
            } else if (pid > 0) {
                // Parent process - store child PID
                child_pids.push_back(pid);
                char msg[256];
                snprintf(msg, sizeof(msg), "Spawned Host-%d Core-%d (PID: %d) for symbols %d-%d", 
                        host_id + 1, core_id, pid, start_idx, end_idx); // host_id+1 to show Host-1, Host-2
                log_fast(msg);
            } else {
                char msg[256];
                snprintf(msg, sizeof(msg), "ERROR: Failed to fork process for Host-%d Core-%d", host_id + 1, core_id); // host_id+1 to show Host-1, Host-2
                log_fast(msg);
            }
            usleep(100000); // 100ms delay between spawns
        }
        
        // Parent process: wait for all children
        log_fast("All 4 cores launched. Waiting for children...");
        log_fast("Press Ctrl+C to stop all clients");
        
        // Set up signal handler to kill all children
        signal(SIGINT, [](int /* sig */) {
            enable_logging = true;
            log_fast("\nParent received SIGINT, killing child processes...");
            int sys_ret = system("pkill -P $$ binance_cs_ws_client 2>/dev/null");
            (void)sys_ret; // Ignore return value intentionally
            g_running = false;
            exit(0);
        });
        
        // Wait for all children
        int status;
        for (pid_t pid : child_pids) {
            waitpid(pid, &status, 0);
            char msg[256];
            snprintf(msg, sizeof(msg), "Child %d exited with status %d", pid, status);
            log_fast(msg);
        }
        log_fast("All child processes exited. Parent exiting.");
        return 0;
    }
    
    // Legacy mode: single process with command-line arguments
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <host> <port> [instruments_file] [cpu_core]\n";
        std::cerr << "Example: " << argv[0] << " 63.180.84.140 10000 binance_websocket_instruments.json 1\n";
        std::cerr << "Or run without arguments to spawn 4 cores (2 hosts × 2 cores)\n";
        return 1;
    }
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <host> <port> [instruments_file] [cpu_core]\n";
        std::cerr << "Example: " << argv[0] << " 63.180.84.140 10000 binance_websocket_instruments.json 1\n";
        return 1;
    }
    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    std::string host = argv[1];
    
    // Parse port with error handling
    int port;
    try {
        port = std::stoi(argv[2]);
    } catch (const std::exception& e) {
        std::cerr << "ERROR: Invalid port number: " << argv[2] << std::endl;
        return 1;
    }
    
    std::string instruments_file = (argc >= 4) ? argv[3] : "common_symbol_info.json";
    
    // Parse cpu_core with error handling
    int cpu_core = 1;
    if (argc >= 5) {
        try {
            cpu_core = std::stoi(argv[4]);
        } catch (const std::exception& e) {
            std::cerr << "WARNING: Invalid CPU core number: " << argv[4] << ", using default: 1" << std::endl;
            cpu_core = 1;
        }
    }
    
    // Legacy single-process mode
    return run_core_client(cpu_core, 0, host, port, -1, -1, instruments_file);
}

// Function to run a single core client
int run_core_client(int core_id, int host_id, const std::string& host, int port, 
                    int start_index, int end_index, const std::string& instruments_file) {
    g_core_id = core_id;
    g_host_id = host_id;
    
    // Register signal handler for Ctrl+C
    signal(SIGINT, signal_handler);
    
    // HFT OPTIMIZATION: Pin to specified CPU core
    set_cpu_affinity(core_id);
    
    // HFT OPTIMIZATION: Calibrate RDTSC for fast timestamps
    calibrate_rdtsc();
    
    enable_logging = true;
    char init_msg[256];
    snprintf(init_msg, sizeof(init_msg), "=== ULTRA-LOW LATENCY HFT CLIENT (Host-%d Core-%d) ===", host_id + 1, core_id); // host_id+1 to show Host-1, Host-2
    log_fast(init_msg);
    log_fast("Press Ctrl+C to stop gracefully");
    
    // Start USDTTRY update thread (fetches from Binance Global every 60 seconds)
    pthread_t usdttry_thread;
    if (pthread_create(&usdttry_thread, nullptr, usdttry_update_thread, nullptr) == 0) {
        log_fast("USDTTRY update thread started (fetches from Binance Global every 60 seconds)");
        pthread_detach(usdttry_thread);  // Detach thread so it cleans up automatically
    } else {
        log_fast("WARNING: Failed to start USDTTRY update thread");
    }
    
    // Load instruments
    std::vector<uint32_t> all_instruments;
    if (!load_instruments_from_json(instruments_file, all_instruments, g_instrument_to_symbol)) {
        log_fast("ERROR: Failed to load instruments from instruments file");
        return 1;
    }
    
    // Filter instruments based on start_index and end_index
    std::vector<uint32_t> instruments;
    if (start_index >= 0 && end_index >= 0 && start_index <= end_index) {
        // Load symbol-to-index mapping from binance_cs_symbol_mapping.json (similar to TR client)
        std::unordered_map<std::string, size_t> symbol_to_index;
        std::ifstream mapping_file("binance_cs_symbol_mapping.json");
        if (mapping_file.is_open()) {
            std::string content((std::istreambuf_iterator<char>(mapping_file)), std::istreambuf_iterator<char>());
            mapping_file.close();
            
            // Parse symbol_to_index
            size_t pos = content.find("\"symbol_to_index\"");
            if (pos != std::string::npos) {
                pos = content.find('{', pos);
                if (pos != std::string::npos) {
                    pos++;
                    while (pos < content.length()) {
                        while (pos < content.length() && (content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r' || content[pos] == '\t' || content[pos] == ',')) pos++;
                        if (pos >= content.length() || content[pos] == '}') break;
                        
                        if (content[pos] != '"') break;
                        pos++;
                        const char* key_start = content.c_str() + pos;
                        while (pos < content.length() && content[pos] != '"') pos++;
                        if (pos >= content.length()) break;
                        std::string symbol(key_start, content.c_str() + pos - key_start);
                        pos++;
                        
                        while (pos < content.length() && (content[pos] == ' ' || content[pos] == ':')) pos++;
                        
                        const char* val_start = content.c_str() + pos;
                        char* val_end = nullptr;
                        size_t index = static_cast<size_t>(strtoul(val_start, &val_end, 10));
                        if (val_end == val_start) break;
                        symbol_to_index[symbol] = index;
                        pos = val_end - content.c_str();
                    }
                }
            }
        }
        
        // Filter instruments based on symbol index
        for (uint32_t inst_id : all_instruments) {
            std::string symbol = g_instrument_to_symbol.count(inst_id) ? g_instrument_to_symbol[inst_id] : "";
            if (!symbol.empty() && symbol_to_index.count(symbol)) {
                size_t idx = symbol_to_index[symbol];
                if (idx >= (size_t)start_index && idx <= (size_t)end_index) {
                    instruments.push_back(inst_id);
                }
            }
        }
        
        char log_msg[256];
        snprintf(log_msg, sizeof(log_msg), "Host-%d Core-%d: Filtered %zu instrument(s) for symbols %d-%d", 
                host_id + 1, core_id, instruments.size(), start_index, end_index); // host_id+1 to show Host-1, Host-2
        log_fast(log_msg);
    } else {
        // Load all instruments (backward compatibility)
        instruments = all_instruments;
        char log_msg[256];
        snprintf(log_msg, sizeof(log_msg), "Host-%d Core-%d: Loaded all %zu instrument(s)", 
                host_id + 1, core_id, instruments.size()); // host_id+1 to show Host-1, Host-2
        log_fast(log_msg);
    }
    
    if (instruments.empty()) {
        log_fast("ERROR: No instruments to subscribe to");
        return 1;
    }
    
    // Load symbol-to-index mapping for shared memory
    std::unordered_map<std::string, size_t> symbol_to_index;
    std::ifstream mapping_file("binance_cs_symbol_mapping.json");
    if (mapping_file.is_open()) {
        std::string content((std::istreambuf_iterator<char>(mapping_file)), std::istreambuf_iterator<char>());
        mapping_file.close();
        
        // Simple JSON parsing: look for "symbol_to_index" field
        size_t pos = content.find("\"symbol_to_index\"");
        if (pos != std::string::npos) {
            pos = content.find('{', pos);
            if (pos != std::string::npos) {
                pos++;
                while (pos < content.length()) {
                    while (pos < content.length() && (content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r' || content[pos] == '\t' || content[pos] == ',')) pos++;
                    if (pos >= content.length() || content[pos] == '}') break;
                    
                    if (content[pos] != '"') break;
                    pos++;
                    const char* key_start = content.c_str() + pos;
                    while (pos < content.length() && content[pos] != '"') pos++;
                    if (pos >= content.length()) break;
                    std::string symbol(key_start, content.c_str() + pos - key_start);
                    pos++;
                    
                    while (pos < content.length() && (content[pos] == ' ' || content[pos] == ':')) pos++;
                    
                    const char* val_start = content.c_str() + pos;
                    char* val_end = nullptr;
                    size_t index = static_cast<size_t>(strtoul(val_start, &val_end, 10));
                    if (val_end == val_start) break;
                    symbol_to_index[symbol] = index;
                    pos = val_end - content.c_str();
                }
            }
        }
    }
    
    // REDUNDANCY: All cores (from both Host-1 and Host-2) use the SAME shared memory
    // (/binance_cs_orderbook_shm) and the SAME symbol-to-index mapping. This ensures that
    // when Host-1 Core-10 receives "GRTUSDT" and Host-2 Core-12 receives "GRTUSDT",
    // both write to the same shared memory location (entries[same_index]), providing redundancy.
    if (!symbol_to_index.empty()) {
        g_shm_manager = new BinanceCSOrderbookSharedMemoryManager(SHM_NAME_BINANCE_CS);
        if (g_shm_manager->initialize(symbol_to_index)) {
            char log_msg[256];
            snprintf(log_msg, sizeof(log_msg), "Shared memory initialized with %zu symbol(s) [REDUNDANCY: All hosts write to same locations]", symbol_to_index.size());
            log_fast(log_msg);
        } else {
            log_fast("WARNING: Failed to initialize shared memory, continuing without it");
            delete g_shm_manager;
            g_shm_manager = nullptr;
        }
    } else {
        log_fast("WARNING: No symbol-to-index mapping found, shared memory disabled");
    }
    
    // Helper function to establish WebSocket connection
    auto connect_websocket = [&](int& sock_fd) -> bool {
        // Create socket
        sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (sock_fd < 0) {
            enable_logging = true;
            log_fast("ERROR: Failed to create socket");
            return false;
        }
        
        // Set TCP options for low latency
        int flag = 1;
        setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        int quickack = 1;
        setsockopt(sock_fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
        
        // Connect
        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        
        if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
            struct hostent* he = gethostbyname(host.c_str());
            if (!he) {
                enable_logging = true;
                char log_msg[512];
                snprintf(log_msg, sizeof(log_msg), "[Host-%d Core-%d] ERROR: Failed to resolve host: %s - %s (errno=%d)", 
                        host_id + 1, core_id, host.c_str(), hstrerror(h_errno), h_errno);
                log_fast(log_msg);
                close(sock_fd);
                return false;
            }
            memcpy(&server_addr.sin_addr, he->h_addr_list[0], he->h_length);
        }
        
        enable_logging = true;
        char conn_msg[256];
        snprintf(conn_msg, sizeof(conn_msg), "Connecting to Host-%d (%s:%d)...", host_id + 1, host.c_str(), port);
        log_fast(conn_msg);
        
        if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            enable_logging = true;
            char log_msg[512];
            snprintf(log_msg, sizeof(log_msg), "[Host-%d Core-%d] ERROR: Failed to connect to %s:%d - %s (errno=%d)", 
                    host_id + 1, core_id, host.c_str(), port, strerror(errno), errno);
            log_fast(log_msg);
            close(sock_fd);
            return false;
        }
        
        enable_logging = true;
        char tcp_msg[256];
        snprintf(tcp_msg, sizeof(tcp_msg), "TCP connected to %s:%d", host.c_str(), port);
        log_fast(tcp_msg);
        
        // CS WebSocket uses standard WebSocket protocol (plain, not SSL)
        // Send WebSocket handshake to /api/v6 endpoint
        std::stringstream handshake_ss;
        handshake_ss << "GET /api/v6 HTTP/1.1\r\n"
                     << "Host: " << host << ":" << port << "\r\n"
                     << "Upgrade: websocket\r\n"
                     << "Connection: Upgrade\r\n"
                     << "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==\r\n"
                     << "Sec-WebSocket-Version: 13\r\n"
                     << "\r\n";
        
        std::string handshake = handshake_ss.str();
        ssize_t sent = send(sock_fd, handshake.c_str(), handshake.length(), 0);
        if (sent < 0) {
            enable_logging = true;
            char err_msg[512];
            snprintf(err_msg, sizeof(err_msg), "[Host-%d Core-%d] ERROR: Failed to send WebSocket handshake: %s (errno=%d)", 
                    host_id + 1, core_id, strerror(errno), errno);
            log_fast(err_msg);
            close(sock_fd);
            return false;
        } else if ((size_t)sent < handshake.length()) {
            enable_logging = true;
            char warn_msg[512];
            snprintf(warn_msg, sizeof(warn_msg), "[Host-%d Core-%d] WARNING: Partial handshake send: %zd of %zu bytes", 
                    host_id + 1, core_id, sent, handshake.length());
            log_fast(warn_msg);
        }
        
        // Read HTTP response
        char http_buf[4096];
        int http_len = 0;
        int total_read = 0;
        
        while (total_read < (int)sizeof(http_buf) - 1) {
            int n = recv(sock_fd, http_buf + total_read, sizeof(http_buf) - total_read - 1, 0);
            if (n > 0) {
                total_read += n;
                http_buf[total_read] = '\0';
                if (strstr(http_buf, "\r\n\r\n")) {
                    break;
                }
            } else if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    usleep(1000);
                    continue;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        
        http_len = total_read;
        // Check for successful WebSocket handshake (101 Switching Protocols)
        bool handshake_ok = false;
        if (http_len > 0) {
            bool has_101 = (strstr(http_buf, "101") != nullptr);
            bool has_upgrade = (strstr(http_buf, "upgrade") != nullptr) || 
                               (strstr(http_buf, "Upgrade") != nullptr) ||
                               (strstr(http_buf, "UPGRADE") != nullptr);
            handshake_ok = has_101 && has_upgrade;
        }
        
        if (handshake_ok) {
            enable_logging = true;
            log_fast("✓ WebSocket connected!");
            return true;
        } else {
            enable_logging = true;
            char err_msg[1024];
            snprintf(err_msg, sizeof(err_msg), "[Host-%d Core-%d] ERROR: WebSocket handshake failed", 
                    host_id + 1, core_id);
            log_fast(err_msg);
            if (http_len > 0) {
                char http_log[2048];
                snprintf(http_log, sizeof(http_log), "[Host-%d Core-%d] HTTP Response:\n%.*s", 
                        host_id + 1, core_id, http_len, http_buf);
                log_fast(http_log);
            } else {
                char no_resp_msg[256];
                snprintf(no_resp_msg, sizeof(no_resp_msg), "[Host-%d Core-%d] No HTTP response received", 
                        host_id + 1, core_id);
                log_fast(no_resp_msg);
            }
            close(sock_fd);
            return false;
        }
    };
    
    // Initial connection
    int sock = -1;
    if (!connect_websocket(sock)) {
        return 1;
    }
    
    // Helper function to send masked WebSocket frame (client->server frames must be masked)
    auto send_ws_frame = [&](const std::string& payload) {
        unsigned char frame[1024];
        size_t frame_len = 0;
        frame[frame_len++] = 0x81; // FIN + TEXT
        size_t payload_len = payload.length();
        
        // Generate random masking key
        unsigned char mask_key[4];
        for (int i = 0; i < 4; i++) {
            mask_key[i] = rand() & 0xFF;
        }
        
        // Set payload length with mask bit
        if (payload_len < 126) {
            frame[frame_len++] = 0x80 | payload_len; // Set mask bit (0x80)
        } else if (payload_len < 65536) {
            frame[frame_len++] = 0x80 | 126; // Set mask bit
            frame[frame_len++] = (payload_len >> 8) & 0xFF;
            frame[frame_len++] = payload_len & 0xFF;
        }
        
        // Add masking key
        memcpy(frame + frame_len, mask_key, 4);
        frame_len += 4;
        
        // Mask payload
        for (size_t i = 0; i < payload_len; i++) {
            frame[frame_len++] = payload[i] ^ mask_key[i % 4];
        }
        
        ssize_t sent = send(sock, frame, frame_len, 0);
        if (sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            // Log send errors (but not EAGAIN/EWOULDBLOCK which are normal for non-blocking sockets)
            bool was_logging = enable_logging;
            enable_logging = true;
            char err_msg[512];
            snprintf(err_msg, sizeof(err_msg), "[Host-%d Core-%d] send() error: %s (errno=%d)", 
                    host_id + 1, core_id, strerror(errno), errno);
            log_fast(err_msg);
            enable_logging = was_logging;
        } else if (sent >= 0 && (size_t)sent < frame_len) {
            // Partial send - log warning
            bool was_logging = enable_logging;
            enable_logging = true;
            char warn_msg[512];
            snprintf(warn_msg, sizeof(warn_msg), "[Host-%d Core-%d] send() partial: sent %zd of %zu bytes", 
                    host_id + 1, core_id, sent, frame_len);
            log_fast(warn_msg);
            enable_logging = was_logging;
        }
    };
    
    // Read CS config from instruments file (once, reuse for reconnections)
    std::string cs_org = "Quantix";
    std::string cs_app_name = "WSClient";
    std::string cs_app_ver = "1.0.0";
    
    std::ifstream inst_file(instruments_file);
    if (inst_file.is_open()) {
        std::string content((std::istreambuf_iterator<char>(inst_file)), std::istreambuf_iterator<char>());
        inst_file.close();
        
        // Simple JSON parsing for CS config
        size_t pos = content.find("\"cs_org\"");
        if (pos != std::string::npos) {
            pos = content.find(':', pos);
            if (pos != std::string::npos) {
                pos++;
                while (pos < content.length() && (content[pos] == ' ' || content[pos] == '"')) pos++;
                size_t start = pos;
                while (pos < content.length() && content[pos] != '"' && content[pos] != ',') pos++;
                if (pos > start) cs_org = content.substr(start, pos - start);
            }
        }
        
        pos = content.find("\"cs_app_name\"");
        if (pos != std::string::npos) {
            pos = content.find(':', pos);
            if (pos != std::string::npos) {
                pos++;
                while (pos < content.length() && (content[pos] == ' ' || content[pos] == '"')) pos++;
                size_t start = pos;
                while (pos < content.length() && content[pos] != '"' && content[pos] != ',') pos++;
                if (pos > start) cs_app_name = content.substr(start, pos - start);
            }
        }
        
        pos = content.find("\"cs_app_ver\"");
        if (pos != std::string::npos) {
            pos = content.find(':', pos);
            if (pos != std::string::npos) {
                pos++;
                while (pos < content.length() && (content[pos] == ' ' || content[pos] == '"')) pos++;
                size_t start = pos;
                while (pos < content.length() && content[pos] != '"' && content[pos] != ',') pos++;
                if (pos > start) cs_app_ver = content.substr(start, pos - start);
            }
        }
    }
    
    std::string process_id = "cpp_client_" + std::to_string(getpid());
    std::string sub_options = "{\"depthTopic\":false,\"tradesTopic\":false,\"topOfBookTopic\":true,\"indexPriceTopic\":false,\"markPriceTopic\":false,\"fundingRateTopic\":false,\"liquidationsTopic\":false,\"topOfBookCoalescing\":false}";
    
    // Helper function to perform login and subscription
    auto login_and_subscribe = [&](int /* sock_fd */, auto& send_ws_frame_func) -> bool {
        // CS protocol: First send login message [13, org, app_name, app_ver, process_id]
        std::stringstream login_msg;
        login_msg << "[13,\"" << cs_org << "\",\"" << cs_app_name << "\",\"" << cs_app_ver << "\",\"" << process_id << "\"]";
        
        std::string login_str = login_msg.str();
        enable_logging = true;
        char login_log[256];
        snprintf(login_log, sizeof(login_log), "Sending login: %s", login_str.c_str());
        log_fast(login_log);
        send_ws_frame_func(login_str);
        usleep(200000); // Wait 200ms after login (like Python code)
        
        // CS protocol: Send individual subscription for each instrument
        enable_logging = true;
        char sub_log[256];
        snprintf(sub_log, sizeof(sub_log), "Subscribing to %zu instrument(s)...", instruments.size());
        log_fast(sub_log);
        
        // Add random initial delay (1-3 seconds) to stagger subscriptions across cores
        srand(time(NULL) ^ getpid() ^ core_id);
        int random_initial_delay_us = 1000000 + (rand() % 2000001); // 1-3 seconds
        usleep(random_initial_delay_us);
        
        for (size_t i = 0; i < instruments.size(); i++) {
            std::stringstream sub_msg;
            sub_msg << "[11," << instruments[i] << "," << sub_options << "]";
            std::string sub_str = sub_msg.str();
            send_ws_frame_func(sub_str);
            usleep(10000); // 10ms delay between subscriptions (matches Python code: 0.01s)
        }
        
        // Wait a bit after all subscriptions to let server process them
        usleep(500000); // 500ms wait after subscriptions
        return true;
    };
    
    // Main loop buffer
    alignas(64) unsigned char buffer[65536];
    size_t buffer_pos = 0;
    
    // Set socket to non-blocking for reading during subscriptions
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
    
    // Helper function to send WebSocket pong frame (declare before use)
    auto send_ws_pong = [&](const unsigned char* ping_payload, size_t payload_len) {
        unsigned char pong_frame[128];
        size_t frame_len = 0;
        pong_frame[frame_len++] = 0x8A; // FIN + PONG (0x0A)
        
        if (payload_len < 126) {
            pong_frame[frame_len++] = payload_len;
        } else if (payload_len < 65536) {
            pong_frame[frame_len++] = 126;
            pong_frame[frame_len++] = (payload_len >> 8) & 0xFF;
            pong_frame[frame_len++] = payload_len & 0xFF;
        }
        
        if (payload_len > 0 && payload_len < 126) {
            memcpy(pong_frame + frame_len, ping_payload, payload_len);
            frame_len += payload_len;
        }
        
        ssize_t sent = send(sock, pong_frame, frame_len, 0);
        if (sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            // Log pong send errors (but not EAGAIN/EWOULDBLOCK which are normal for non-blocking sockets)
            bool was_logging = enable_logging;
            enable_logging = true;
            char err_msg[512];
            snprintf(err_msg, sizeof(err_msg), "[Host-%d Core-%d] send_ws_pong() error: %s (errno=%d)", 
                    g_host_id + 1, g_core_id, strerror(errno), errno);
            log_fast(err_msg);
            enable_logging = was_logging;
        }
    };
    
    // Helper function to read and process messages (used during subscription and main loop)
    auto read_and_process_messages = [&]() -> bool {
        // Always try to read, even if buffer has data (process existing data first, then read more)
        int n = recv(sock, buffer + buffer_pos, sizeof(buffer) - buffer_pos, 0);
        if (n > 0) {
            buffer_pos += n;
        }
        
        // Process complete frames from buffer (whether new data arrived or not)
        // This ensures we process all available frames, not just when new data arrives
        size_t offset = 0;
        bool processed_any = false;
        while (offset < buffer_pos && g_running) {
            size_t frame_size = 0;
            if (parse_ws_frame(buffer, buffer_pos, &offset, &frame_size)) {
                processed_any = true;
                // parse_ws_frame already validated the frame and returned frame_size
                // Now extract payload: server frames are NOT masked
                size_t payload_offset = offset + 2; // Skip FIN+opcode and mask+len byte
                uint64_t payload_len = buffer[offset + 1] & 0x7F;
                
                // Adjust for extended payload length
                if (payload_len == 126) {
                    payload_len = (buffer[offset + 2] << 8) | buffer[offset + 3];
                    payload_offset = offset + 4; // 2 bytes header + 2 bytes extended length
                } else if (payload_len == 127) {
                    payload_len = 0;
                    for (int j = 0; j < 8; j++) {
                        payload_len = (payload_len << 8) | buffer[offset + 2 + j];
                    }
                    payload_offset = offset + 10; // 2 bytes header + 8 bytes extended length
                } else {
                    payload_offset = offset + 2; // 2 bytes header, no extended length
                }
                
                // Server frames are NOT masked, so payload starts immediately after header
                if (offset + frame_size <= buffer_pos) {
                    // Ensure null termination for string processing
                    if (payload_offset + payload_len <= buffer_pos) {
                        buffer[payload_offset + payload_len] = '\0';
                    }
                    
                    uint8_t opcode = buffer[offset] & 0x0F;
                    if (opcode == 0x09) { // Ping
                        send_ws_pong(buffer + payload_offset, payload_len);
                    } else if (opcode == 0x01 || opcode == 0x02) {
                        // Process all message types - TOB messages (type 6) will increment counter
                        // Other types (5=READY, 14=subscription confirm) are ignored but don't cause errors
                        process_tob_message((const char*)buffer + payload_offset, payload_len);
                    } else {
                        // Log unexpected opcodes for debugging (only occasionally to avoid spam)
                        static uint64_t unexpected_opcode_count = 0;
                        unexpected_opcode_count++;
                        if (unexpected_opcode_count % 1000 == 0) {
                            enable_logging = true;
                            char debug_msg[256];
                            snprintf(debug_msg, sizeof(debug_msg), "[Host-%d Core-%d] Unexpected opcode: 0x%02X (count: %llu)", 
                                    g_host_id + 1, g_core_id, opcode, (unsigned long long)unexpected_opcode_count);
                            log_fast(debug_msg);
                            enable_logging = false;
                        }
                    }
                    offset += frame_size; // Use frame_size from parse_ws_frame
                } else {
                    break; // Incomplete frame
                }
            } else {
                // Check for ping/close frames
                if (frame_size > 0 && offset + 2 <= buffer_pos) {
                    uint8_t opcode = buffer[offset] & 0x0F;
                    if (opcode == 0x09) {
                        size_t ping_payload_offset = offset + 2;
                        uint64_t ping_payload_len = buffer[offset + 1] & 0x7F;
                        if (ping_payload_len == 126) {
                            ping_payload_len = (buffer[offset + 2] << 8) | buffer[offset + 3];
                            ping_payload_offset += 2;
                        } else if (ping_payload_len == 127) {
                            ping_payload_len = 0;
                            for (int j = 0; j < 8; j++) {
                                ping_payload_len = (ping_payload_len << 8) | buffer[offset + 2 + j];
                            }
                            ping_payload_offset += 8;
                        }
                        if (ping_payload_offset + ping_payload_len <= buffer_pos) {
                            send_ws_pong(buffer + ping_payload_offset, ping_payload_len);
                        }
                    } else if (opcode == 0x08) {
                        return false; // Close frame
                    }
                }
                if (frame_size == 0) break;
                offset += frame_size;
            }
        }
        if (offset > 0 && offset < buffer_pos) {
            memmove(buffer, buffer + offset, buffer_pos - offset);
            buffer_pos -= offset;
        } else if (offset >= buffer_pos) {
            buffer_pos = 0;
        }
        
        // Handle recv() return value
        if (n > 0) {
            // Return true if we processed frames OR if we read new data (even if no frames processed yet)
            return true;
        } else if (n < 0) {
            // Handle recoverable errors
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Return true if we processed any frames, false otherwise
                return processed_any;
            } else if (errno == EINTR) {
                return processed_any; // Interrupted system call, but may have processed frames
            } else {
                // Actual error - ALWAYS log it (force enable logging for errors)
                bool was_logging = enable_logging;
                enable_logging = true;
                char err_msg[512];
                snprintf(err_msg, sizeof(err_msg), "[Host-%d Core-%d] recv error: %s (errno=%d)", 
                        g_host_id + 1, g_core_id, strerror(errno), errno);
                log_fast(err_msg);
                enable_logging = was_logging; // Restore previous state
                return false; // Error
            }
        } else if (n == 0) {
            // Connection closed - return false (will be logged once in main loop)
            return false; // Connection closed
        } else {
            return false; // Should not reach here
        }
    };
    
    // Perform initial login and subscription
    if (!login_and_subscribe(sock, send_ws_frame)) {
        close(sock);
        return 1;
    }
    
    enable_logging = true;
    log_fast("=== STREAMING MARKET DATA (HFT OPTIMIZED) ===");
    
    // HFT OPTIMIZATION: Disable logging in hot path for maximum performance
    enable_logging = false;
    
    // Ensure g_running is true before entering main loop
    g_running = true;
    
    // Main loop with reconnection support
    int update_count = 0;
    uint64_t last_log_time = get_timestamp_ns();
    uint64_t current_time = 0;
    const uint64_t LOG_INTERVAL_NS = 5000000000ULL; // 5 seconds
    
    // Reconnection state
    int reconnect_delay_ms = 2000; // Start with 2 seconds
    const int MAX_RECONNECT_DELAY_MS = 30000; // Max 30 seconds
    int reconnect_attempts = 0;
    
    // Socket is already set to non-blocking from initialization phase
    
    while (g_running) {
        // Use the helper function to read and process messages
        bool read_success = read_and_process_messages();
        
        if (!read_success) {
            // Check if it's a real error or just no data
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                // Not an error, just no data - continue
            } else {
                // Real error or connection closed - attempt reconnection
                enable_logging = true;
                if (errno != 0) {
                    char err_msg[256];
                    snprintf(err_msg, sizeof(err_msg), "[Host-%d Core-%d] Connection error: %s (errno=%d). Attempting to reconnect...", 
                            g_host_id + 1, g_core_id, strerror(errno), errno);
                    log_fast(err_msg);
                } else {
                    char close_msg[256];
                    snprintf(close_msg, sizeof(close_msg), "[Host-%d Core-%d] Connection closed by server. Attempting to reconnect...", 
                            g_host_id + 1, g_core_id);
                    log_fast(close_msg);
                }
                
                // Close current socket
                if (sock >= 0) {
                    close(sock);
                    sock = -1;
                }
                
                // Wait before reconnecting (exponential backoff)
                for (int i = 0; i < reconnect_delay_ms && g_running; i += 100) {
                    usleep(100000); // 100ms increments
                }
                
                if (!g_running) {
                    break;
                }
                
                // Attempt reconnection
                reconnect_attempts++;
                enable_logging = true;
                char reconnect_msg[256];
                snprintf(reconnect_msg, sizeof(reconnect_msg), "[Host-%d Core-%d] Reconnection attempt #%d (delay: %dms)...", 
                        host_id + 1, core_id, reconnect_attempts, reconnect_delay_ms);
                log_fast(reconnect_msg);
                
                int new_sock = -1;
                if (connect_websocket(new_sock)) {
                    // Reconnection successful - reset delay and update socket
                    sock = new_sock;
                    reconnect_delay_ms = 2000; // Reset to initial delay
                    reconnect_attempts = 0;
                    
                    // Set socket to non-blocking
                    int flags = fcntl(sock, F_GETFL, 0);
                    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
                    
                    // Clear buffer for new connection
                    buffer_pos = 0;
                    
                    // Re-login and re-subscribe
                    if (login_and_subscribe(sock, send_ws_frame)) {
                        enable_logging = true;
                        log_fast("=== STREAMING MARKET DATA (HFT OPTIMIZED) ===");
                        enable_logging = false;
                        // Continue main loop
                    } else {
                        enable_logging = true;
                        log_fast("ERROR: Failed to re-login/subscribe after reconnection");
                        close(sock);
                        sock = -1;
                        // Increase delay for next attempt
                        reconnect_delay_ms = std::min(reconnect_delay_ms * 2, MAX_RECONNECT_DELAY_MS);
                    }
                } else {
                    // Reconnection failed - increase delay for next attempt
                    reconnect_delay_ms = std::min(reconnect_delay_ms * 2, MAX_RECONNECT_DELAY_MS);
                    enable_logging = true;
                    char fail_msg[256];
                    snprintf(fail_msg, sizeof(fail_msg), "[Host-%d Core-%d] Reconnection failed. Will retry in %dms...", 
                            host_id + 1, core_id, reconnect_delay_ms);
                    log_fast(fail_msg);
                }
                
                // Continue loop to retry connection
                continue;
            }
        } else {
            // Connection is healthy - reset reconnect delay
            if (reconnect_attempts > 0) {
                reconnect_delay_ms = 2000;
                reconnect_attempts = 0;
            }
        }
        
        // Check if time to log stats (every 5 seconds)
        current_time = get_timestamp_ns();
        if (current_time - last_log_time >= LOG_INTERVAL_NS) {
            update_count++;
            enable_logging = true;
            log_core_stats(g_symbol_data, g_instrument_to_symbol, update_count);
            enable_logging = false;
            last_log_time = current_time;
        }
        
        // Small delay to prevent busy-waiting
        usleep(1000); // 1ms sleep
    }
    
    enable_logging = true;
    log_fast("\nShutting down gracefully...");
    
    // Cleanup shared memory
    if (g_shm_manager) {
        g_shm_manager->cleanup();
        delete g_shm_manager;
        g_shm_manager = nullptr;
    }
    
    // Close socket
    if (sock >= 0) {
        close(sock);
    }
    
    char stop_msg[256];
    snprintf(stop_msg, sizeof(stop_msg), "Host-%d Core-%d stopped.", host_id + 1, core_id); // host_id+1 to show Host-1, Host-2
    log_fast(stop_msg);
    
    return 0;
}











