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

// Structure to store symbol data for table display
struct SymbolData {
    double ask_price;
    double ask_qty;
    double bid_price;
    double bid_qty;
    double spread;
    double spread_pct;
};

// Global symbol data map
static std::map<uint32_t, SymbolData> g_symbol_data;
static std::unordered_map<uint32_t, std::string> g_instrument_to_symbol;

// Shared memory manager
static BinanceOrderbookSharedMemoryManager* g_shm_manager = nullptr;

// Load instruments and symbol mapping from JSON file
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
        
    // Simple JSON parsing - look for "instruments" array
    size_t pos = content.find("\"instruments\"");
    if (pos == std::string::npos) return false;
    
    pos = content.find('[', pos);
    if (pos == std::string::npos) return false;
    pos++;
    
    // Parse instrument IDs
    while (pos < content.length()) {
        // Skip whitespace
        while (pos < content.length() && (content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r' || content[pos] == '\t')) pos++;
        if (pos >= content.length() || content[pos] == ']') break;
        
        // Parse number
        const char* num_start = content.c_str() + pos;
        char* num_end = nullptr;
        uint32_t inst_id = static_cast<uint32_t>(strtoul(num_start, &num_end, 10));
        if (num_end == num_start) break;
        instruments.push_back(inst_id);
        pos = num_end - content.c_str();
        
        // Skip comma
        while (pos < content.length() && (content[pos] == ',' || content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r')) pos++;
    }
    
    // Parse instrument_to_binance_symbol mapping
    pos = content.find("\"instrument_to_binance_symbol\"");
    if (pos != std::string::npos) {
        pos = content.find('{', pos);
        if (pos != std::string::npos) {
            pos++;
            while (pos < content.length()) {
                // Skip whitespace
                while (pos < content.length() && (content[pos] == ' ' || content[pos] == '\n' || content[pos] == '\r' || content[pos] == '\t' || content[pos] == ',')) pos++;
                if (pos >= content.length() || content[pos] == '}') break;
                
                // Parse key (instrument_id as string)
                if (content[pos] != '"') break;
                pos++;
                const char* key_start = content.c_str() + pos;
                while (pos < content.length() && content[pos] != '"') pos++;
                if (pos >= content.length()) break;
                std::string key_str(key_start, content.c_str() + pos - key_start);
                uint32_t inst_id = static_cast<uint32_t>(strtoul(key_str.c_str(), nullptr, 10));
                pos++; // Skip closing quote
                
                // Skip colon
                while (pos < content.length() && (content[pos] == ' ' || content[pos] == ':')) pos++;
                
                // Parse value (symbol string)
                if (pos >= content.length() || content[pos] != '"') break;
                pos++;
                const char* val_start = content.c_str() + pos;
                while (pos < content.length() && content[pos] != '"') pos++;
                if (pos >= content.length()) break;
                std::string symbol(val_start, content.c_str() + pos - val_start);
                instrument_to_symbol[inst_id] = symbol;
                pos++; // Skip closing quote
            }
        }
    }
    
    return !instruments.empty();
}

// Print table of top-of-book data
void print_table(const std::map<uint32_t, SymbolData>& symbol_data,
                 const std::unordered_map<uint32_t, std::string>& instrument_to_symbol,
                 int update_count) {
    // Use std::cout for safer output (automatically handles null termination)
    std::cout << "\033[2J\033[H"; // Clear screen and move cursor to top
    
    // Print header
    std::cout << "╔════════════════════════════════════════════════════════════════════════════════════════╗\n"
              << "║                    Binance CS Top-of-Book Data (Update #" << update_count << ")                          ║\n"
              << "╠════════════════════════════════════════════════════════════════════════════════════════╣\n"
              << "║ Symbol    │ Best Ask Price │ Ask Qty │ Best Bid Price │ Bid Qty │ Spread    │ Spread % ║\n"
              << "╠════════════════════════════════════════════════════════════════════════════════════════╣\n";
    
    // Print data rows
    for (const auto& pair : symbol_data) {
        uint32_t inst_id = pair.first;
        const SymbolData& data = pair.second;
        std::string symbol = instrument_to_symbol.count(inst_id) ? instrument_to_symbol.at(inst_id) : std::to_string(inst_id);
        
        std::cout << std::fixed << std::setprecision(8)
                  << "║ " << std::setw(9) << std::left << symbol << " │ "
                  << std::setw(14) << std::right << data.ask_price << " │ "
                  << std::setw(8) << std::setprecision(4) << data.ask_qty << " │ "
                  << std::setw(14) << std::setprecision(8) << data.bid_price << " │ "
                  << std::setw(8) << std::setprecision(4) << data.bid_qty << " │ "
                  << std::setw(9) << std::setprecision(8) << data.spread << " │ "
                  << std::setw(7) << std::setprecision(4) << data.spread_pct << "% ║\n";
    }
    
    // Print footer
    std::cout << "╚════════════════════════════════════════════════════════════════════════════════════════╝\n"
              << std::flush;
}

// Process CS TOB message: [msgType, instrument, prevEventId, eventId, adapterTimestamp, exchangeTimestamp, data]
// Data format: [[side, price, quantity, ordercount], [side, price, quantity, ordercount]]
// side: 0 (BID) or 1 (ASK)
// Full example: [6, 123456, "12345678-1", "12345678-2", 1580143531008103451, 1580143530031129024, [[0, "7098.25", "10", 1], [1, "7099.25", "20", 1]]]
void process_tob_message(const char* data, size_t len) {
    // Find TOB message type (6) - simple check for [6,
    const char* tob_marker = (const char*)memmem(data, len, "[6,", 3);
    if (!tob_marker) return;
    
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
    double bid_price = 0.0, bid_qty = 0.0;
    double ask_price = 0.0, ask_qty = 0.0;
    
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
        const char* qty_start = entry_pos;
        while (entry_pos < data_end && *entry_pos != '"') {
            if (*entry_pos == '\\' && entry_pos + 1 < data_end) entry_pos++; // Skip escaped characters
            entry_pos++;
        }
        if (entry_pos >= data_end) break;
        size_t qty_len = entry_pos - qty_start;
        double qty = fast_atof(qty_start, qty_len);
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
        if (side == 0) {  // BID - take highest price
            if (bid_price == 0.0 || price > bid_price) {
                bid_price = price;
                bid_qty = qty;
            }
        } else if (side == 1) {  // ASK - take lowest price
            if (ask_price == 0.0 || price < ask_price) {
                ask_price = price;
                ask_qty = qty;
            }
        }
        
        // Skip comma/whitespace to next entry
        while (entry_pos < data_end && (*entry_pos == ',' || *entry_pos == ' ' || *entry_pos == '\n' || *entry_pos == '\r')) entry_pos++;
    }
    
    // Update symbol data only if we have at least one valid price
    if (bid_price > 0.0 || ask_price > 0.0) {
        double spread = 0.0;
        double spread_pct = 0.0;
        if (bid_price > 0.0 && ask_price > 0.0) {
            spread = ask_price - bid_price;
            spread_pct = (spread / bid_price) * 100.0;
        }
        
        g_symbol_data[instrument_id] = {
            ask_price, ask_qty, bid_price, bid_qty, spread, spread_pct
        };
        
        // Update shared memory if available
        if (g_shm_manager && bid_price > 0.0 && ask_price > 0.0) {
            std::string symbol = g_instrument_to_symbol.count(instrument_id) ? 
                                 g_instrument_to_symbol[instrument_id] : "";
            if (!symbol.empty()) {
                int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                g_shm_manager->update_orderbook_cs(instrument_id, symbol, ask_price, ask_qty,
                                                   bid_price, bid_qty, timestamp);
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

int main(int argc, char* argv[]) {
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
    
    std::string instruments_file = (argc >= 4) ? argv[3] : "binance_websocket_instruments.json";
    
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
    
    // Set CPU affinity
    set_cpu_affinity(cpu_core);
    std::cout << "CPU core set to: " << cpu_core << std::endl;
    
    // Calibrate RDTSC
    calibrate_rdtsc();
    
    // Load instruments
    std::vector<uint32_t> instruments;
    if (!load_instruments_from_json(instruments_file, instruments, g_instrument_to_symbol)) {
        std::cerr << "ERROR: Failed to load instruments from " << instruments_file << std::endl;
        return 1;
    }
    
    std::cout << "Loaded " << instruments.size() << " instrument(s)" << std::endl;
    
    // Load symbol-to-index mapping for shared memory
    // Create mapping from instrument symbols to indices
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
    
    if (!symbol_to_index.empty()) {
        g_shm_manager = new BinanceOrderbookSharedMemoryManager(SHM_NAME_BINANCE_CS);
        if (g_shm_manager->initialize(symbol_to_index)) {
            std::cout << "Shared memory initialized with " << symbol_to_index.size() << " symbol(s)" << std::endl;
        } else {
            std::cerr << "WARNING: Failed to initialize shared memory, continuing without it" << std::endl;
            delete g_shm_manager;
            g_shm_manager = nullptr;
        }
    } else {
        std::cout << "WARNING: No symbol-to-index mapping found, shared memory disabled" << std::endl;
    }
    
    // CS WebSocket uses plain WebSocket (ws://), not SSL
    // Create socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "ERROR: Failed to create socket" << std::endl;
        return 1;
    }
    
    // Set TCP options for low latency
    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    int quickack = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
    
    // Connect
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        struct hostent* he = gethostbyname(host.c_str());
        if (!he) {
            std::cerr << "ERROR: Failed to resolve host: " << host << std::endl;
            close(sock);
            return 1;
        }
        memcpy(&server_addr.sin_addr, he->h_addr_list[0], he->h_length);
    }
    
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "ERROR: Failed to connect to " << host << ":" << port << std::endl;
        close(sock);
        return 1;
    }
    
    std::cout << "TCP connected to " << host << ":" << port << std::endl;
    
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
    send(sock, handshake.c_str(), handshake.length(), 0);
    
    // Read HTTP response
    char http_buf[4096];
    int http_len = 0;
    int total_read = 0;
    
    while (total_read < (int)sizeof(http_buf) - 1) {
        int n = recv(sock, http_buf + total_read, sizeof(http_buf) - total_read - 1, 0);
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
    if (http_len > 0 && strstr(http_buf, "101") && strstr(http_buf, "Upgrade")) {
        std::cout << "✓ WebSocket connected!" << std::endl;
    } else {
        std::cerr << "ERROR: WebSocket handshake failed" << std::endl;
        if (http_len > 0) {
            std::cerr << "HTTP Response:\n" << http_buf << std::endl;
        }
        close(sock);
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
        
        send(sock, frame, frame_len, 0);
    };
    
    // CS protocol: First send login message [13, org, app_name, app_ver, process_id]
    // Read CS config from instruments file
    std::string cs_org = "Quantx";
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
    std::stringstream login_msg;
    login_msg << "[13,\"" << cs_org << "\",\"" << cs_app_name << "\",\"" << cs_app_ver << "\",\"" << process_id << "\"]";
    
    std::string login_str = login_msg.str();
    std::cout << "Sending login: " << login_str << std::endl;
    send_ws_frame(login_str);
    usleep(200000); // Wait 200ms after login (like Python code)
    
    // CS protocol: Send individual subscription for each instrument
    // Format: [11, instrument_id, SUB_OPTIONS]
    // SUB_OPTIONS: {"topOfBookTopic": true, "topOfBookCoalescing": false}
    std::string sub_options = "{\"topOfBookTopic\":true,\"topOfBookCoalescing\":false}";
    
    std::cout << "Subscribing to " << instruments.size() << " instrument(s)..." << std::endl;
    
    // Subscribe to each instrument individually
    for (size_t i = 0; i < instruments.size(); i++) {
        std::stringstream sub_msg;
        sub_msg << "[11," << instruments[i] << "," << sub_options << "]";
        std::string sub_str = sub_msg.str();
        std::cout << "  Subscribing to instrument " << instruments[i] << ": " << sub_str << std::endl;
        send_ws_frame(sub_str);
        usleep(10000); // 10ms delay between subscriptions (like Python code)
    }
    
    std::cout << "=== STREAMING MARKET DATA (HFT OPTIMIZED) ===" << std::endl;
    
    // Main loop
    alignas(64) unsigned char buffer[65536];
    size_t buffer_pos = 0;
    int update_count = 0;
    uint64_t last_print_time = get_timestamp_ns();
    const uint64_t PRINT_INTERVAL_NS = 5000000000ULL; // 5 seconds
    
    // Set socket to non-blocking for better control
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
    
    while (g_running) {
        int n = recv(sock, buffer + buffer_pos, sizeof(buffer) - buffer_pos, 0);
        
        if (n > 0) {
            buffer_pos += n;
            
            // Process complete frames
            size_t offset = 0;
            while (offset < buffer_pos && g_running) {
                size_t frame_size = 0;
                if (parse_ws_frame(buffer, buffer_pos, &offset, &frame_size)) {
                    // Extract payload (server frames are NOT masked)
                    size_t payload_offset = offset + 2;
                    // Server frames don't have mask bit, so no need to skip mask
                    uint64_t payload_len = buffer[offset + 1] & 0x7F;
                    if (payload_len == 126) {
                        payload_len = (buffer[offset + 2] << 8) | buffer[offset + 3];
                        payload_offset += 2;
                    } else if (payload_len == 127) {
                        payload_len = 0;
                        for (int i = 0; i < 8; i++) {
                            payload_len = (payload_len << 8) | buffer[offset + 2 + i];
                        }
                        payload_offset += 8;
                    }
                    
                    size_t total_frame_size = payload_offset - offset + payload_len;
                    if (offset + total_frame_size <= buffer_pos) {
                        buffer[payload_offset + payload_len] = '\0';
                        
                        // Process TOB message
                        process_tob_message((const char*)buffer + payload_offset, payload_len);
                        
                        offset += total_frame_size;
                    } else {
                        // Incomplete frame, wait for more data
                        break;
                    }
                } else {
                    if (frame_size == 0) break;
                    offset += frame_size;
                }
            }
            
            // Move remaining data to start
            if (offset > 0 && offset < buffer_pos) {
                memmove(buffer, buffer + offset, buffer_pos - offset);
                buffer_pos -= offset;
            } else if (offset >= buffer_pos) {
                buffer_pos = 0;
            }
            
            // Prevent buffer overflow - process more aggressively
            if (buffer_pos > sizeof(buffer) - 1024) {
                // Try to process remaining frames before resetting
                size_t offset = 0;
                while (offset < buffer_pos) {
                    size_t frame_size = 0;
                    if (parse_ws_frame(buffer, buffer_pos, &offset, &frame_size)) {
                        size_t payload_offset = offset + 2;
                        uint64_t payload_len = buffer[offset + 1] & 0x7F;
                        if (payload_len == 126) {
                            payload_len = (buffer[offset + 2] << 8) | buffer[offset + 3];
                            payload_offset += 2;
                        } else if (payload_len == 127) {
                            payload_len = 0;
                            for (int i = 0; i < 8; i++) {
                                payload_len = (payload_len << 8) | buffer[offset + 2 + i];
                            }
                            payload_offset += 8;
                        }
                        size_t total_frame_size = payload_offset - offset + payload_len;
                        if (offset + total_frame_size <= buffer_pos) {
                            process_tob_message((const char*)buffer + payload_offset, payload_len);
                            offset += total_frame_size;
                        } else {
                            break;
                        }
                    } else {
                        if (frame_size == 0) break;
                        offset += frame_size;
                    }
                }
                if (offset > 0) {
                    memmove(buffer, buffer + offset, buffer_pos - offset);
                    buffer_pos -= offset;
                } else {
                    buffer_pos = 0; // Reset if we can't process
                }
            }
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(10000); // 10ms delay when no data available
                continue;
            } else {
                std::cout << "Connection error: " << strerror(errno) << std::endl;
                break;
            }
        } else {
            std::cout << "Connection closed by server" << std::endl;
            break;
        }
        
        // Process complete frames
        size_t offset = 0;
        while (offset < buffer_pos && g_running) {
            size_t frame_size = 0;
            if (parse_ws_frame(buffer, buffer_pos, &offset, &frame_size)) {
                // Extract payload
                size_t payload_offset = offset + 2;
                if (buffer[offset + 1] & 0x80) payload_offset += 4; // Skip mask
                if ((buffer[offset + 1] & 0x7F) == 126) payload_offset += 2;
                else if ((buffer[offset + 1] & 0x7F) == 127) payload_offset += 8;
                
                size_t payload_len = frame_size - (payload_offset - offset);
                buffer[payload_offset + payload_len] = '\0';
                
                // Process TOB message
                process_tob_message((const char*)buffer + payload_offset, payload_len);
                
                // Check if time to print table
                uint64_t now = get_timestamp_ns();
                if (now - last_print_time >= PRINT_INTERVAL_NS) {
                    update_count++;
                    print_table(g_symbol_data, g_instrument_to_symbol, update_count);
                    last_print_time = now;
                }
            }
            
            offset += frame_size;
            if (frame_size == 0) break;
        }
        
        // Move remaining data to start
        if (offset < buffer_pos) {
            memmove(buffer, buffer + offset, buffer_pos - offset);
            buffer_pos -= offset;
        } else {
            buffer_pos = 0;
        }
        
        // Check if time to print table (every 5 seconds)
        uint64_t now = get_timestamp_ns();
        if (now - last_print_time >= PRINT_INTERVAL_NS) {
            update_count++;
            print_table(g_symbol_data, g_instrument_to_symbol, update_count);
            last_print_time = now;
        }
    }
    
    std::cout << "\nShutting down gracefully..." << std::endl;
    std::cout.flush();
    
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
    
    std::cout << "Binance CS WebSocket client stopped." << std::endl;
    std::cout.flush();
    
    return 0;
}











