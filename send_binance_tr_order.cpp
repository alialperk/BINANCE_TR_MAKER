#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>
#include "json.hpp"
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <iomanip>
#include <sstream>
#include <vector>
#include <numeric>
#include <algorithm>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
using json = nlohmann::json;

// ======= CONFIGURATION =======
const std::string API_KEY = "531CAbb391574ca9b80483DCce051A67zuzzoCGUVdisHFAh0BhkizQU6ATYkB41";
const std::string API_SECRET = "D351356BDc508589d044b7b07854755dyEokAE9NPFGCbmAaeGQcaS3Qh1vLX61B";

const std::string HOST = "www.binance.tr";
const std::string PORT = "443";
const std::string ORDER_ENDPOINT = "/open/v1/orders";
const std::string ACCOUNT_ENDPOINT = "/open/v1/account/spot";

const std::string SYMBOL = "ACE_TRY";
const int SIDE = 0;  // 0=BUY, 1=SELL
const int ORDER_TYPE = 1;  // 1=LIMIT
const std::string PRICE = "10";
const std::string QUANTITY = "22";
const int ORDER_COUNT = 1;
const int RECV_WINDOW = 5000;
const int WAIT_BETWEEN_ORDERS_MS = 1300;
// =============================

// URL encode helper
std::string url_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (char c : value) {
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << std::uppercase;
            escaped << '%' << std::setw(2) << int((unsigned char)c);
            escaped << std::nouppercase;
        }
    }

    return escaped.str();
}

// HMAC SHA256 signature
std::string hmac_sha256(const std::string& key, const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    
    HMAC_CTX* hmac = HMAC_CTX_new();
    HMAC_Init_ex(hmac, key.c_str(), key.length(), EVP_sha256(), nullptr);
    HMAC_Update(hmac, (unsigned char*)data.c_str(), data.length());
    unsigned int len = SHA256_DIGEST_LENGTH;
    HMAC_Final(hmac, hash, &len);
    HMAC_CTX_free(hmac);
    
    std::stringstream ss;
    for(int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    
    return ss.str();
}

// Get current timestamp in milliseconds
long long get_timestamp_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

// High-precision timer
double get_time_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()
    ).count() / 1000.0;  // Return in milliseconds
}

// Sign payload
std::string sign_payload(const std::string& query) {
    std::string signature = hmac_sha256(API_SECRET, query);
    return query + "&signature=" + signature;
}

struct AccountResult {
    double round_trip_ms;
    int http_status;
    json response;
};

struct OrderResult {
    double round_trip_ms;
    int http_status;
    long long order_sent_time;
    long long order_create_time;
    long long response_timestamp;
    long long diff_sent_to_response;
    long long diff_sent_to_create;
    long long diff_create_to_response;
    json response;
};

class BinanceClient {
private:
    net::io_context ioc_;
    ssl::context ctx_;
    tcp::resolver resolver_;
    beast::ssl_stream<beast::tcp_stream> stream_;
    
public:
    BinanceClient() 
        : ctx_(ssl::context::tlsv12_client)
        , resolver_(ioc_)
        , stream_(ioc_, ctx_) {
        
        // Load root certificates
        ctx_.set_default_verify_paths();
        ctx_.set_verify_mode(ssl::verify_none);  // For speed; use verify_peer in production
        
        // Set SNI Hostname
        if(!SSL_set_tlsext_host_name(stream_.native_handle(), HOST.c_str())) {
            beast::error_code ec{static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()};
            throw beast::system_error{ec};
        }
    }
    
    void connect() {
        auto const results = resolver_.resolve(HOST, PORT);
        beast::get_lowest_layer(stream_).connect(results);
        stream_.handshake(ssl::stream_base::client);
    }
    
    void close() {
        beast::error_code ec;
        stream_.shutdown(ec);
        // Ignore errors on shutdown
    }
    
    AccountResult get_account_info() {
        std::string query = "recvWindow=" + std::to_string(RECV_WINDOW) + 
                           "&timestamp=" + std::to_string(get_timestamp_ms());
        
        std::string signed_query = sign_payload(query);
        std::string target = ACCOUNT_ENDPOINT + "?" + signed_query;
        
        http::request<http::string_body> req{http::verb::get, target, 11};
        req.set(http::field::host, HOST);
        req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
        req.set("X-MBX-APIKEY", API_KEY);
        
        double t0 = get_time_us();
        http::write(stream_, req);
        
        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        http::read(stream_, buffer, res);
        double t1 = get_time_us();
        
        double round_trip = t1 - t0;
        
        json jv = json::parse(res.body());
        
        return AccountResult{round_trip, static_cast<int>(res.result_int()), jv};
    }
    
    OrderResult send_limit_order() {
        long long order_sent_time = get_timestamp_ms();
        
        std::string query = "symbol=" + SYMBOL +
                           "&side=" + std::to_string(SIDE) +
                           "&type=" + std::to_string(ORDER_TYPE) +
                           "&price=" + PRICE +
                           "&quantity=" + QUANTITY +
                           "&recvWindow=" + std::to_string(RECV_WINDOW) +
                           "&timestamp=" + std::to_string(order_sent_time);
        
        std::string body = sign_payload(query);
        
        http::request<http::string_body> req{http::verb::post, ORDER_ENDPOINT, 11};
        req.set(http::field::host, HOST);
        req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
        req.set(http::field::content_type, "application/x-www-form-urlencoded");
        req.set("X-MBX-APIKEY", API_KEY);
        req.body() = body;
        req.prepare_payload();
        
        double t0 = get_time_us();
        http::write(stream_, req);
        
        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        http::read(stream_, buffer, res);
        double t1 = get_time_us();
        
        double round_trip = t1 - t0;
        
        json jv = json::parse(res.body());
        
        long long response_timestamp = 0;
        long long order_create_time = 0;
        
        if (jv.is_object()) {
            if (jv.contains("timestamp")) {
                response_timestamp = jv["timestamp"].get<long long>();
            }
            if (jv.contains("data") && jv["data"].is_object()) {
                auto& data = jv["data"];
                if (data.contains("createTime")) {
                    order_create_time = data["createTime"].get<long long>();
                }
            }
        }
        
        long long diff_sent_to_response = response_timestamp - order_sent_time;
        long long diff_sent_to_create = order_create_time - order_sent_time;
        long long diff_create_to_response = response_timestamp - order_create_time;
        
        return OrderResult{
            round_trip,
            static_cast<int>(res.result_int()),
            order_sent_time,
            order_create_time,
            response_timestamp,
            diff_sent_to_response,
            diff_sent_to_create,
            diff_create_to_response,
            jv
        };
    }
};

int main() {
    try {
        std::cout << "== BINANCE TR ORDER TEST (ULTRA-FAST C++) ==\n\n";
        
        BinanceClient client;
        client.connect();
        
        // Step 1: Get account info
        std::cout << "=== STEP 1: GETTING ACCOUNT INFORMATION ===\n";
        AccountResult account_result = client.get_account_info();
        
        std::cout << "HTTP Status: " << account_result.http_status << "\n";
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Round-trip: " << account_result.round_trip_ms << " ms\n";
        
        auto& resp = account_result.response;
        if (resp.contains("code") && resp["code"].get<int>() == 0) {
            auto& data = resp["data"];
            std::cout << "\n✓ Account Info Retrieved Successfully\n";
            std::cout << "Can Trade: " << data["canTrade"].get<int>() << "\n";
            
            if (data.contains("accountAssets")) {
                std::cout << "\nAccount Assets:\n";
                auto& assets = data["accountAssets"];
                for (auto& asset : assets) {
                    std::string asset_name = asset["asset"].get<std::string>();
                    double free = std::stod(asset["free"].get<std::string>());
                    double locked = std::stod(asset["locked"].get<std::string>());
                    double total = free + locked;
                    if (total > 0) {
                        std::cout << "  " << asset_name << ": Free=" << free 
                                  << ", Locked=" << locked << ", Total=" << total << "\n";
                    }
                }
            }
        } else {
            std::cout << "✗ Error getting account info\n";
            std::cout << "Response: " << account_result.response << "\n";
            return 1;
        }
        
        // Step 2: Send orders
        std::cout << "\n" << std::string(50, '=') << "\n";
        std::cout << "=== STEP 2: SENDING ORDERS ===\n";
        std::cout << "Symbol=" << SYMBOL << ", Price=" << PRICE 
                  << ", Qty=" << QUANTITY << ", Side=" << (SIDE == 0 ? "BUY" : "SELL") << "\n";
        std::cout << ORDER_COUNT << " emir gönderilecek...\n\n";
        
        std::vector<double> round_trips;
        std::vector<long long> diff_sent_to_create_list;
        
        for (int i = 0; i < ORDER_COUNT; i++) {
            std::cout << "--- Order " << (i+1) << "/" << ORDER_COUNT << " ---\n";
            OrderResult result = client.send_limit_order();
            
            round_trips.push_back(result.round_trip_ms);
            diff_sent_to_create_list.push_back(result.diff_sent_to_create);
            
            std::cout << "HTTP Status: " << result.http_status << "\n";
            std::cout << std::fixed << std::setprecision(2);
            std::cout << "Round-trip: " << result.round_trip_ms << " ms\n\n";
            
            std::cout << "=== TIMING INFORMATION ===\n";
            std::cout << "Order Sent Time (client): " << result.order_sent_time << "\n";
            std::cout << "Order Create Time (server): " << result.order_create_time << "\n";
            std::cout << "Response Timestamp (server): " << result.response_timestamp << "\n\n";
            
            std::cout << "=== TIME DIFFERENCES (ms) ===\n";
            std::cout << "Sent → Response: " << result.diff_sent_to_response << " ms\n";
            std::cout << "⏱️  CRITICAL: Sent → Create: " << result.diff_sent_to_create << " ms\n";
            std::cout << "Create → Response: " << result.diff_create_to_response << " ms\n\n";
            
            std::cout << "RESPONSE: " << result.response << "\n\n";
            
            if (i < ORDER_COUNT - 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_BETWEEN_ORDERS_MS));
            }
        }
        
        // Summary
        std::cout << "=== SUMMARY ===\n";
        double min_rt = *std::min_element(round_trips.begin(), round_trips.end());
        double max_rt = *std::max_element(round_trips.begin(), round_trips.end());
        double avg_rt = std::accumulate(round_trips.begin(), round_trips.end(), 0.0) / round_trips.size();
        
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Round-trip → min " << min_rt << " ms | avg " << avg_rt 
                  << " ms | max " << max_rt << " ms\n";
        
        if (!diff_sent_to_create_list.empty()) {
            long long min_create = *std::min_element(diff_sent_to_create_list.begin(), 
                                                     diff_sent_to_create_list.end());
            long long max_create = *std::max_element(diff_sent_to_create_list.begin(), 
                                                     diff_sent_to_create_list.end());
            double avg_create = std::accumulate(diff_sent_to_create_list.begin(), 
                                               diff_sent_to_create_list.end(), 0.0) / 
                               diff_sent_to_create_list.size();
            
            std::cout << "⏱️  CRITICAL METRIC - Sent → Create: min " << min_create 
                      << " | avg " << avg_create << " | max " << max_create << "\n";
        }
        
        client.close();
        
    } catch (std::exception const& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}