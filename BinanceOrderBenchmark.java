import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class BinanceOrderBenchmark {
    
    // ======= CONFIGURATION =======
    private static final String BASE_URL = "https://www.binance.tr";
    private static final String ORDER_ENDPOINT = "/open/v1/orders";
    private static final String ACCOUNT_ENDPOINT = "/open/v1/account/spot";
    
    // 🔐 SET YOUR API KEYS HERE
    private static final String API_KEY = "531CAbb391574ca9b80483DCce051A67zuzzoCGUVdisHFAh0BhkizQU6ATYkB41";
    private static final String API_SECRET = "D351356BDc508589d044b7b07854755dyEokAE9NPFGCbmAaeGQcaS3Qh1vLX61B";
    
    private static final String SYMBOL = "ACE_TRY";
    private static final int SIDE = 0;  // 0=BUY, 1=SELL
    private static final int ORDER_TYPE = 1;  // 1=LIMIT
    private static final String PRICE = "10";
    private static final String QUANTITY = "22";
    private static final int ORDER_COUNT = 5;
    private static final int RECV_WINDOW = 5000;
    private static final long WAIT_BETWEEN_ORDERS_MS = 1300;
    
    // Pre-compiled HMAC for performance
    private static Mac hmacSha256;
    private static final Gson gson = new Gson();
    
    // High-performance HTTP client
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)  // HTTP/2 for better performance
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    
    static {
        try {
            hmacSha256 = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(API_SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            hmacSha256.init(secretKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize HMAC", e);
        }
    }
    
    // =============================
    
    public static void main(String[] args) throws Exception {
        System.out.println("== BINANCE TR ORDER TEST (ULTRA-FAST JAVA) ==\n");
        
        // Step 1: Get account info
        System.out.println("=== STEP 1: GETTING ACCOUNT INFORMATION ===");
        AccountResult accountResult = getAccountInfo();
        
        System.out.printf("HTTP Status: %d%n", accountResult.httpStatus);
        System.out.printf("Round-trip: %.2f ms%n", accountResult.roundTripMs);
        
        JsonObject resp = accountResult.response;
        if (resp.has("code") && resp.get("code").getAsInt() == 0) {
            JsonObject data = resp.getAsJsonObject("data");
            System.out.println("\n✓ Account Info Retrieved Successfully");
            System.out.printf("Can Trade: %s%n", data.get("canTrade"));
            
            if (data.has("accountAssets")) {
                System.out.println("\nAccount Assets:");
                data.getAsJsonArray("accountAssets").forEach(asset -> {
                    JsonObject assetObj = asset.getAsJsonObject();
                    String assetName = assetObj.get("asset").getAsString();
                    double free = assetObj.get("free").getAsDouble();
                    double locked = assetObj.get("locked").getAsDouble();
                    double total = free + locked;
                    if (total > 0) {
                        System.out.printf("  %s: Free=%.8f, Locked=%.8f, Total=%.8f%n", 
                                assetName, free, locked, total);
                    }
                });
            }
        } else {
            System.out.println("✗ Error getting account info:");
            System.out.println("  Response: " + resp);
            return;
        }
        
        // Step 2: Send orders
        System.out.println("\n" + "=".repeat(50));
        System.out.println("=== STEP 2: SENDING ORDERS ===");
        System.out.printf("Symbol=%s, Price=%s, Qty=%s, Side=%s%n", 
                SYMBOL, PRICE, QUANTITY, SIDE == 0 ? "BUY" : "SELL");
        System.out.printf("%d emir gönderilecek...%n%n", ORDER_COUNT);
        
        List<Double> roundTrips = new ArrayList<>();
        List<Long> diffSentToCreateList = new ArrayList<>();
        
        for (int i = 0; i < ORDER_COUNT; i++) {
            System.out.printf("--- Order %d/%d ---%n", i + 1, ORDER_COUNT);
            OrderResult result = sendLimitOrder();
            
            roundTrips.add(result.roundTripMs);
            
            System.out.printf("HTTP Status: %d%n", result.httpStatus);
            System.out.printf("Round-trip: %.2f ms%n", result.roundTripMs);
            System.out.println();
            System.out.println("=== TIMING INFORMATION ===");
            System.out.printf("Order Sent Time (client): %d%n", result.orderSentTime);
            System.out.printf("Order Create Time (server): %d%n", result.orderCreateTime);
            System.out.printf("Response Timestamp (server): %d%n", result.responseTimestamp);
            System.out.println();
            System.out.println("=== TIME DIFFERENCES (ms) ===");
            if (result.diffSentToResponse != null) {
                System.out.printf("Sent → Response: %d ms%n", result.diffSentToResponse);
            }
            if (result.diffSentToCreate != null) {
                System.out.printf("⏱️  CRITICAL: Sent → Create: %d ms%n", result.diffSentToCreate);
                diffSentToCreateList.add(result.diffSentToCreate);
            }
            if (result.diffCreateToResponse != null) {
                System.out.printf("Create → Response: %d ms%n", result.diffCreateToResponse);
            }
            System.out.println();
            System.out.println("RESPONSE: " + result.response);
            System.out.println();
            
            if (i < ORDER_COUNT - 1) {
                Thread.sleep(WAIT_BETWEEN_ORDERS_MS);
            }
        }
        
        // Summary
        System.out.println("=== SUMMARY ===");
        DoubleSummaryStatistics rtStats = roundTrips.stream()
                .mapToDouble(Double::doubleValue)
                .summaryStatistics();
        System.out.printf("Round-trip → min %.2f ms | avg %.2f ms | max %.2f ms%n",
                rtStats.getMin(), rtStats.getAverage(), rtStats.getMax());
        
        if (!diffSentToCreateList.isEmpty()) {
            LongSummaryStatistics createStats = diffSentToCreateList.stream()
                    .mapToLong(Long::longValue)
                    .summaryStatistics();
            System.out.printf("⏱️  CRITICAL METRIC - Sent → Create: min %d | avg %.2f | max %d%n",
                    createStats.getMin(), createStats.getAverage(), createStats.getMax());
        }
    }
    
    private static String signPayload(Map<String, String> params) {
        StringBuilder query = new StringBuilder();
        params.forEach((key, value) -> {
            if (query.length() > 0) query.append("&");
            query.append(URLEncoder.encode(key, StandardCharsets.UTF_8))
                 .append("=")
                 .append(URLEncoder.encode(value, StandardCharsets.UTF_8));
        });
        
        byte[] hash = hmacSha256.doFinal(query.toString().getBytes(StandardCharsets.UTF_8));
        StringBuilder signature = new StringBuilder();
        for (byte b : hash) {
            signature.append(String.format("%02x", b));
        }
        
        return query.toString() + "&signature=" + signature;
    }
    
    private static AccountResult getAccountInfo() throws Exception {
        String url = BASE_URL + ACCOUNT_ENDPOINT;
        
        Map<String, String> params = new LinkedHashMap<>();
        params.put("recvWindow", String.valueOf(RECV_WINDOW));
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        
        String queryWithSig = signPayload(params);
        String fullUrl = url + "?" + queryWithSig;
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(fullUrl))
                .header("X-MBX-APIKEY", API_KEY)
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        
        long t0 = System.nanoTime();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        long t1 = System.nanoTime();
        
        double roundTrip = (t1 - t0) / 1_000_000.0;  // Convert to ms
        
        JsonObject data = gson.fromJson(response.body(), JsonObject.class);
        
        return new AccountResult(roundTrip, response.statusCode(), data);
    }
    
    private static OrderResult sendLimitOrder() throws Exception {
        String url = BASE_URL + ORDER_ENDPOINT;
        
        long orderSentTime = System.currentTimeMillis();
        
        Map<String, String> params = new LinkedHashMap<>();
        params.put("symbol", SYMBOL);
        params.put("side", String.valueOf(SIDE));
        params.put("type", String.valueOf(ORDER_TYPE));
        params.put("price", PRICE);
        params.put("quantity", QUANTITY);
        params.put("recvWindow", String.valueOf(RECV_WINDOW));
        params.put("timestamp", String.valueOf(orderSentTime));
        
        String body = signPayload(params);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("X-MBX-APIKEY", API_KEY)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        
        long t0 = System.nanoTime();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        long t1 = System.nanoTime();
        
        double roundTrip = (t1 - t0) / 1_000_000.0;
        
        JsonObject data = gson.fromJson(response.body(), JsonObject.class);
        
        Long responseTimestamp = data.has("timestamp") ? data.get("timestamp").getAsLong() : null;
        Long orderCreateTime = null;
        if (data.has("data") && data.get("data").isJsonObject()) {
            JsonObject dataObj = data.getAsJsonObject("data");
            if (dataObj.has("createTime")) {
                orderCreateTime = dataObj.get("createTime").getAsLong();
            }
        }
        
        Long diffSentToResponse = responseTimestamp != null ? responseTimestamp - orderSentTime : null;
        Long diffSentToCreate = orderCreateTime != null ? orderCreateTime - orderSentTime : null;
        Long diffCreateToResponse = (orderCreateTime != null && responseTimestamp != null) 
                ? responseTimestamp - orderCreateTime : null;
        
        return new OrderResult(
                roundTrip,
                response.statusCode(),
                orderSentTime,
                orderCreateTime,
                responseTimestamp,
                diffSentToResponse,
                diffSentToCreate,
                diffCreateToResponse,
                data
        );
    }
    
    static class AccountResult {
        double roundTripMs;
        int httpStatus;
        JsonObject response;
        
        AccountResult(double roundTripMs, int httpStatus, JsonObject response) {
            this.roundTripMs = roundTripMs;
            this.httpStatus = httpStatus;
            this.response = response;
        }
    }
    
    static class OrderResult {
        double roundTripMs;
        int httpStatus;
        long orderSentTime;
        Long orderCreateTime;
        Long responseTimestamp;
        Long diffSentToResponse;
        Long diffSentToCreate;
        Long diffCreateToResponse;
        JsonObject response;
        
        OrderResult(double roundTripMs, int httpStatus, long orderSentTime,
                   Long orderCreateTime, Long responseTimestamp,
                   Long diffSentToResponse, Long diffSentToCreate,
                   Long diffCreateToResponse, JsonObject response) {
            this.roundTripMs = roundTripMs;
            this.httpStatus = httpStatus;
            this.orderSentTime = orderSentTime;
            this.orderCreateTime = orderCreateTime;
            this.responseTimestamp = responseTimestamp;
            this.diffSentToResponse = diffSentToResponse;
            this.diffSentToCreate = diffSentToCreate;
            this.diffCreateToResponse = diffCreateToResponse;
            this.response = response;
        }
    }
}