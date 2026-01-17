CXX = g++
CXXFLAGS = -std=c++17 -O3 -march=native -flto -Wall -Wextra -fPIC
LIBS = -lboost_system -lboost_thread -lssl -lcrypto -lpthread

# Standalone executable (for testing)
websocket_client_cpp: websocket_client_cpp.cpp
	$(CXX) $(CXXFLAGS) -o websocket_client_cpp websocket_client_cpp.cpp $(LIBS)

# Shared library for Python integration
libwebsocket_order_sender.so: websocket_order_sender.cpp
	$(CXX) $(CXXFLAGS) -shared -o libwebsocket_order_sender.so websocket_order_sender.cpp $(LIBS)

clean:
	rm -f websocket_client_cpp libwebsocket_order_sender.so

.PHONY: clean
