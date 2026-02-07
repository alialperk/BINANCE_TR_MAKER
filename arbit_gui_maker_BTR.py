import sys
import redis
import pickle
import pandas as pd
import json
import logging
import os
import subprocess
import time
from datetime import datetime
from functools import lru_cache
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
from datetime import datetime
import arbit_config_maker_BTR as arbit_config_maker
import ccxt
import numpy as np

# Helper to check if a value is a number
def is_number(x):
    try:
        float(x)
        return True
    except (ValueError, TypeError):
        return False

from PyQt6.QtWidgets import (QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, 
                            QHBoxLayout, QLabel, QPushButton, QTableWidget, QTableWidgetItem,
                            QHeaderView, QComboBox, QLineEdit, QSpinBox, QDoubleSpinBox,
                            QCheckBox, QMessageBox, QSplitter, QFrame, QGridLayout, QStackedWidget,
                            QMenu, QGroupBox, QScrollArea, QSizePolicy, QAbstractItemView, QListWidget)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QThread, QSize, QItemSelectionModel, QLocale
from PyQt6.QtGui import QFont, QColor, QBrush, QCursor, QIcon, QPixmap, QPalette, QPainter, QAction

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataRefreshThread(QThread):
    data_refreshed = pyqtSignal(object)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.running = True
        self.redis = redis.Redis(host='localhost', port=6379)
    def run(self):
        while self.running:
            try:
                df = self.get_maker_arbitrage_table()
                self.data_refreshed.emit(df)
            except Exception as e:
                logging.error(f"Error in refresh thread: {e}")
            time.sleep(1)  # Refresh every second
            
    def stop(self):
        self.running = False
        
    def get_maker_arbitrage_table(self):
        """Get and format arbitrage table data."""
        
        try:
            pickled_data = self.redis.get('maker_arbitrage_table')
            if pickled_data:
                df = pickle.loads(pickled_data)
                df.columns = arbit_config_maker.columns
                
                # Convert timestamp columns to datetime and format them, handling NaN values
                def format_timestamp(ts):
                    try:
                        if pd.isna(ts):
                            return ''
                        dt = pd.to_datetime(ts, unit='ms')
                        return dt.strftime('%H:%M:%S')
                    except Exception as e:
                        print(f"Error formatting timestamp {ts}: {e}")
                        return ''
                
                # Format time columns
                if 'Binance_Time' in df.columns:
                    df['Binance_Time'] = df['Binance_Time'].apply(format_timestamp)
                if 'EXCHANGE_Time' in df.columns:
                    df['EXCHANGE_Time'] = df['EXCHANGE_Time'].apply(format_timestamp)
                
                # Convert TimeDiff from milliseconds to seconds
                #df['TimeDiff'] = df['TimeDiff'].apply(lambda x: round(x/1000, 3) if pd.notna(x) else '')
                
                # Ensure numeric columns are properly typed
                numeric_columns = [
                    'Binance_AskP1', 'Binance_AskA1', 'Binance_BidP1', 'Binance_BidA1',
                    'Binance_free_usdt', 'Binance_free_TRY', 'EXCHANGE_free_TRY',
                    'OpenMargin', 'CloseMargin',
                    'EXCHANGE_PositionAmount_TRY', 'OpenTriggerMargin', 'CloseTriggerMargin',
                    'OpenStopMargin', 'CloseStopMargin', 'OpenAggression', 'CloseAggression',
                    'OpenMarginWindow', 'CloseMarginWindow', 'MinBuyOrderAmount_TRY', 'BuyOrderAmount_TRY', 'MaxBuyOrderAmount_TRY', 'MinSellOrderAmount_TRY', 'MaxSellOrderAmount_TRY', 'MaxPositionAmount_TRY', 'USDTTRY_bid', 'USDTTRY_ask',
                    'Binance_TimeDiff', 'binance_absolute_time_diff'  # CRITICAL: Must be numeric for proper sorting
                ]
                
                # Ensure EXCHANGE_Symbol is treated as string before any numeric operations
                if 'EXCHANGE_Symbol' in df.columns:
                    df['EXCHANGE_Symbol'] = df['EXCHANGE_Symbol'].astype(str)
                
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Ensure BaseSymbol column exists - use Base_Symbol from config if available, otherwise create from EXCHANGE_Symbol
                if 'BaseSymbol' in df.columns:
                    # BaseSymbol already exists, ensure it's string and handle NaN values
                    df['BaseSymbol'] = df['BaseSymbol'].astype(str).replace('nan', '').replace('NaN', '')
                elif 'Base_Symbol' in df.columns:
                    # Map Base_Symbol (from config) to BaseSymbol (for GUI)
                    df['BaseSymbol'] = df['Base_Symbol'].astype(str).replace('nan', '').replace('NaN', '')
                elif 'EXCHANGE_Symbol' in df.columns:
                    # Fallback: create BaseSymbol from EXCHANGE_Symbol
                    df['BaseSymbol'] = df['EXCHANGE_Symbol'].astype(str).str.replace('_TRY', '').replace('nan', '').replace('NaN', '')
                else:
                    # Last resort: create empty BaseSymbol column
                    df['BaseSymbol'] = ''
               
                return df
        except Exception as e:
            print(f"Error retrieving arbitrage table: {e}")
            return None

EXCHANGE_history_page_no = 1
binance_history_page_no = 1
move_history_page_no = 1


maker_EXCHANGE_trade_history_columns = ["ExecutionTime", "OrderId", "Symbol", "Core", "Side", "Price", "Amount", "AmountTRY", "ExecutedMargin"]

class TradeHistoryRefreshThread(QThread):

    EXCHANGE_data_refreshed = pyqtSignal(object)
    binance_data_refreshed = pyqtSignal(object)
    EXCHANGE_open_orders_refreshed = pyqtSignal(object)
    move_history_data_refreshed = pyqtSignal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.running = True
        self.redis = redis.Redis(host='localhost', port=6379)
        self.page_size = 100  # Number of records per page


    def run(self):

        while self.running:
            try:

                #Get EXCHANGE trade records
                EXCHANGE_trade_records = []
                EXCHANGE_start_index = -self.page_size*EXCHANGE_history_page_no
                EXCHANGE_end_index = -self.page_size*(EXCHANGE_history_page_no-1) - 1
                # Get the most recent items (from newest to oldest)
                EXCHANGE_trade_records_data = self.redis.lrange('maker_EXCHANGE_trade_history', EXCHANGE_start_index, EXCHANGE_end_index)
                
                for record in EXCHANGE_trade_records_data:
                    try:
                        trade_data = json.loads(record)
                        required_fields = maker_EXCHANGE_trade_history_columns
                        if all(field in trade_data for field in required_fields):
                            EXCHANGE_trade_records.append(trade_data)
                    except json.JSONDecodeError:
                        continue
                
                if EXCHANGE_trade_records:
                    EXCHANGE_df = pd.DataFrame(EXCHANGE_trade_records)
                    self.EXCHANGE_data_refreshed.emit(EXCHANGE_df)
                
                # Get Binance trade records
                binance_trade_records = []  
                binance_start_index = -self.page_size*binance_history_page_no
                binance_end_index = -self.page_size*(binance_history_page_no-1) - 1
                # Get the most recent items (from newest to oldest)
                binance_trade_records_data = self.redis.lrange('maker_BINANCE_trade_history', binance_start_index, binance_end_index)

                for record in binance_trade_records_data:
                    try:
                        trade_data = json.loads(record)
                        required_fields = ["OrderTime", "Symbol", "Amount"]
                        if all(field in trade_data for field in required_fields):
                            binance_trade_records.append(trade_data)
                    except json.JSONDecodeError:
                        continue
                
                if binance_trade_records:
                    binance_df = pd.DataFrame(binance_trade_records)
                    self.binance_data_refreshed.emit(binance_df)

                #Get EXCHANGE open orders
                EXCHANGE_open_orders = []
                EXCHANGE_open_orders_data = self.redis.lrange('maker_open_orders', 0, -1)
                
                
                for record in EXCHANGE_open_orders_data:
                    try:
                        order_data = json.loads(record)
                        # Ensure all required fields are present
                        required_fields = ['OrderTime', 'OrderId', 'Symbol', 'Side', 'Price', 'Amount', 'AmountTRY', 'TriggerMargin']
                        if all(field in order_data for field in required_fields):
                            EXCHANGE_open_orders.append(order_data)
                    except json.JSONDecodeError:
                        continue
                
                if EXCHANGE_open_orders:
                    EXCHANGE_open_orders_df = pd.DataFrame(EXCHANGE_open_orders)
                    # Sort by OrderTime descending (newest first)
                    # Handle both formats: with and without microseconds
                    EXCHANGE_open_orders_df['OrderTime'] = pd.to_datetime(EXCHANGE_open_orders_df['OrderTime'], format='mixed', errors='coerce')
                    EXCHANGE_open_orders_df = EXCHANGE_open_orders_df.sort_values('OrderTime', ascending=False)
                    # Convert back to string format
                    EXCHANGE_open_orders_df['OrderTime'] = EXCHANGE_open_orders_df['OrderTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    self.EXCHANGE_open_orders_refreshed.emit(EXCHANGE_open_orders_df)
                else:
                    # Emit empty DataFrame with correct columns
                    EXCHANGE_open_orders_df = pd.DataFrame(columns=['OrderTime', 'OrderId', 'Symbol', 'Side', 'Price', 'Amount', 'AmountTRY', 'TriggerMargin'])
                    self.EXCHANGE_open_orders_refreshed.emit(EXCHANGE_open_orders_df)
                
                # Get Move History records
                move_history_records = []
                move_history_start_index = -self.page_size*move_history_page_no
                move_history_end_index = -self.page_size*(move_history_page_no-1) - 1
                # Get the most recent items (from newest to oldest)
                move_history_data = self.redis.lrange('maker_move_history', move_history_start_index, move_history_end_index)
                
                # Get total count for debugging
                total_move_history_count = self.redis.llen('maker_move_history')
                logging.debug(f"Move history: Total count in Redis: {total_move_history_count}, Fetched records: {len(move_history_data)}")
                
                for record in move_history_data:
                    try:
                        move_data = json.loads(record)
                        required_fields = ["MoveTime", "OrderID", "Symbol", "Core", "BinanceExecutedMargin", "EXCHANGEExecutedMargin", "StopMargin", "MoveThreshold", "MarginDifference", "PNL", "Source"]
                        if all(field in move_data for field in required_fields):
                            move_history_records.append(move_data)
                        else:
                            missing_fields = [field for field in required_fields if field not in move_data]
                            logging.debug(f"Move history record missing fields: {missing_fields}, record keys: {list(move_data.keys())}")
                    except json.JSONDecodeError as e:
                        logging.debug(f"Error parsing move history record: {e}")
                        continue
                
                if move_history_records:
                    move_history_df = pd.DataFrame(move_history_records)
                    # Sort by MoveTime descending (newest first)
                    # Handle both formats: with and without microseconds
                    move_history_df['MoveTime'] = pd.to_datetime(move_history_df['MoveTime'], format='mixed', errors='coerce')
                    move_history_df = move_history_df.sort_values('MoveTime', ascending=False)
                    # Convert back to string format (preserve milliseconds - truncate to 3 digits)
                    move_history_df['MoveTime'] = move_history_df['MoveTime'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:23]
                    self.move_history_data_refreshed.emit(move_history_df)
                else:
                    # Emit empty DataFrame with correct columns
                    move_history_df = pd.DataFrame(columns=['MoveTime', 'OrderID', 'Symbol', 'Core', 'BinanceExecutedMargin', 'EXCHANGEExecutedMargin', 'StopMargin', 'MoveThreshold', 'MarginDifference', 'PNL', 'Source'])
                    self.move_history_data_refreshed.emit(move_history_df)
            
                
            except Exception as e:
                logging.error(f"Error in trade history refresh thread: {e}")
            
            time.sleep(1)  # Refresh every second
            
    def stop(self):
        self.running = False

class NumericTableWidgetItem(QTableWidgetItem):
    """Custom QTableWidgetItem that sorts numerically"""
    
    def __init__(self, text, value=None):
        super().__init__(text)
        self.value = value if value is not None else text
        
    def __lt__(self, other):
        if hasattr(other, 'value'):
            if isinstance(self.value, (int, float)) and isinstance(other.value, (int, float)):
                return self.value < other.value
            elif isinstance(self.value, str) and isinstance(other.value, str):
                return self.value.lower() < other.value.lower()
        
        # Fall back to string comparison of the text
        return self.text().lower() < other.text().lower()

class BooleanTableWidgetItem(QTableWidgetItem):
    """Custom QTableWidgetItem for sorting boolean values (True/False)"""
    def __init__(self, value):
        # Store boolean value
        self.bool_value = bool(value)
        # Set text to sortable value (0 for False, 1 for True) so Qt can sort even when widget is present
        # The widget will be displayed, but sorting will use this text value
        super().__init__(str(int(self.bool_value)))
        # Set the data role to the boolean value for proper sorting
        # Use UserRole to store the sortable value
        self.setData(Qt.ItemDataRole.UserRole, int(self.bool_value))
        # Also set DisplayRole for fallback sorting
        self.setData(Qt.ItemDataRole.DisplayRole, int(self.bool_value))
        
    def __lt__(self, other):
        """Sort: False values first, then True values"""
        if isinstance(other, BooleanTableWidgetItem):
            # False (0) < True (1), so False comes first
            return int(self.bool_value) < int(other.bool_value)
        # If comparing with non-BooleanTableWidgetItem, try to get data from UserRole
        other_value = other.data(Qt.ItemDataRole.UserRole)
        if other_value is not None:
            return int(self.bool_value) < int(other_value)
        # Try to parse text as integer for comparison
        try:
            other_text = other.text()
            if other_text:
                return int(self.bool_value) < int(other_text)
        except (ValueError, AttributeError):
            pass
        # Fallback: treat as False
        return False

class ArbitrageMonitor(QMainWindow):
    def __init__(self):
        super().__init__()
        
        # Kill all related processes before starting GUI
        self.kill_all_related_processes()
        
        
        # Set CPU affinity for GUI process (isolated from trading processes)
        # This ensures GUI doesn't interfere with trading processes (cores 0-23)
        self.set_gui_cpu_affinity()
        
        self.open_trade_columns = [
            'BaseSymbol', 'Maker_Type', 'OpenMargin', 'OpenTriggerMargin', 'OpenStopMargin', 'OpenAggression', 'OpenMarginWindow',
            'EXCHANGE_PositionAmount_TRY', 'EXCHANGE_PositionAmount_coin', 'Binance_PositionAmount_coin',
            'MaxPositionAmount_TRY', 'CapacityGap_TRY', 'Binance_Time', 'EXCHANGE_Time', 'Binance_TimeDiff', 'binance_absolute_time_diff']

        self.close_trade_columns = [
            'BaseSymbol', 'Maker_Type', 'CloseMargin', 'CloseTriggerMargin', 'CloseStopMargin', 'CloseAggression', 'CloseMarginWindow',
            'EXCHANGE_PositionAmount_TRY', 'EXCHANGE_PositionAmount_coin', 'Binance_PositionAmount_coin',
            'Binance_Time', 'EXCHANGE_Time', 'Binance_TimeDiff']

        self.EXCHANGE_open_orders_columns = [
            'OrderTime', 'OrderId', 'Symbol', 'Core', 'Side', 'Price', 'Amount', 'AmountTRY', 'TriggerMargin'
        ]

        
        self.maker_arbitrage_table = None

        self.prev_arbitrage_state = None
        self.prev_enable_orders_state = None
        
        # Store C++ WebSocket process references for cleanup
        self.cpp_EXCHANGE_process = None
        self.cpp_binance_process = None
        self.cpp_binance_cs_process = None
        
        # Store arbitrage script process references for cleanup
        self.arbitrage_processes = []
        
        # Initialize balance history for 10-second moving average (single account: EXCHANGE)
        self.EXCHANGE_balance_history = []  # Keep last 10 readings for moving average
        self.buy_order_initialized = False  # Track if buy orders have been initialized
        
        # Initialize Redis connection
        self.redis = redis.Redis(host='localhost', port=6379)

        TEST_MODE = self.redis.get('TEST_MODE')
        if TEST_MODE == b'1':
            self.account_type = "MAKER_TEST"
        else:
            self.account_type = "MAKER"
        
        # Reset maker_arbitrage_state if it's in "starting" or "stopping" state at GUI startup
        # This ensures a clean state when GUI starts (prevents stuck states from previous sessions)
        try:
            arbitrage_state = self.redis.get('maker_arbitrage_state')
            if arbitrage_state:
                state_str = arbitrage_state.decode('utf-8') if isinstance(arbitrage_state, bytes) else arbitrage_state
                if state_str in ['starting', 'stopping']:
                    self.redis.set('maker_arbitrage_state', 'stopped')
                    logging.info(f"GUI startup: Reset maker_arbitrage_state from '{state_str}' to 'stopped'")
        except Exception as e:
            logging.warning(f"Error checking/resetting maker_arbitrage_state at GUI startup: {e}")
        
        # Create dummy move history record at startup if Redis is empty (only once)
        try:
            total_count = self.redis.llen('maker_move_history')
            if total_count == 0:
                from datetime import datetime
                StopMargin = 30.00
                BinanceExecutedMargin = 14.58
                EXCHANGEExecutedMargin = 25.00
                MarginDifference = round((BinanceExecutedMargin - StopMargin), 2)
                PNL = round(-150.50, 2)

                dummy_move_history = {
                    'MoveTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                    'OrderID': '12345678',
                    'Symbol': 'BTC',
                    'Core': 1,
                    'BinanceExecutedMargin': BinanceExecutedMargin,
                    'EXCHANGEExecutedMargin': EXCHANGEExecutedMargin,
                    'StopMargin': StopMargin,
                    'MoveThreshold': 15,
                    'MarginDifference': MarginDifference,
                    'PNL': PNL,
                    'Source': 'BINANCE'
                }
                # Save dummy record to Redis
                self.redis.rpush('maker_move_history', json.dumps(dummy_move_history))
                logging.info("Dummy move history record created at startup")
        except Exception as e:
            logging.error(f"Error creating dummy move history at startup: {e}")

        # Set up the main window
        self.setWindowTitle("EXCHANGE-BINANCE MAKER")
        # Set window icon
        icon_path = os.path.join(os.path.dirname(__file__), 'EXCHANGElogo.png')
        print(f"Icon path: {icon_path}")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
        self.setGeometry(100, 100, 1920, 1080)

        # Create the central widget and main layout
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)
        
        # Initialize refresh threads first
        self.refresh_thread = DataRefreshThread()
        self.refresh_thread.data_refreshed.connect(self.update_monitoring_data)
        
        self.trade_history_thread = TradeHistoryRefreshThread()
        self.trade_history_thread.EXCHANGE_data_refreshed.connect(self.update_maker_EXCHANGE_trade_history)
        self.trade_history_thread.binance_data_refreshed.connect(self.update_maker_binance_trade_history)
        self.trade_history_thread.EXCHANGE_open_orders_refreshed.connect(self.update_EXCHANGE_open_orders)
        self.trade_history_thread.move_history_data_refreshed.connect(self.update_maker_move_history)
        # Create tab widget
        self.tab_widget = DetachableTabWidget()

        # Set a custom stylesheet to increase the tab bar height
        self.tab_widget.setStyleSheet("""
            QTabBar::tab {
                height: 40px;  /* Set the desired height for the tab bar */
            }
        """)
        
        # Create the monitoring page
        self.monitoring_page = QWidget()
        self.tab_widget.addTab(self.monitoring_page, "Monitoring")
        
        # Create the settings page
        self.settings_page = QWidget()
        self.tab_widget.addTab(self.settings_page, "Settings")
        
        # Create the symbol management page
        self.symbol_management_page = QWidget()
        self.tab_widget.addTab(self.symbol_management_page, "Symbol Management")
        
        # Connect tab change signal
        self.tab_widget.currentChanged.connect(self.on_tab_changed)
        
        # Add the tab widget to the main layout
        self.main_layout.addWidget(self.tab_widget)
        
        # Create a widget to hold the controls
        control_widget = QWidget()
        control_layout = QHBoxLayout(control_widget)
        # Apply a custom stylesheet to the control widget
        control_widget.setStyleSheet("""
            QWidget {
                border: 1px solid #555555;     /* Optional: Add a border */
                border-radius: 5px;         /* Optional: Add rounded corners */
                padding: 2px;              /* Add padding inside the widget */
            }
        """)
        control_layout.setContentsMargins(5, 0, 5, 0)
        control_layout.setSpacing(5)  # Reduce spacing to move buttons closer together
        
        # Health Details button - leftmost part
        self.health_details_btn = QPushButton("Health\nDetails")
        self.health_details_btn.setFixedWidth(65)
        self.health_details_btn.setFixedHeight(45)
        self.health_details_btn.setStyleSheet("""
            QPushButton {
                background-color: #555555;
                color: white;
                font-weight: normal;
                font-size: 11px;
            }
            QPushButton:hover {
                background-color: #1565c0;
            }
            QPushButton:pressed {
                background-color: #0d47a1;
            }
        """)
        self.health_details_btn.clicked.connect(self.show_health_details)
        control_layout.addWidget(self.health_details_btn)
        
        # Health Status Box (2x2 grid)
        health_status_widget = QWidget()
        health_status_layout = QGridLayout(health_status_widget)
        health_status_layout.setContentsMargins(5, 5, 5, 5)
        health_status_layout.setSpacing(3)
        
        # Create status lights (QLabel with colored background)
        # Row 1, Col 1: EXCHANGE Data
        self.EXCHANGE_data_order_label = QLabel("EXCHANGE DATA | ORDER")
        self.EXCHANGE_data_order_label.setStyleSheet("color: white; font-size: 11px;")
        self.EXCHANGE_data_light = QLabel()
        self.EXCHANGE_data_light.setFixedSize(15, 15)
        self.EXCHANGE_data_light.setStyleSheet("background-color: gray; border-radius: 7px;")
        self.EXCHANGE_order_light = QLabel()
        self.EXCHANGE_order_light.setFixedSize(15, 15)
        self.EXCHANGE_order_light.setStyleSheet("background-color: gray; border-radius: 7px;")
        health_status_layout.addWidget(self.EXCHANGE_data_order_label, 0, 0)
        health_status_layout.addWidget(self.EXCHANGE_data_light, 0, 1)
        health_status_layout.addWidget(self.EXCHANGE_order_light, 0, 2)
        
        # Row 2, Col 1: CS Binance Data
        self.cs_data_order_label = QLabel("BINANCE DATA | ORDER")
        self.cs_data_order_label.setStyleSheet("color: white; font-size: 11px;")
        self.cs_data_light = QLabel()
        self.cs_data_light.setFixedSize(15, 15)
        self.cs_data_light.setStyleSheet("background-color: gray; border-radius: 7px;")
        self.cs_order_light = QLabel()
        self.cs_order_light.setFixedSize(15, 15)
        self.cs_order_light.setStyleSheet("background-color: gray; border-radius: 7px;")
        health_status_layout.addWidget(self.cs_data_order_label, 1, 0)
        health_status_layout.addWidget(self.cs_data_light, 1, 1)
        health_status_layout.addWidget(self.cs_order_light, 1, 2)
                
        health_status_widget.setFixedWidth(200)
        health_status_widget.setFixedHeight(50)
        health_status_widget.setStyleSheet("""
            QWidget {
                border: 1px solid #555555;
                border-radius: 3px;
                background-color: #2d2d2d;
            }
        """)
        control_layout.addWidget(health_status_widget)
        
        # System control button - make it more compact
        self.system_control_btn = QPushButton("START")
        self.system_control_btn.setFixedWidth(130)  # Make button narrower
        self.system_control_btn.setFixedHeight(45)  # Make button narrower
        self.system_control_btn.setStyleSheet("""
            QPushButton {
                background-color: darkred;
                color: white;
            }
            QPushButton:hover {
                background-color: orange;
            }
        """)
        self.system_control_btn.clicked.connect(self.toggle_system)
        control_layout.addWidget(self.system_control_btn)
        
        # Orders control button
        self.orders_control_btn = QPushButton("Orders Disabled\n(press to enable)")
        self.orders_control_btn.setFixedWidth(130)
        self.orders_control_btn.setFixedHeight(45)
        self.orders_control_btn.setStyleSheet("""
            QPushButton {
                background-color: darkred;
                color: white;
            }
            QPushButton:hover {
                background-color: orange;
            }
        """)
        self.orders_control_btn.clicked.connect(self.toggle_orders)
        control_layout.addWidget(self.orders_control_btn)
        


        # Update balance button - make it more compact with 2-line text
        self.update_balance_btn = QPushButton("Update\nBalances")
        self.update_balance_btn.setFixedWidth(90)  # Make button narrower
        self.update_balance_btn.setFixedHeight(45)  # Make button taller for 2 lines
        self.update_balance_btn.clicked.connect(self.update_balances)
        self.update_balance_btn.setEnabled(True)
        self.update_balance_btn.setStyleSheet("""
            QPushButton {
                background-color: #0d47a1;
                color: white;
                font-weight: normal;
                font-size: 12px;
            }
            QPushButton:hover {
                background-color: #1565c0;
            }
            QPushButton:pressed {
                background-color: #0d47a1;
            }
        """)
        control_layout.addWidget(self.update_balance_btn)        
        
        # Cancel orders button
        self.cancel_orders_btn = QPushButton("CANCEL ALL\nORDERS")
        self.cancel_orders_btn.setFixedWidth(100)  # Make button narrower
        self.cancel_orders_btn.setFixedHeight(45)  # Make button taller for 2 lines
        self.cancel_orders_btn.clicked.connect(self.cancel_orders)
        self.cancel_orders_btn.setEnabled(True)
        self.cancel_orders_btn.setStyleSheet("""
            QPushButton {
                background-color: darkred;
                color: white;
                font-weight: normal;
                font-size: 12px;
            }
            QPushButton:hover {
                background-color: red;
            }
            QPushButton:pressed {
                background-color: darkred;
            }
        """)
        control_layout.addWidget(self.cancel_orders_btn)
        
        # Balances label - fix the escape sequence
        # Balances label - fix the escape sequence and add line spacing
        self.balances_label = QLabel("""
            <div style="line-height: 1.4;">
                Binance: $0.00 (₺0.00)<br>
                EXCHANGE: ₺0.00
            </div>
        """)
        control_layout.addWidget(self.balances_label)
        
        # PNL label
        self.pnl_label = QLabel("""
            <div style="line-height: 1.4;">
                CumPNL: ₺0.00<br>
                TL_Pos: ₺0.00
            </div>
        """)
        control_layout.addWidget(self.pnl_label)
        
        # PNL2 label
        self.pnl2_label = QLabel("""
            <div style="line-height: 1.4;">
                USDT_Pos: 0.00<br>
                AVG_USDTTRY: 0.00
            </div>
        """)
        control_layout.addWidget(self.pnl2_label)

        control_layout.addSpacing(20)
        
        # Add the control widget to the tab corner
        self.tab_widget.setCornerWidget(control_widget, Qt.Corner.TopRightCorner)
        
        # Set up each tab
        self.setup_monitoring_page()
        self.setup_settings_page()
     
        # Start refresh threads
        self.refresh_thread.start()
        self.trade_history_thread.start()

        
        # Timer for updating balances and PNL
        self.balance_timer = QTimer()
        self.balance_timer.timeout.connect(self.update_balances_and_pnl)

        # Check system status
        self.balance_timer.timeout.connect(self.check_system_status)
        self.balance_timer.start(1000)  # Update every second
        
        # Timer for updating health status
        self.health_timer = QTimer()
        self.health_timer.timeout.connect(self.update_health_status)
        self.health_timer.start(2000)  # Update every 2 seconds
        
        # Initialize highlighted rows for trade history
        self.EXCHANGE_highlighted_rows = {}
        self.binance_highlighted_rows = {}
        
        # Initialize last seen trade counts
        self.last_EXCHANGE_trade_count = 0
        self.last_binance_trade_count = 0
        
        # Add variables to track selection
        self.open_trade_selected_symbol = None
        self.open_trade_selected_row = None
        self.apply_dark_theme()

        #Executed trade highlight duration in seconds
        self.highlight_duration = 10

        symbol_list = self.redis.get('EXCHANGE_Symbol_List')
        self.current_symbols = json.loads(symbol_list.decode('utf-8')) if symbol_list else []
        

        # Initialize selected symbols list in memory
        self.selected_symbols = []
        
        # Track selected symbols for persistent selection across table refreshes
        self.open_trade_selected_symbols = set()
        self.close_trade_selected_symbols = set()
        
        # Store previous selection for restoration
        self.previous_open_selection = set()
        self.previous_close_selection = set()
        
        # Track restoration state to prevent redundancy
        self.is_restoring_open = False
        self.is_restoring_close = False
        
        # Global selection tracking - accumulate selections instead of overwriting
        self.current_open_selection = set()
        self.current_close_selection = set()
        
        # Track the last selected row for shift+click range selection
        self.last_open_selected_row = -1
        self.last_close_selected_row = -1
        
        # Flag to prevent restoration during user interaction
        self.user_interacting = False
        
        # Flag to block selection tracking during updates
        self.block_selection_tracking = False

    def apply_dark_theme(self):
        """Apply a simplified dark theme using only stylesheets"""
        self.setStyleSheet("""
            /* Main application */
            QWidget {
                background-color: #222222;
                color: #ffffff;
            }
            
            /* Tables */
            QTableWidget {
                gridline-color: #555555;
                background-color: #2d2d2d;
                alternate-background-color: #353535;
                color: #ffffff;
                border: 1px solid #555555;
            }
            
            QTableWidget::item:selected {
                background-color: #ff9900;
                color: #000000;
            }
            
            /* Headers */
            QHeaderView::section {
                background-color: #1a1a1a;
                color: #ffffff;
                padding: 5px;
                border: 1px solid #555555;
            }
            
            /* Tabs */
            QTabWidget::pane {
                border: 1px solid #555555;
            }
            
            QTabBar::tab {
                background-color: #2d2d2d;
                color: #ffffff;
                padding: 8px 12px;
                border: 1px solid #555555;
                border-bottom: none;
            }
            
            QTabBar::tab:selected {
                background-color: #ff9900;
                color: #000000;
            }
            
            /* Buttons */
            QPushButton {
                background-color: #444444;
                color: #ffffff;
                border: 1px solid #666666;
                padding: 5px;
                border-radius: 3px;
            }
            
            QPushButton:hover {
                background-color: #555555;
            }
            
            QPushButton:pressed {
                background-color: #ff9900;
                color: #000000;
            }
            
            /* Input fields */
            QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox {
                background-color: #444444;
                color: #ffffff;
                border: 1px solid #666666;
                padding: 3px;
            }
        """)

        # Update checkbox styling to have lighter borders
        self.setStyleSheet(self.styleSheet() + """
            QCheckBox {
                color: #e0e0e0;
            }
            QCheckBox::indicator {
                width: 13px;
                height: 13px;
                border: 1px solid #a0a0a0; /* Lighter border color (was likely darker) */
                background: #2d2d2d;
            }
            QCheckBox::indicator:checked {
                background-color: darkorange;
                border: 1px solid #a0a0a0; /* Lighter border color */
            }
           
        """)
        

    def set_gui_cpu_affinity(self):
        """Set CPU affinity for GUI process to an isolated core
        
        CPU core allocation:
        - Cores 0-9: EXCHANGE (Binance TR) C++ WebSocket clients (10 cores)
        - Cores 10-13: Binance CS C++ WebSocket clients (4 cores)
        - Cores 14-23: Python arbitrage scripts (10 scripts)
        - Cores 24+: GUI, OS, Redis, monitoring, and other system processes
        """
        try:
            import os
            import psutil
            
            # Use core 24 for GUI (first available after all trading processes)
            # If system has fewer cores, use the last available core
            gui_cpu_core = 24
            
            # Get current process
            current_process = psutil.Process()
            
            # Check if the requested core exists
            available_cores = list(range(psutil.cpu_count()))
            if gui_cpu_core not in available_cores:
                # Fall back to last available core (should be after cores 0-23)
                if len(available_cores) > 23:
                    gui_cpu_core = available_cores[-1]  # Use last core
                elif len(available_cores) > 13:
                    # System has more than 13 cores but less than 24, use last available
                    gui_cpu_core = available_cores[-1]
                else:
                    # System has fewer cores, use the last one
                    gui_cpu_core = available_cores[-1] if available_cores else 0
                logging.warning(f"Requested CPU core 24 not available. Using core {gui_cpu_core} instead.")
            
            # Set CPU affinity
            current_process.cpu_affinity([gui_cpu_core])
            
            logging.info(f"GUI process (PID: {os.getpid()}) pinned to CPU Core {gui_cpu_core}")
            logging.info(f"  Note: GUI isolated from trading processes (cores 0-23: C++ clients + Python scripts)")
            
            # Verify the affinity was set correctly
            p = psutil.Process(os.getpid())
            actual_core = p.cpu_num()
            if actual_core == gui_cpu_core:
                logging.info(f"✓ GUI CPU affinity verified: running on Core {actual_core}")
            else:
                logging.warning(f"GUI CPU affinity mismatch: requested Core {gui_cpu_core}, actual Core {actual_core}")
            
        except ImportError:
            logging.warning("psutil not available, GUI CPU affinity not set. Install with: pip install psutil")
        except Exception as e:
            logging.warning(f"Error setting GUI CPU affinity: {e} (GUI will continue without CPU affinity)")

    def closeEvent(self, event):
        # Stop threads when closing the application
        self.refresh_thread.stop()
        self.trade_history_thread.stop()
        
        # Wait for threads to finish
        self.refresh_thread.wait()
        self.trade_history_thread.wait()
        event.accept()
        
        # Note: C++ processes and arbitrage scripts are NOT stopped when closing GUI
        # They should be stopped using the STOP button in the GUI before closing
        
    def toggle_system(self):
        arbitrage_state = self.redis.get('maker_arbitrage_state')
        
        if arbitrage_state == b'stopped' or arbitrage_state is None:
            # Kill all related processes before starting
            self.kill_all_related_processes()
            
            self.redis.set('maker_arbitrage_state', 'starting')
            self.system_control_btn.setText("Starting...")
            self.system_control_btn.setEnabled(False)
            logging.info("System starting... checking for C++ WebSocket optimization...")
            
            # Check if C++ optimization is enabled and start C++ client if needed
            self.start_cpp_websocket_if_needed()
            
            # Small delay to let C++ client initialize
            time.sleep(2)
            
            logging.info("Starting arbit_core.py in new terminal window")
            self.start_arbitrage_local()    
        elif arbitrage_state == b'running':
            self.redis.set('maker_arbitrage_state', 'stopping')
            self.system_control_btn.setText("Stopping...")
            self.system_control_btn.setEnabled(False)
            self.redis.publish('arbit_commands', b'stop')
            logging.info("Arbit commands: stop sent to the redis --> arbit_core.py")
            
            # Stop C++ WebSocket services when stopping the system
            print("\n" + "=" * 80)
            print("STOPPING SYSTEM - Stopping C++ WebSocket services")
            print("=" * 80 + "\n")
            logging.info("=" * 80)
            logging.info("STOPPING SYSTEM - Stopping C++ WebSocket services")
            logging.info("=" * 80)
            self.stop_cpp_services()
    
    def toggle_orders(self):
        """Toggle the orders state between enabled and disabled"""
        try:
            current_state = self.redis.get('maker_enable_orders')
            
            if current_state == b'1':
                # Send disable orders command to Redis
                self.redis.publish('arbit_commands', b'disable_orders')
                logging.info("Orders disabled command sent to Redis")
            elif current_state == b'0':
                # Send enable orders command to Redis
                self.redis.publish('arbit_commands', b'enable_orders')
                logging.info("Orders enabled command sent to Redis")
                
        except Exception as e:
            logging.error(f"Error sending enable/disable orders command to Redis: {e}")

    def start_cpp_websocket_if_needed(self):
        """Start C++ WebSocket clients if USE_CPP_OPTIMIZATION is enabled"""
        try:
            # Read collocation value from Redis at the start
            try:
                collocation_value = self.redis.get('collocation')
                if collocation_value is None:
                    collocation_value = 0
                else:
                    collocation_value = int(collocation_value)
                logging.info(f"GUI: Read collocation value from Redis: {collocation_value}")
            except Exception as e:
                logging.warning(f"Could not read collocation from Redis: {e}, defaulting to 0")
                collocation_value = 0
            
            # Get the current file's directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            core_script = os.path.join(current_dir, "arbit_core_maker_BTR_1.py")
            
            # Check if USE_CPP_OPTIMIZATION is enabled
            try:
                with open(core_script, 'r') as f:
                    content = f.read()
                    
                    # Always start EXCHANGE C++ WebSocket client (Binance TR)
                    logging.info("Starting EXCHANGE C++ WebSocket client (Binance TR)...")
                    
                    # Path to the startup script
                    cpp_startup_script = os.path.join(current_dir, "start_binance_tr_websocket.sh")
                    
                    if os.path.exists(cpp_startup_script):
                        # Make sure it's executable
                        os.chmod(cpp_startup_script, 0o755)
                        
                        # Start C++ WebSocket client in a new terminal window
                        # Keep terminal open after process exits (same behavior as Python scripts)
                        cmd_command = (
                            f'gnome-terminal --title="EXCHANGE C++ WebSocket Client (Binance TR)" -- bash -c "'
                            f'cd "{current_dir}" && '
                            f'"{cpp_startup_script}"; '
                            f'exec bash"'
                        )
                        
                        logging.info(f"Starting EXCHANGE C++ WebSocket client: {cmd_command}")
                        
                        # Execute command in a new terminal window
                        self.cpp_EXCHANGE_process = subprocess.Popen(
                            cmd_command,
                            shell=True
                        )
                        
                        logging.info("EXCHANGE C++ WebSocket client started in new terminal window")
                        logging.info("Waiting 2 seconds for EXCHANGE C++ client to initialize...")
                        time.sleep(2)  # Give C++ client time to start
                    else:
                        logging.error(f"EXCHANGE C++ startup script not found: {cpp_startup_script}")
                        logging.error("Cannot start EXCHANGE C++ WebSocket client!")
                    
                    # Start Binance CS C++ WebSocket client
                    # Always start binance_cs_ws_client (Binance CS C++ client)
                    logging.info("Starting Binance CS C++ WebSocket client...")
                    
                    # Path to the Binance CS startup script
                    binance_cs_startup_script = os.path.join(current_dir, "start_binance_websocket.sh")
                    
                    if os.path.exists(binance_cs_startup_script):
                        # Make sure it's executable
                        os.chmod(binance_cs_startup_script, 0o755)
                        
                        # Start Binance CS C++ WebSocket client in a new terminal window
                        cmd_command = (
                            f'gnome-terminal --title="Binance CS C++ WebSocket Client" -- bash -c "'
                            f'cd "{current_dir}" && '
                            f'"{binance_cs_startup_script}"; '
                            f'exec bash"'
                        )
                        
                        logging.info(f"Starting Binance CS C++ WebSocket client: {cmd_command}")
                        
                        # Execute command in a new terminal window
                        self.cpp_binance_cs_process = subprocess.Popen(
                            cmd_command,
                            shell=True
                        )
                        
                        logging.info("Binance CS C++ WebSocket client started in new terminal window")
                        logging.info("Waiting 3 seconds for Binance CS C++ client to initialize...")
                        time.sleep(3)  # Give C++ client time to start
                    else:
                        logging.warning(f"Binance CS C++ startup script not found: {binance_cs_startup_script}")
                        logging.warning("Falling back to regular Binance WebSocket connection")
                    
                    # Additional Binance C++ WebSocket client for collocation mode (if collocation == 1)
                    # CRITICAL: Only start if collocation == 1 (if collocation == 0, use standard binance_websocket_ticker)
                    if collocation_value == 0:
                        logging.info(f"GUI: collocation={collocation_value} - Additional Binance C++ client will NOT start (using standard binance_websocket_ticker)")
                    elif collocation_value == 1:
                        logging.info("C++ WebSocket optimization for Binance is enabled, starting Binance C++ clients...")
                        
                        # Check healthy CS hosts before starting clients
                        try:
                            import CS_host_health_distribution as CS_health
                            CS_INSTRUMENTS_MAP = arbit_config_maker.cs_instruments_map
                            
                            if CS_INSTRUMENTS_MAP:
                                # Get all available hosts
                                all_hosts = CS_health.get_all_available_hosts(CS_INSTRUMENTS_MAP)
                                logging.info(f"GUI: Found {len(all_hosts)} available CS host(s): {all_hosts}")
                                
                                # Perform health checks
                                import asyncio
                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                                healthy_hosts, unhealthy_hosts = loop.run_until_complete(
                                    CS_health.check_hosts_health(all_hosts, timeout=5.0)
                                )
                                loop.close()
                                
                                if not healthy_hosts:
                                    logging.error("GUI: No healthy CS hosts found! Cannot start Binance C++ clients.")
                                    logging.warning("Falling back to regular Binance WebSocket connection")
                                else:
                                    # Use first 2 healthy hosts (or all if less than 2)
                                    hosts_to_use = healthy_hosts[:2]
                                    logging.info(f"GUI: Selected {len(hosts_to_use)} healthy host(s) for redundancy: {hosts_to_use}")
                                    
                                    # Update instruments file with healthy hosts
                                    instruments_file = os.path.join(current_dir, "binance_websocket_instruments.json")
                                    if os.path.exists(instruments_file):
                                        try:
                                            with open(instruments_file, 'r') as f:
                                                instruments_data = json.load(f)
                                            instruments_data["hosts"] = hosts_to_use
                                            with open(instruments_file, 'w') as f:
                                                json.dump(instruments_data, f, indent=2)
                                            logging.info(f"GUI: Updated {instruments_file} with healthy hosts: {hosts_to_use}")
                                        except Exception as e:
                                            logging.warning(f"GUI: Could not update instruments file with healthy hosts: {e}")
                                    
                                    # Path to the Binance startup script
                                    binance_cpp_startup_script = os.path.join(current_dir, "start_binance_websocket.sh")
                                    
                                    if os.path.exists(binance_cpp_startup_script):
                                        # Make sure it's executable
                                        os.chmod(binance_cpp_startup_script, 0o755)
                                        
                                        # Start Binance C++ WebSocket clients in a new terminal window
                                        # Both clients will connect to different healthy hosts and subscribe to all symbols
                                        # Both will write to the same shared memory for redundancy
                                        cmd_command = (
                                            f'gnome-terminal --title="Binance C++ WebSocket Clients (Redundancy)" -- bash -c "'
                                            f'cd "{current_dir}" && '
                                            f'"{binance_cpp_startup_script}"; '
                                            f'exec bash"'
                                        )
                                        
                                        logging.info(f"Starting Binance C++ WebSocket clients (redundancy mode): {cmd_command}")
                                        
                                        # Execute command in a new terminal window
                                        self.cpp_binance_process = subprocess.Popen(
                                            cmd_command,
                                            shell=True
                                        )
                                        
                                        logging.info(f"Binance C++ WebSocket clients started in new terminal window")
                                        logging.info(f"  - {len(hosts_to_use)} client(s) will connect to: {hosts_to_use}")
                                        logging.info(f"  - Both clients will subscribe to ALL symbols")
                                        logging.info(f"  - Both clients will write to the same shared memory for redundancy")
                                        logging.info("Waiting 3 seconds for Binance C++ clients to initialize...")
                                        time.sleep(3)  # Give C++ clients time to start
                                    else:
                                        logging.warning(f"Binance C++ startup script not found: {binance_cpp_startup_script}")
                                        logging.warning("Falling back to regular Binance WebSocket connection")
                            else:
                                logging.warning("GUI: CS_INSTRUMENTS_MAP is empty, cannot check healthy hosts")
                                logging.warning("Falling back to regular Binance WebSocket connection")
                        except Exception as e:
                            logging.warning(f"GUI: Error checking healthy CS hosts: {e}")
                            import traceback
                            logging.warning(traceback.format_exc())
                            logging.warning("Falling back to regular Binance WebSocket connection")
            except Exception as e:
                logging.warning(f"Could not check USE_CPP_OPTIMIZATION setting: {e}")
                logging.warning("Proceeding with regular WebSocket connection")
        except Exception as e:
            logging.error(f"Error starting C++ WebSocket clients: {e}")
            logging.warning("Proceeding with regular WebSocket connection")

    def kill_all_related_processes(self):
        """Kill all arbit_core_maker_HFT processes and C++ client processes"""
        try:
            logging.info("=" * 80)
            logging.info("KILLING ALL RELATED PROCESSES")
            logging.info("=" * 80)
            print("Killing all related processes...")
            
            # Patterns to kill
            patterns_to_kill = [
                'arbit_core_maker_BTR',  # All arbit_core_maker_BTR scripts (1, 2, 3, 4, etc.)
                'EXCHANGE_ws_client',  # EXCHANGE C++ executable
                'binance_cs_ws_client',  # Binance CS C++ executable
                'binance_tr_ws_client'  # Binance TR C++ executable (if exists)
            ]
            
            # Step 1: Send SIGTERM (graceful shutdown signal)
            print("Step 1: Sending SIGTERM (graceful shutdown signal)...")
            logging.info("Step 1: Sending SIGTERM (graceful shutdown signal)...")
            for pattern in patterns_to_kill:
                try:
                    result = subprocess.run(['pkill', '-TERM', '-f', pattern], timeout=2, check=False, capture_output=True)
                    if result.returncode == 0:
                        print(f"  Sent SIGTERM to processes matching: {pattern}")
                        logging.info(f"  Sent SIGTERM to processes matching: {pattern}")
                except Exception as e:
                    logging.debug(f"Error sending SIGTERM for {pattern}: {e}")
            
            # Step 2: Wait for graceful shutdown
            print("Step 2: Waiting 2 seconds for graceful shutdown...")
            logging.info("Step 2: Waiting 2 seconds for graceful shutdown...")
            time.sleep(2)
            
            # Step 3: Force kill any remaining processes
            print("Step 3: Force killing any remaining processes...")
            logging.info("Step 3: Force killing any remaining processes...")
            for pattern in patterns_to_kill:
                try:
                    result = subprocess.run(['pkill', '-9', '-f', pattern], timeout=2, check=False, capture_output=True)
                    if result.returncode == 0:
                        print(f"  Force killed processes matching: {pattern}")
                        logging.info(f"  Force killed processes matching: {pattern}")
                except Exception as e:
                    logging.debug(f"Error force killing {pattern}: {e}")
            
            print("All related processes killed!")
            logging.info("All related processes killed!")
            
        except Exception as e:
            logging.error(f"Error killing related processes: {e}")
            print(f"Error killing related processes: {e}")

    def stop_cpp_services(self):
        """Stop all C++ WebSocket services and arbitrage scripts on shutdown"""
        print("=" * 80)
        print("STOPPING ALL SERVICES - stop_cpp_services() CALLED")
        print("=" * 80)
        try:
            logging.info("=" * 80)
            logging.info("STOPPING ALL SERVICES - stop_cpp_services() CALLED")
            logging.info("=" * 80)
            print("Stopping all services...")
            
            # Send stop command to all arbitrage scripts via Redis (they will shutdown gracefully themselves)
            try:
                self.redis.publish('arbit_commands', b'stop')
                print("Sent stop command to all arbitrage scripts via Redis")
                logging.info("Sent stop command to all arbitrage scripts via Redis")
                # Scripts will handle their own shutdown gracefully - no need to kill them
            except Exception as e:
                print(f"Error sending stop command via Redis: {e}")
                logging.warning(f"Error sending stop command via Redis: {e}")
            
            # Note: Python scripts will shutdown gracefully themselves after receiving Redis stop command
            # No need to kill them - they handle their own cleanup
            
            # Stop EXCHANGE C++ process if we have a reference
            if self.cpp_EXCHANGE_process is not None:
                try:
                    logging.info("Terminating EXCHANGE C++ WebSocket process...")
                    self.cpp_EXCHANGE_process.terminate()
                    try:
                        self.cpp_EXCHANGE_process.wait(timeout=2)
                        logging.info("EXCHANGE C++ WebSocket process terminated gracefully")
                    except subprocess.TimeoutExpired:
                        logging.warning("EXCHANGE C++ WebSocket process did not terminate, forcing kill...")
                        self.cpp_EXCHANGE_process.kill()
                        self.cpp_EXCHANGE_process.wait()
                        logging.info("EXCHANGE C++ WebSocket process killed")
                except Exception as e:
                    logging.error(f"Error stopping EXCHANGE C++ process: {e}")
            
            # Stop Binance C++ process if we have a reference
            if self.cpp_binance_process is not None:
                try:
                    logging.info("Terminating Binance C++ WebSocket process...")
                    self.cpp_binance_process.terminate()
                    try:
                        self.cpp_binance_process.wait(timeout=2)
                        logging.info("Binance C++ WebSocket process terminated gracefully")
                    except subprocess.TimeoutExpired:
                        logging.warning("Binance C++ WebSocket process did not terminate, forcing kill...")
                        self.cpp_binance_process.kill()
                        self.cpp_binance_process.wait()
                        logging.info("Binance C++ WebSocket process killed")
                except Exception as e:
                    logging.error(f"Error stopping Binance C++ process: {e}")
            
            # Stop Binance CS C++ process if we have a reference
            if self.cpp_binance_cs_process is not None:
                try:
                    logging.info("Terminating Binance CS C++ WebSocket process...")
                    self.cpp_binance_cs_process.terminate()
                    try:
                        self.cpp_binance_cs_process.wait(timeout=2)
                        logging.info("Binance CS C++ WebSocket process terminated gracefully")
                    except subprocess.TimeoutExpired:
                        logging.warning("Binance CS C++ WebSocket process did not terminate, forcing kill...")
                        self.cpp_binance_cs_process.kill()
                        self.cpp_binance_cs_process.wait()
                        logging.info("Binance CS C++ WebSocket process killed")
                except Exception as e:
                    logging.error(f"Error stopping Binance CS C++ process: {e}")
            
            # Note: Python scripts handle their own shutdown via Redis stop command
            # No need to kill them - they will exit gracefully
            
            # Kill all C++ websocket processes - graceful shutdown first (like Ctrl+C), then force if needed
            try:
                print("Attempting to gracefully shutdown C++ WebSocket processes (sending SIGTERM like Ctrl+C)...")
                logging.info("Attempting to gracefully shutdown C++ WebSocket processes (sending SIGTERM like Ctrl+C)...")
                
                # Method 1: Graceful shutdown - send SIGTERM to all C++ processes (like Ctrl+C)
                # Note: We only kill the actual C++ executables, NOT the terminal windows
                # Terminal windows will remain open showing the exit message
                patterns_to_kill = [
                    'EXCHANGE_ws_client',  # EXCHANGE C++ executable (Binance TR)
                    'binance_cs_ws_client',  # Binance CS C++ executable
                    'binance_tr_ws_client'  # Binance TR C++ executable (if exists)
                    # Note: We don't kill terminal processes - they should stay open
                ]
                
                # Step 1: Send SIGTERM (graceful shutdown signal, like Ctrl+C)
                print("Step 1: Sending SIGTERM (graceful shutdown signal, like Ctrl+C)...")
                logging.info("Step 1: Sending SIGTERM (graceful shutdown signal, like Ctrl+C)...")
                for pattern in patterns_to_kill:
                    try:
                        # Send SIGTERM to all processes matching the pattern (including child processes)
                        result = subprocess.run(['pkill', '-TERM', '-f', pattern], timeout=2, check=False, capture_output=True)
                        if result.returncode == 0:
                            print(f"  Sent SIGTERM to processes matching: {pattern}")
                            logging.info(f"  Sent SIGTERM to processes matching: {pattern}")
                        else:
                            # Check if process exists
                            check_result = subprocess.run(['pgrep', '-f', pattern], timeout=2, check=False, capture_output=True)
                            if check_result.returncode == 0:
                                print(f"  Warning: Could not send SIGTERM to {pattern}, but process exists")
                                logging.warning(f"  Warning: Could not send SIGTERM to {pattern}, but process exists")
                    except Exception as e:
                        logging.debug(f"Error sending SIGTERM for {pattern}: {e}")
                
                # Step 2: Wait for graceful shutdown (give processes time to clean up)
                # Longer wait for C++ processes to handle cleanup properly
                print("Step 2: Waiting 3 seconds for graceful shutdown...")
                logging.info("Step 2: Waiting 3 seconds for graceful shutdown...")
                time.sleep(3)
                
                # Step 3: Check if processes are still running
                print("Step 3: Checking if C++ WebSocket processes are still running...")
                logging.info("Step 3: Checking if C++ WebSocket processes are still running...")
                still_running = []
                for pattern in patterns_to_kill:
                    try:
                        result = subprocess.run(['pgrep', '-f', pattern], timeout=2, check=False, capture_output=True, text=True)
                        if result.returncode == 0 and result.stdout.strip():
                            pids = [pid.strip() for pid in result.stdout.strip().split('\n') if pid.strip()]
                            if pids:
                                still_running.append((pattern, pids))
                                print(f"  Found {len(pids)} process(es) still running for {pattern} (PIDs: {', '.join(pids)})")
                                logging.warning(f"  Found {len(pids)} process(es) still running for {pattern} (PIDs: {', '.join(pids)})")
                    except Exception as e:
                        logging.debug(f"Error checking processes for {pattern}: {e}")
                
                if still_running:
                    print(f"Step 4: Some processes still running, sending SIGKILL (force kill)...")
                    logging.warning(f"Step 4: Some processes still running, sending SIGKILL (force kill)...")
                    for pattern, pids in still_running:
                        print(f"  Force killing {len(pids)} process(es) matching {pattern} (PIDs: {', '.join(pids)})")
                        logging.warning(f"  Force killing {len(pids)} process(es) matching {pattern} (PIDs: {', '.join(pids)})")
                        try:
                            # Force kill with SIGKILL
                            subprocess.run(['pkill', '-9', '-f', pattern], timeout=2, check=False)
                            # Also kill child processes
                            for pid in pids:
                                try:
                                    subprocess.run(['pkill', '-9', '-P', pid], timeout=1, check=False)
                                except:
                                    pass
                        except Exception as e:
                            logging.error(f"Error force killing {pattern}: {e}")
                    time.sleep(1)
                    print("Step 5: Force kill completed")
                    logging.info("Step 5: Force kill completed")
                else:
                    print("Step 4: All C++ WebSocket processes terminated gracefully!")
                    logging.info("Step 4: All C++ WebSocket processes terminated gracefully!")
                    logging.info("Step 4: All processes terminated gracefully!")
                
                time.sleep(1)
                
                # Method 2: Fallback - Find any remaining processes by executable path and kill gracefully
                current_dir = os.path.dirname(os.path.abspath(__file__))
                executable_paths = [
                    os.path.join(current_dir, 'build', 'websocket_shm_client'),
                    os.path.join(current_dir, 'build_binance', 'websocket_shm_client'),
                    os.path.join(current_dir, 'build_binance', 'binance_websocket_shm_client'),
                ]
                
                remaining_found = False
                for exec_path in executable_paths:
                    if os.path.exists(exec_path):
                        exec_name = os.path.basename(exec_path)
                        try:
                            # Find PIDs using the executable name
                            result = subprocess.run(
                                ['pgrep', '-f', exec_name],
                                capture_output=True,
                                text=True,
                                timeout=2
                            )
                            if result.returncode == 0 and result.stdout.strip():
                                pids = [pid.strip() for pid in result.stdout.strip().split('\n') if pid.strip()]
                                if pids:
                                    remaining_found = True
                                    print(f"  Found {len(pids)} remaining process(es) for {exec_name} (PIDs: {', '.join(pids)})")
                                    logging.warning(f"  Found {len(pids)} remaining process(es) for {exec_name} (PIDs: {', '.join(pids)})")
                                    # Try graceful kill first
                                    for pid in pids:
                                        try:
                                            subprocess.run(['kill', '-TERM', pid], timeout=1, check=False)
                                        except:
                                            pass
                                    time.sleep(6)  # Give more time for graceful shutdown (especially for Binance to avoid deadlock)
                                    # Force kill if still running
                                    for pid in pids:
                                        try:
                                            check = subprocess.run(['ps', '-p', pid], capture_output=True, timeout=1)
                                            if check.returncode == 0:
                                                print(f"    Process {pid} still running, force killing...")
                                                logging.warning(f"    Process {pid} still running, force killing...")
                                                subprocess.run(['kill', '-KILL', pid], timeout=1, check=False)
                                        except:
                                            pass
                        except:
                            pass
                
                # Method 3: Final cleanup - use killall only if processes still exist
                if remaining_found:
                    time.sleep(1)
                    print("  Attempting final cleanup with killall...")
                    logging.info("  Attempting final cleanup with killall...")
                    try:
                        # Try graceful first
                        subprocess.run(['killall', '-TERM', 'websocket_shm_client'], timeout=2, check=False)
                        subprocess.run(['killall', '-TERM', 'binance_websocket_shm_client'], timeout=2, check=False)
                        time.sleep(6)  # Wait for graceful shutdown (especially for Binance to avoid deadlock)
                        # Force kill as last resort
                        subprocess.run(['killall', '-KILL', 'websocket_shm_client'], timeout=2, check=False)
                        subprocess.run(['killall', '-KILL', 'binance_websocket_shm_client'], timeout=2, check=False)
                    except:
                        pass
                
                # Final verification - wait a bit and check
                time.sleep(1)
                final_check = subprocess.run(
                    ['pgrep', '-f', 'websocket_shm_client|binance_websocket_shm_client'],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                if final_check.returncode == 0 and final_check.stdout.strip():
                    remaining = final_check.stdout.strip().split('\n')
                    warning_msg = f"WARNING: Some C++ processes may still be running (PIDs: {', '.join(remaining)})"
                    print(warning_msg)
                    logging.warning(warning_msg)
                    print("You may need to manually close the terminal windows with Ctrl+C")
                    logging.warning("You may need to manually close the terminal windows with Ctrl+C")
                else:
                    success_msg = "All C++ WebSocket processes cleanup completed"
                    print(success_msg)
                    logging.info(success_msg)
                        
            except Exception as e:
                error_msg = f"Error in C++ process cleanup: {e}"
                print(error_msg)
                logging.error(error_msg)
                import traceback
                tb = traceback.format_exc()
                print(tb)
                logging.error(tb)
            
            completion_msg = "All services cleanup completed"
            print(completion_msg)
            logging.info(completion_msg)
            
        except Exception as e:
            error_msg = f"Error stopping services: {e}"
            print(error_msg)
            logging.error(error_msg)
            import traceback
            tb = traceback.format_exc()
            print(tb)
            logging.error(tb)

    def export_redis_to_csv(self, key_name):
        """
        Connects to Redis, fetches data from specified lists, 
        and exports each list to a separate CSV file.
        """
        try:
            start_time = time.time()
        
            # Connect to local Redis
            export_redis_connection = redis.Redis(host='localhost', port=6379)

            # Test connection
            export_redis_connection.ping()
            logging.info("Successfully connected to Redis for export BINANCE & EXCHANGE trade history.")

            if key_name == "EXCHANGE":
                key_name = "maker_EXCHANGE_trade_history"
            elif key_name == "BINANCE":
                key_name = "maker_BINANCE_trade_history"
            elif key_name == "MOVE":
                key_name = "maker_move_history"
            else:
                logging.error(f"Invalid key name: {key_name}")
                return
            logging.info(f"Processing key: '{key_name}'...")

            # Get ALL data from Redis list (from 0 to -1)
            redis_data = export_redis_connection.lrange(key_name, 0, -1)

            if not redis_data:
                logging.warning(f"No data found for key '{key_name}'. Skipping.")
                return

            # Convert each JSON string from Redis to Python dictionary
            data_list = [json.loads(record) for record in redis_data]
            
            # Convert data list to Pandas DataFrame
            df = pd.DataFrame(data_list)

            time_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Determine output filename
            #first check if there is a folder called trade_histories
            if not os.path.exists("trade_histories"):
                os.makedirs("trade_histories")
            output_filename = f"trade_histories/{key_name}_{time_tag}.csv"
            
            # Save DataFrame to CSV file
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            end_time = time.time()
            
            logging.info(f"SUCCESS! Exported {len(df)} records from '{key_name}' to '{output_filename}' in {end_time - start_time} seconds")
            self.statusBar().showMessage(f"Exported {len(df)} records from '{key_name}' to '{output_filename}' in {end_time - start_time} seconds", 3000)

        except redis.exceptions.ConnectionError as e:
            logging.error(f"Could not connect to Redis: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)

    def clear_EXCHANGE_trade_history(self):
        """Clear EXCHANGE trade history from Redis and update GUI table"""
        try:
            # First export history to csv then clear
            self.export_redis_to_csv("EXCHANGE")

            # Clear data from Redis
            self.redis.delete('maker_EXCHANGE_trade_history')
            
            # Clear GUI table
            self.EXCHANGE_trade_table.setRowCount(0)
            
            # Reset highlighted rows
            self.EXCHANGE_highlighted_rows = {}
            
            # Reset last trade count
            self.last_EXCHANGE_trade_count = 0

            # Reset trade page
            global EXCHANGE_history_page_no
            EXCHANGE_history_page_no = 1
            
            # Show success message
            QMessageBox.information(self, "Success", "EXCHANGE trade history cleared")
            self.statusBar().showMessage("EXCHANGE trade history cleared")
            
        except Exception as e:
            logging.error(f"Error clearing EXCHANGE trade history: {e}")
            QMessageBox.warning(self, "Error", f"Failed to clear EXCHANGE trade history: {e}")

    def clear_binance_trade_history(self):
        """Clear BINANCE trade history from Redis and update GUI table"""
        try:
            # First export history to csv then clear
            self.export_redis_to_csv("BINANCE")
            
            # Clear data from Redis
            self.redis.delete('maker_BINANCE_trade_history')
            
            # Clear GUI table
            self.binance_trade_table.setRowCount(0)
            
            # Reset highlighted rows
            self.binance_highlighted_rows = {}
            
            # Reset last trade count
            self.last_binance_trade_count = 0

            # Reset trade page
            global binance_history_page_no
            binance_history_page_no = 1
            
            # Show success message
            QMessageBox.information(self, "Success", "BINANCE trade history cleared")
            self.statusBar().showMessage("BINANCE trade history cleared")
            
        except Exception as e:
            logging.error(f"Error clearing BINANCE trade history: {e}")
            QMessageBox.warning(self, "Error", f"Failed to clear BINANCE trade history: {e}")

    def clear_move_history(self):
        """Clear Move History from Redis and update GUI table"""
        try:
            # First export history to csv then clear
            self.export_redis_to_csv("MOVE")
            
            # Clear data from Redis
            self.redis.delete('maker_move_history')
            
            # Clear GUI table
            self.move_history_table.setRowCount(0)
            
            # Reset move history page
            global move_history_page_no
            move_history_page_no = 1
            move_history_page_size = self.redis.llen('maker_move_history') // 100
            self.move_history_page_label.setText(f"Page {move_history_page_no} of {move_history_page_size}")
            
            # Show success message
            QMessageBox.information(self, "Success", "Move history cleared")
            self.statusBar().showMessage("Move history cleared")
            
        except Exception as e:
            logging.error(f"Error clearing Move history: {e}")
            QMessageBox.warning(self, "Error", f"Failed to clear Move history: {e}")

    def clear_trade_history(self):
        """Clear trade history from Redis and update GUI tables"""
        # Clear data from Redis
        self.redis.delete('maker_BINANCE_trade_history')
        self.redis.delete('maker_EXCHANGE_trade_history')
        self.redis.delete('maker_move_history')
        
        # Clear GUI tables
        self.EXCHANGE_trade_table.setRowCount(0)
        self.binance_trade_table.setRowCount(0)
        self.move_history_table.setRowCount(0)
        
        # Reset highlighted rows
        self.EXCHANGE_highlighted_rows = {}
        self.binance_highlighted_rows = {}
        
        # Reset last trade counts
        self.last_EXCHANGE_trade_count = 0
        self.last_binance_trade_count = 0

        # Reset trade pages
        global EXCHANGE_history_page_no
        global binance_history_page_no

        EXCHANGE_history_page_no = 1
        binance_history_page_no = 1
        
        # Show success message
        QMessageBox.information(self, "Success", "Trade history cleared")
        self.statusBar().showMessage("Trade history cleared")

    def update_balances(self):
        """Send command to Redis to update balances for all account types"""
        try:

            logging.info(f"Account type: {self.account_type}")

            self.EXCHANGE_MAKER_balances = arbit_config_maker.fetch_EXCHANGE_balance(self.account_type)

            self.redis.publish('arbit_commands', b'update_balances')
           
            self.statusBar().showMessage(f"{self.account_type} balances updated! @ " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 3000)
            
        except Exception as e:
            logging.error(f"Error sending balance update command: {e}")
            self.statusBar().showMessage(f"Error: {str(e)}", 3000)

    def cancel_orders(self):
        """Send command to Redis to cancel all open orders"""
        try:
            # Send cancel all orders command to Redis
            self.redis.publish('arbit_commands', b'cancel_all_orders')
            logging.info("Cancel all orders command sent to Redis")
            self.statusBar().showMessage("Cancel all orders command sent! @ " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 3000)
            
        except Exception as e:
            logging.error(f"Error sending cancel all orders command to Redis: {e}")
            self.statusBar().showMessage(f"Error: {str(e)}", 3000)

    def start_arbitrage_local(self):
        """Start the distributed arbitrage system locally"""
        try:
            # Get the current file's directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            # Try .venv first, then venv, then system python3
            venv_python = os.path.join(current_dir, ".venv", "bin", "python")
            if not os.path.exists(venv_python):
                venv_python = os.path.join(current_dir, "venv", "bin", "python")
            if not os.path.exists(venv_python):
                venv_python = "/usr/bin/python3"  # Fallback to system python3
            
            # Define all script paths (all 10 arbitrage core scripts)
            script_paths = [
                os.path.join(current_dir, "arbit_core_maker_BTR_1.py"),
                os.path.join(current_dir, "arbit_core_maker_BTR_2.py"),
                os.path.join(current_dir, "arbit_core_maker_BTR_3.py"),
                os.path.join(current_dir, "arbit_core_maker_BTR_4.py"),
                os.path.join(current_dir, "arbit_core_maker_BTR_5.py"),
                os.path.join(current_dir, "arbit_core_maker_BTR_6.py"),
                os.path.join(current_dir, "arbit_core_maker_BTR_7.py"),
                os.path.join(current_dir, "arbit_core_maker_BTR_8.py"),
                os.path.join(current_dir, "arbit_core_maker_BTR_9.py"),
                os.path.join(current_dir, "arbit_core_maker_BTR_10.py"),
            ]
            
            # Log the paths
            logging.info(f"Current directory: {current_dir}")
            logging.info(f"Python path: {venv_python}")
            
            # Start each script in a separate terminal window
            for i, script_path in enumerate(script_paths, 1):
                if os.path.exists(script_path):
                    # Command to open a new terminal window and run the script
                    # Keep terminal open after script exits (same behavior as C++ terminals)
                    cmd_command = (
                        f'gnome-terminal --title="Arbitrage Script {i}" -- bash -c "'
                        f'cd "{current_dir}" && '
                        f'"{venv_python}" "{script_path}"; '
                        f'exec bash"'
                    )
                    
                    logging.info(f"Starting Script {i}: {cmd_command}")
                    
                    # Execute command in a new terminal window
                    process = subprocess.Popen(
                        cmd_command,
                        shell=True
                    )
                    
                    # Store process reference for cleanup
                    self.arbitrage_processes.append(process)
                    
                    logging.info(f"Started Script {i} in new terminal window")
                    
                    # Small delay between starting scripts to avoid overwhelming the system
                    if i < len(script_paths):
                        time.sleep(0.5)  # 500ms delay between script starts
                else:
                    logging.warning(f"Script {i} not found: {script_path}")
            
            logging.info(f"Started distributed arbitrage system with {len([s for s in script_paths if os.path.exists(s)])} scripts")
            
        except Exception as e:
            logging.error(f"Error starting arbitrage scripts: {e}")
            
    def setup_monitoring_page(self):
        """Set up the monitoring page"""
        # Create layout for monitoring page
        monitoring_layout = QVBoxLayout(self.monitoring_page)
        
        # Create main splitter to divide the page vertically
        main_splitter = QSplitter(Qt.Orientation.Vertical)
        main_splitter.setChildrenCollapsible(False)  # Prevent sections from being collapsed
        
        # Create top section splitter (horizontal)
        top_splitter = QSplitter(Qt.Orientation.Horizontal)
        top_splitter.setChildrenCollapsible(False)
        
        # Create open trade monitor table
        open_trade_widget = QWidget()
        open_trade_layout = QVBoxLayout(open_trade_widget)
        open_trade_layout.setContentsMargins(0, 0, 0, 0)
        
        # Create header with title and search bar that spans the entire width
        open_trade_header = QWidget()
        open_trade_header.setStyleSheet("background-color: #444444;")
        open_trade_header_layout = QHBoxLayout(open_trade_header)
        open_trade_header_layout.setContentsMargins(5, 5, 5, 5)
        
        # Add search functionality at the left
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white;")
        open_trade_header_layout.addWidget(search_label)
        
        self.open_trade_search_input = QLineEdit()
        self.open_trade_search_input.setPlaceholderText("Type to search...")
        self.open_trade_search_input.textChanged.connect(self.filter_open_trade_table)
        self.open_trade_search_input.textEdited.connect(self.convert_to_uppercase)
        self.open_trade_search_input.setMaximumWidth(150)
        self.open_trade_search_input.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        open_trade_header_layout.addWidget(self.open_trade_search_input)
        
        # Add clear button
        clear_button = QPushButton("Clear")
        clear_button.setMaximumWidth(60)
        clear_button.clicked.connect(lambda: self.open_trade_search_input.clear())
        clear_button.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        open_trade_header_layout.addWidget(clear_button)

        self.selected_symbols = []



        self.open_enable_orders_btn = QPushButton("Set BUY Maker")
        self.open_enable_orders_btn.clicked.connect(lambda: self.toggle_maker_type(type=1))
        open_trade_header_layout.addWidget(self.open_enable_orders_btn)

        self.open_enable_combo_orders_btn = QPushButton("Set COMBO Maker")
        self.open_enable_combo_orders_btn.clicked.connect(lambda: self.toggle_maker_type(type=13))
        open_trade_header_layout.addWidget(self.open_enable_combo_orders_btn)
        
        self.open_disable_orders_btn = QPushButton("Reset")
        self.open_disable_orders_btn.clicked.connect(lambda: self.toggle_maker_type(type=9))
        open_trade_header_layout.addWidget(self.open_disable_orders_btn)
       # Add spacer to push title to center
        open_trade_header_layout.addStretch(1)
        
        # Get symbol count from Redis
        try:
            symbols_data = self.redis.get('EXCHANGE_Symbol_List')
            symbol_count = len(json.loads(symbols_data.decode('utf-8'))) if symbols_data else 0
        except Exception as e:
            logging.error(f"Error getting symbol count: {e}")
            symbol_count = 0
        
        # Add title label in the center
        self.open_trade_label = QLabel(f"BUY MONITOR (Active: 0 Inactive: 0 Total: {symbol_count} symbols)")
        self.open_trade_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.open_trade_label.setStyleSheet("font-weight: bold; color: white; padding: 5px;")
        open_trade_header_layout.addWidget(self.open_trade_label)
        
        # Add another spacer to balance the layout
        open_trade_header_layout.addStretch(1)
        
        # Add header to open trade layout
        open_trade_layout.addWidget(open_trade_header)
        
        # Create open trade table
        self.open_trade_table = QTableWidget()
        # Create display name mapping for column headers
        open_trade_display_columns = [col.replace('binance_absolute_time_diff', 'Binance Abs Time Diff(ms)') for col in self.open_trade_columns]
        self.setup_table(self.open_trade_table, open_trade_display_columns)
        # Connect manual selection tracking
        self.open_trade_table.selectionModel().selectionChanged.connect(self.manual_track_open_selection)
        open_trade_layout.addWidget(self.open_trade_table)
        
        # Restore column order for open trade table
        self.restore_column_order(self.open_trade_table)

        # Create EXCHANGE trade history table
        EXCHANGE_trade_widget = QWidget()
        EXCHANGE_trade_layout = QVBoxLayout(EXCHANGE_trade_widget)
        EXCHANGE_trade_layout.setContentsMargins(0, 0, 0, 0)
        
        # Create header with title and pagination controls
        EXCHANGE_trade_header = QWidget()
        EXCHANGE_trade_header.setStyleSheet("background-color: #444444;")
        EXCHANGE_trade_header_layout = QHBoxLayout(EXCHANGE_trade_header)
        EXCHANGE_trade_header_layout.setContentsMargins(5, 5, 5, 5)
        
        # Add title label at the left
        EXCHANGE_trade_label = QLabel("EXCHANGE TRADE HISTORY")
        EXCHANGE_trade_label.setStyleSheet("font-weight: bold; color: white; padding: 5px;")
        EXCHANGE_trade_header_layout.addWidget(EXCHANGE_trade_label)
        
        # Add spacer to push pagination controls to the right
        EXCHANGE_trade_header_layout.addStretch(1)
        
        # Add pagination controls
        global EXCHANGE_history_page_size
        EXCHANGE_history_page_size = self.redis.llen('maker_EXCHANGE_trade_history') // 100
        self.EXCHANGE_page_label = QLabel(f"Page 1 of {EXCHANGE_history_page_size}")
        self.EXCHANGE_page_label.setStyleSheet("color: white;")
        EXCHANGE_trade_header_layout.addWidget(self.EXCHANGE_page_label)
        
        pagination_layout = QHBoxLayout()
        pagination_layout.setSpacing(2)
        
        self.EXCHANGE_prev_btn = QPushButton("◄")
        self.EXCHANGE_prev_btn.setFixedSize(30, 25)
        self.EXCHANGE_prev_btn.clicked.connect(lambda: self.change_EXCHANGE_page(-1))
        self.EXCHANGE_prev_btn.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        EXCHANGE_trade_header_layout.addWidget(self.EXCHANGE_prev_btn)
        
        self.EXCHANGE_next_btn = QPushButton("►")
        self.EXCHANGE_next_btn.setFixedSize(30, 25)
        self.EXCHANGE_next_btn.clicked.connect(lambda: self.change_EXCHANGE_page(1))
        self.EXCHANGE_next_btn.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        EXCHANGE_trade_header_layout.addWidget(self.EXCHANGE_next_btn)
        
        self.EXCHANGE_reset_btn = QPushButton("Reset")
        self.EXCHANGE_reset_btn.setFixedWidth(50)
        self.EXCHANGE_reset_btn.clicked.connect(self.reset_EXCHANGE_page)
        self.EXCHANGE_reset_btn.setStyleSheet("""
            QPushButton {
                background-color: #555555; 
                color: white; 
                border: 1px solid #666666;
            }
            QPushButton:hover {
                background-color: #777777;
                border: 1px solid #888888;
            }
        """)
        EXCHANGE_trade_header_layout.addWidget(self.EXCHANGE_reset_btn)
        
        # Add export and clear buttons for EXCHANGE
        self.EXCHANGE_export_btn = QPushButton("Export")
        self.EXCHANGE_export_btn.setFixedWidth(60)
        self.EXCHANGE_export_btn.clicked.connect(lambda: self.export_redis_to_csv("EXCHANGE"))
        self.EXCHANGE_export_btn.setStyleSheet("""
            QPushButton {
                background-color: #cc6600; 
                color: white; 
                border: 1px solid #cc6600;
            }
            QPushButton:hover {
                background-color: #ff8800;
                border: 1px solid #ff8800;
            }
        """)
        EXCHANGE_trade_header_layout.addWidget(self.EXCHANGE_export_btn)
        
        self.EXCHANGE_clear_btn = QPushButton("Clear")
        self.EXCHANGE_clear_btn.setFixedWidth(60)
        self.EXCHANGE_clear_btn.clicked.connect(self.clear_EXCHANGE_trade_history)
        self.EXCHANGE_clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #cc0000; 
                color: white; 
                border: 1px solid #cc0000;
            }
            QPushButton:hover {
                background-color: #ff0000;
                border: 1px solid #ff0000;
            }
        """)
        EXCHANGE_trade_header_layout.addWidget(self.EXCHANGE_clear_btn)
        
        # Add header to EXCHANGE trade layout
        EXCHANGE_trade_layout.addWidget(EXCHANGE_trade_header)
        
        # Create EXCHANGE trade table
        self.EXCHANGE_trade_table = QTableWidget()
        self.setup_table(self.EXCHANGE_trade_table, maker_EXCHANGE_trade_history_columns)
        EXCHANGE_trade_layout.addWidget(self.EXCHANGE_trade_table)
        
        # Restore column order for EXCHANGE trade table
        self.restore_column_order(self.EXCHANGE_trade_table)

        # Create EXCHANGE trade open orders table
        EXCHANGE_open_orders_widget = QWidget()
        EXCHANGE_open_orders_layout = QVBoxLayout(EXCHANGE_open_orders_widget)
        EXCHANGE_open_orders_layout.setContentsMargins(0, 0, 0, 0)

        # Create header with title and search bar that spans the entire width
        EXCHANGE_open_orders_header = QWidget()
        EXCHANGE_open_orders_header.setStyleSheet("background-color: #444444;")
        EXCHANGE_open_orders_header_layout = QHBoxLayout(EXCHANGE_open_orders_header)
        EXCHANGE_open_orders_header_layout.setContentsMargins(5, 5, 5, 5)

        # Add search functionality at the left
        search_label = QLabel("Search:")
        search_label.setStyleSheet("color: white;")
        EXCHANGE_open_orders_header_layout.addWidget(search_label)
        
        self.EXCHANGE_open_orders_search_input = QLineEdit()
        self.EXCHANGE_open_orders_search_input.setPlaceholderText("Type to search...")
        self.EXCHANGE_open_orders_search_input.textChanged.connect(self.filter_EXCHANGE_open_orders)
        self.EXCHANGE_open_orders_search_input.textEdited.connect(self.convert_to_uppercase)
        self.EXCHANGE_open_orders_search_input.setMaximumWidth(150)
        self.EXCHANGE_open_orders_search_input.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        EXCHANGE_open_orders_header_layout.addWidget(self.EXCHANGE_open_orders_search_input)

        # Add clear button
        clear_button = QPushButton("Clear")
        clear_button.setMaximumWidth(60)
        clear_button.clicked.connect(lambda: self.EXCHANGE_open_orders_search_input.clear())
        clear_button.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        EXCHANGE_open_orders_header_layout.addWidget(clear_button)


        # Add spacer to push title to center
        EXCHANGE_open_orders_header_layout.addStretch(1)

        # Get order count from Redis
        try:
            order_count = self.redis.llen('maker_open_orders')
        except Exception as e:
            logging.error(f"Error getting order count: {e}")
            order_count = 0
        # Add title label in the center
        self.EXCHANGE_open_orders_label = QLabel(f"EXCHANGE OPEN ORDERS ({order_count} orders)")
        self.EXCHANGE_open_orders_label.setStyleSheet("font-weight: bold; color: white; padding: 5px;")
        EXCHANGE_open_orders_header_layout.addWidget(self.EXCHANGE_open_orders_label)

        # Add another spacer to balance the layout
        EXCHANGE_open_orders_header_layout.addStretch(1)

        # Add header to EXCHANGE open orders layout
        EXCHANGE_open_orders_layout.addWidget(EXCHANGE_open_orders_header)

        # Create EXCHANGE open orders table
        self.EXCHANGE_open_orders_table = QTableWidget()
        self.setup_table(self.EXCHANGE_open_orders_table, self.EXCHANGE_open_orders_columns)
        EXCHANGE_open_orders_layout.addWidget(self.EXCHANGE_open_orders_table)

        # Restore column order for EXCHANGE open orders table
        self.restore_column_order(self.EXCHANGE_open_orders_table)
     
        # Create Binance trade history table

        # Add widgets to top splitter
        top_splitter.addWidget(open_trade_widget)
        top_splitter.addWidget(EXCHANGE_open_orders_widget)

        # Create bottom section splitter (horizontal)
        bottom_splitter = QSplitter(Qt.Orientation.Horizontal)
        bottom_splitter.setChildrenCollapsible(False)
        
        # Create close trade monitor table
        close_trade_widget = QWidget()
        close_trade_layout = QVBoxLayout(close_trade_widget)
        close_trade_layout.setContentsMargins(0, 0, 0, 0)
        
        # Create header with title and search bar that spans the entire width
        close_trade_header = QWidget()
        close_trade_header.setStyleSheet("background-color: #444444;")
        close_trade_header_layout = QHBoxLayout(close_trade_header)
        close_trade_header_layout.setContentsMargins(5, 5, 5, 5)
        
        # Add search functionality at the left
        close_search_label = QLabel("Search:")
        close_search_label.setStyleSheet("color: white;")
        close_trade_header_layout.addWidget(close_search_label)
        
        self.close_trade_search_input = QLineEdit()
        self.close_trade_search_input.setPlaceholderText("Type to search...")
        self.close_trade_search_input.textChanged.connect(self.filter_close_trade_table)
        self.close_trade_search_input.textEdited.connect(self.convert_to_uppercase)
        self.close_trade_search_input.setMaximumWidth(150)
        self.close_trade_search_input.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        close_trade_header_layout.addWidget(self.close_trade_search_input)
        
        # Add clear button
        close_clear_button = QPushButton("Clear")
        close_clear_button.setMaximumWidth(60)
        close_clear_button.clicked.connect(lambda: self.close_trade_search_input.clear())
        close_clear_button.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        close_trade_header_layout.addWidget(close_clear_button)



        self.close_enable_orders_btn = QPushButton("Set SELL Maker")
        self.close_enable_orders_btn.clicked.connect(lambda: self.toggle_maker_type(type=3))
        close_trade_header_layout.addWidget(self.close_enable_orders_btn)
        
        self.close_disable_orders_btn = QPushButton("Reset")
        self.close_disable_orders_btn.clicked.connect(lambda: self.toggle_maker_type(type=9))
        close_trade_header_layout.addWidget(self.close_disable_orders_btn)
        # Add spacer to push title to center
        close_trade_header_layout.addStretch(1)
        
        # Add title label in the center
        self.close_trade_label = QLabel(f"SELL MONITOR (Active: 0 Inactive: 0 Total: {symbol_count} symbols)")
        self.close_trade_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.close_trade_label.setStyleSheet("font-weight: bold; color: white; padding: 5px;")
        close_trade_header_layout.addWidget(self.close_trade_label)
        
        # Add another spacer to balance the layout
        close_trade_header_layout.addStretch(1)
        
        # Add header to close trade layout
        close_trade_layout.addWidget(close_trade_header)
        
        self.close_trade_table = QTableWidget()
        self.setup_table(self.close_trade_table, self.close_trade_columns)
        # Connect manual selection tracking
        self.close_trade_table.selectionModel().selectionChanged.connect(self.manual_track_close_selection)
        close_trade_layout.addWidget(self.close_trade_table)
        
        # Restore column order for close trade table
        self.restore_column_order(self.close_trade_table)
        
        # Create Binance trade history table
        binance_trade_widget = QWidget()
        binance_trade_layout = QVBoxLayout(binance_trade_widget)
        binance_trade_layout.setContentsMargins(0, 0, 0, 0)
        
        # Create header with title and pagination controls
        binance_trade_header = QWidget()
        binance_trade_header.setStyleSheet("background-color: #444444;")
        binance_trade_header_layout = QHBoxLayout(binance_trade_header)
        binance_trade_header_layout.setContentsMargins(5, 5, 5, 5)
        
        # Add title label at the left
        binance_trade_label = QLabel("BINANCE TRADE HISTORY")
        binance_trade_label.setStyleSheet("font-weight: bold; color: white; padding: 5px;")
        binance_trade_header_layout.addWidget(binance_trade_label)
        
        # Add spacer to push pagination controls to the right
        binance_trade_header_layout.addStretch(1)
        
        # Add pagination controls for Binance
        global binance_history_page_size
        binance_history_page_size = self.redis.llen('maker_BINANCE_trade_history') // 100
        self.binance_page_label = QLabel(f"Page 1 of {binance_history_page_size}")
        self.binance_page_label.setStyleSheet("color: white;")
        binance_trade_header_layout.addWidget(self.binance_page_label)
        
        self.binance_prev_btn = QPushButton("◄")
        self.binance_prev_btn.setFixedSize(30, 25)
        self.binance_prev_btn.clicked.connect(lambda: self.change_binance_page(-1))
        self.binance_prev_btn.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        binance_trade_header_layout.addWidget(self.binance_prev_btn)
        
        self.binance_next_btn = QPushButton("►")
        self.binance_next_btn.setFixedSize(30, 25)
        self.binance_next_btn.clicked.connect(lambda: self.change_binance_page(1))
        self.binance_next_btn.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        binance_trade_header_layout.addWidget(self.binance_next_btn)
        
        self.binance_reset_btn = QPushButton("Reset")
        self.binance_reset_btn.setFixedWidth(50)
        self.binance_reset_btn.clicked.connect(self.reset_binance_page)
        self.binance_reset_btn.setStyleSheet("""
            QPushButton {
                background-color: #555555; 
                color: white; 
                border: 1px solid #666666;
            }
            QPushButton:hover {
                background-color: #777777;
                border: 1px solid #888888;
            }
        """)
        binance_trade_header_layout.addWidget(self.binance_reset_btn)
        
        # Add export and clear buttons for Binance
        self.binance_export_btn = QPushButton("Export")
        self.binance_export_btn.setFixedWidth(60)
        self.binance_export_btn.clicked.connect(lambda: self.export_redis_to_csv("BINANCE"))
        self.binance_export_btn.setStyleSheet("""
            QPushButton {
                background-color: #cc6600; 
                color: white; 
                border: 1px solid #cc6600;
            }
            QPushButton:hover {
                background-color: #ff8800;
                border: 1px solid #ff8800;
            }
        """)
        binance_trade_header_layout.addWidget(self.binance_export_btn)
        
        self.binance_clear_btn = QPushButton("Clear")
        self.binance_clear_btn.setFixedWidth(60)
        self.binance_clear_btn.clicked.connect(self.clear_binance_trade_history)
        self.binance_clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #cc0000; 
                color: white; 
                border: 1px solid #cc0000;
            }
            QPushButton:hover {
                background-color: #ff0000;
                border: 1px solid #ff0000;
            }
        """)
        binance_trade_header_layout.addWidget(self.binance_clear_btn)
        
        # Add header to Binance trade layout
        binance_trade_layout.addWidget(binance_trade_header)
        
        self.binance_trade_table = QTableWidget()
        self.setup_table(self.binance_trade_table, [
            "OrderTime", "Symbol", "Side", "Amount", "Price",
            "PriceTRY", "AmountTRY", "AmountUSDT", "Fee", "ExecutedMargin", "PNL", "USDTTRY Bid", "USDTTRY Ask"
        ])
        binance_trade_layout.addWidget(self.binance_trade_table)
        
        # Restore column order for Binance trade table
        self.restore_column_order(self.binance_trade_table)
        
        # Add widgets to bottom splitter
        bottom_splitter.addWidget(close_trade_widget)
        
        # Create trade history tab widget
        trade_history_widget = QWidget()
        trade_history_layout = QVBoxLayout(trade_history_widget)
        trade_history_layout.setContentsMargins(0, 0, 0, 0)
        
        # Create tab widget for trade history
        self.trade_history_tabs = QTabWidget()
        self.trade_history_tabs.setStyleSheet("""
            QTabBar::tab {
                height: 30px;
                padding: 5px 10px;
                background-color: #444444;
                color: white;
                border: 1px solid #666666;
            }
            QTabBar::tab:selected {
                background-color: #555555;
            }
        """)
        
        # Add EXCHANGE trade history tab
        self.trade_history_tabs.addTab(EXCHANGE_trade_widget, "EXCHANGE Trade History")
        
        # Add Binance trade history tab
        self.trade_history_tabs.addTab(binance_trade_widget, "Binance Trade History")
        
        # Create Move History tab
        move_history_widget = QWidget()
        move_history_layout = QVBoxLayout(move_history_widget)
        move_history_layout.setContentsMargins(0, 0, 0, 0)
        
        # Create header with title
        move_history_header = QWidget()
        move_history_header.setStyleSheet("background-color: #444444;")
        move_history_header_layout = QHBoxLayout(move_history_header)
        move_history_header_layout.setContentsMargins(5, 5, 5, 5)
        
        # Add title label
        move_history_label = QLabel("MOVE HISTORY")
        move_history_label.setStyleSheet("font-weight: bold; color: white; padding: 5px;")
        move_history_header_layout.addWidget(move_history_label)
        
        # Add spacer to push pagination controls to the right
        move_history_header_layout.addStretch(1)
        
        # Add pagination controls for Move History
        global move_history_page_size
        move_history_page_size = self.redis.llen('maker_move_history') // 100
        self.move_history_page_label = QLabel(f"Page 1 of {move_history_page_size}")
        self.move_history_page_label.setStyleSheet("color: white;")
        move_history_header_layout.addWidget(self.move_history_page_label)
        
        self.move_history_prev_btn = QPushButton("◄")
        self.move_history_prev_btn.setFixedSize(30, 25)
        self.move_history_prev_btn.clicked.connect(lambda: self.change_move_history_page(-1))
        self.move_history_prev_btn.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        move_history_header_layout.addWidget(self.move_history_prev_btn)
        
        self.move_history_next_btn = QPushButton("►")
        self.move_history_next_btn.setFixedSize(30, 25)
        self.move_history_next_btn.clicked.connect(lambda: self.change_move_history_page(1))
        self.move_history_next_btn.setStyleSheet("background-color: #555555; color: white; border: 1px solid #666666;")
        move_history_header_layout.addWidget(self.move_history_next_btn)
        
        self.move_history_reset_btn = QPushButton("Reset")
        self.move_history_reset_btn.setFixedWidth(50)
        self.move_history_reset_btn.clicked.connect(self.reset_move_history_page)
        self.move_history_reset_btn.setStyleSheet("""
            QPushButton {
                background-color: #555555; 
                color: white; 
                border: 1px solid #666666;
            }
            QPushButton:hover {
                background-color: #777777;
                border: 1px solid #888888;
            }
        """)
        move_history_header_layout.addWidget(self.move_history_reset_btn)
        
        # Add export and clear buttons for Move History
        self.move_history_export_btn = QPushButton("Export")
        self.move_history_export_btn.setFixedWidth(60)
        self.move_history_export_btn.clicked.connect(lambda: self.export_redis_to_csv("MOVE"))
        self.move_history_export_btn.setStyleSheet("""
            QPushButton {
                background-color: #cc6600; 
                color: white; 
                border: 1px solid #cc6600;
            }
            QPushButton:hover {
                background-color: #ff8800;
                border: 1px solid #ff8800;
            }
        """)
        move_history_header_layout.addWidget(self.move_history_export_btn)
        
        self.move_history_clear_btn = QPushButton("Clear")
        self.move_history_clear_btn.setFixedWidth(60)
        self.move_history_clear_btn.clicked.connect(self.clear_move_history)
        self.move_history_clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #cc0000; 
                color: white; 
                border: 1px solid #cc0000;
            }
            QPushButton:hover {
                background-color: #ff0000;
                border: 1px solid #ff0000;
            }
        """)
        move_history_header_layout.addWidget(self.move_history_clear_btn)
        
        # Add header to Move History layout
        move_history_layout.addWidget(move_history_header)
        
        # Create Move History table
        self.move_history_table = QTableWidget()
        self.setup_table(self.move_history_table, [
            "MoveTime", "OrderID", "Symbol", "Core", "BinanceMargin", 
            "EXCHANGEMargin", "StopMargin", "MoveThres", "MarginDiff", "PNL", "Source"
        ])
        move_history_layout.addWidget(self.move_history_table)
        
        # Restore column order for Move History table
        self.restore_column_order(self.move_history_table)
        
        # Add Move History tab
        self.move_history_tab_index = self.trade_history_tabs.addTab(move_history_widget, "Move History (0)")
        
        # Add trade history tab widget to layout
        trade_history_layout.addWidget(self.trade_history_tabs)
        
        # Add trade history widget to bottom splitter
        bottom_splitter.addWidget(trade_history_widget)
        bottom_splitter.setSizes([int(self.width() * 0.5), int(self.width() * 0.5)])  # Set initial sizes
        
        # Add splitters to main splitter
        main_splitter.addWidget(top_splitter)
        main_splitter.addWidget(bottom_splitter)
        main_splitter.setSizes([int(self.height() * 0.6), int(self.height() * 0.4)])  # Set initial sizes
        
        # Add main splitter to monitoring layout
        monitoring_layout.addWidget(main_splitter)
        
    def change_EXCHANGE_page(self, direction):
        global EXCHANGE_history_page_no

        EXCHANGE_history_page_size = self.redis.llen('maker_EXCHANGE_trade_history') // 100
        EXCHANGE_history_page_no += direction
        if EXCHANGE_history_page_no > EXCHANGE_history_page_size:
            EXCHANGE_history_page_no = EXCHANGE_history_page_size
        if EXCHANGE_history_page_no < 1:
            EXCHANGE_history_page_no = 1
        self.EXCHANGE_page_label.setText(f"Page {EXCHANGE_history_page_no} of {EXCHANGE_history_page_size}")

    def change_binance_page(self, direction):
        global binance_history_page_no

        binance_history_page_size = self.redis.llen('maker_BINANCE_trade_history') // 100
        binance_history_page_no += direction
        if binance_history_page_no > binance_history_page_size:
            binance_history_page_no = binance_history_page_size
        if binance_history_page_no < 1:
            binance_history_page_no = 1
        self.binance_page_label.setText(f"Page {binance_history_page_no} of {binance_history_page_size}")   
    
    def reset_EXCHANGE_page(self):
        global EXCHANGE_history_page_no

        EXCHANGE_history_page_size = self.redis.llen('maker_EXCHANGE_trade_history') // 100
        EXCHANGE_history_page_no = 1
        self.EXCHANGE_page_label.setText(f"Page {EXCHANGE_history_page_no} of {EXCHANGE_history_page_size}")

    def reset_binance_page(self):
        global binance_history_page_no
        
        binance_history_page_size = self.redis.llen('maker_BINANCE_trade_history') // 100
        binance_history_page_no = 1
        self.binance_page_label.setText(f"Page {binance_history_page_no} of {binance_history_page_size}")
    
    def change_move_history_page(self, direction):
        global move_history_page_no

        move_history_page_size = self.redis.llen('maker_move_history') // 100
        move_history_page_no += direction
        if move_history_page_no > move_history_page_size:
            move_history_page_no = move_history_page_size
        if move_history_page_no < 1:
            move_history_page_no = 1
        self.move_history_page_label.setText(f"Page {move_history_page_no} of {move_history_page_size}")

    def reset_move_history_page(self):
        global move_history_page_no

        move_history_page_size = self.redis.llen('maker_move_history') // 100
        move_history_page_no = 1
        self.move_history_page_label.setText(f"Page {move_history_page_no} of {move_history_page_size}")
        
    
    def filter_EXCHANGE_open_orders(self):
        """Filter EXCHANGE open orders table based on search text"""
        search_text = self.EXCHANGE_open_orders_search_input.text().upper()
        
        for row in range(self.EXCHANGE_open_orders_table.rowCount()):
            symbol_item = self.EXCHANGE_open_orders_table.item(row, 2)  # Symbol is at the third column (index 2)
            if symbol_item:
                symbol = symbol_item.text()
                if search_text and search_text not in symbol:
                    self.EXCHANGE_open_orders_table.hideRow(row)
                else:
                    self.EXCHANGE_open_orders_table.showRow(row)
        
        # Update the status bar with the number of visible rows
        open_visible = sum(1 for row in range(self.EXCHANGE_open_orders_table.rowCount()) 
                          if not self.EXCHANGE_open_orders_table.isRowHidden(row))
        total_open = self.EXCHANGE_open_orders_table.rowCount()
        
        self.statusBar().showMessage(f"EXCHANGE Open Orders: Showing {open_visible} of {total_open} symbols")
    
    
    def filter_open_trade_table(self):
        """Filter open trade table based on search text"""
        search_text = self.open_trade_search_input.text().upper()
        
        for row in range(self.open_trade_table.rowCount()):
            symbol_item = self.open_trade_table.item(row, 0)  # BaseSymbol is in the first column (index 0)
            if symbol_item:
                symbol = symbol_item.text()
                if search_text and search_text not in symbol:
                    self.open_trade_table.hideRow(row)
                else:
                    self.open_trade_table.showRow(row)
        
        # Update the status bar with the number of visible rows
        open_visible = sum(1 for row in range(self.open_trade_table.rowCount()) 
                          if not self.open_trade_table.isRowHidden(row))
        total_open = self.open_trade_table.rowCount()
        
        self.statusBar().showMessage(f"Buy Monitor: Showing {open_visible} of {total_open} symbols")

    def filter_close_trade_table(self):
        """Filter close trade table based on search text"""
        search_text = self.close_trade_search_input.text().upper()
        
        for row in range(self.close_trade_table.rowCount()):
            symbol_item = self.close_trade_table.item(row, 0)  # BaseSymbol is in the first column (index 0)
            if symbol_item:
                symbol = symbol_item.text()
                if search_text and search_text not in symbol:
                    self.close_trade_table.hideRow(row)
                else:
                    self.close_trade_table.showRow(row)
        
        # Update the status bar with the number of visible rows
        close_visible = sum(1 for row in range(self.close_trade_table.rowCount()) 
                           if not self.close_trade_table.isRowHidden(row))
        total_close = self.close_trade_table.rowCount()
        
        self.statusBar().showMessage(f"Sell Monitor: Showing {close_visible} of {total_close} symbols")

    def update_monitoring_data(self, df):
        """Update the monitoring table with new data"""
        if df is None:
            return
        
        try:
            # Update symbol count labels
            symbol_count = len(df) 
            
            # Calculate active/inactive statistics based on maker_type
            active_count = 0
            inactive_count = 0
            total_count = symbol_count
            
            if 'Maker_Type' in df.columns:
                # Active = maker_type is 1 (buy), 3 (sell), or 13 (combo)
                # Inactive = maker_type is 0 or None
                active_count = len(df[df['Maker_Type'].notna() & (df['Maker_Type'].isin([1, 3, 13]))])
                inactive_count = len(df[df['Maker_Type'].isna() | (df['Maker_Type'] == 0)])
            
            # Update both labels with statistics
            if hasattr(self, 'open_trade_label'):
                self.open_trade_label.setText(f"BUY MONITOR (Active: {active_count} Inactive: {inactive_count} Total: {total_count} symbols)")
            if hasattr(self, 'close_trade_label'):
                self.close_trade_label.setText(f"SELL MONITOR (Active: {active_count} Inactive: {inactive_count} Total: {total_count} symbols)")
        except Exception as e:
            logging.error(f"Error updating symbol count: {e}")
        
        # LOG CURRENT SELECTED SYMBOLS BEFORE UPDATE
        open_selected = set()
        close_selected = set()
        
        if hasattr(self, 'open_trade_table'):
            for row in range(self.open_trade_table.rowCount()):
                if self.open_trade_table.selectionModel().isRowSelected(row, self.open_trade_table.rootIndex()):
                    symbol_item = self.open_trade_table.item(row, 0)
                    if symbol_item:
                        open_selected.add(symbol_item.text())
        
        if hasattr(self, 'close_trade_table'):
            for row in range(self.close_trade_table.rowCount()):
                if self.close_trade_table.selectionModel().isRowSelected(row, self.close_trade_table.rootIndex()):
                    symbol_item = self.close_trade_table.item(row, 0)
                    if symbol_item:
                        close_selected.add(symbol_item.text())
        
        #if open_selected:
            #logging.info(f"MONITORING UPDATE - Open trade selected symbols: {open_selected}")
        #if close_selected:
            #logging.info(f"MONITORING UPDATE - Close trade selected symbols: {close_selected}")
        
        # Save current search text for both tables
        open_search = self.open_trade_search_input.text() if hasattr(self, 'open_trade_search_input') else ""
        close_search = self.close_trade_search_input.text() if hasattr(self, 'close_trade_search_input') else ""
        

        # Update open trade table
        self.open_trade_table.setSortingEnabled(False)
        self.open_trade_table.setRowCount(0)

        self.close_trade_table.setSortingEnabled(False)
        self.close_trade_table.setRowCount(0)
        
        # Update open trade table
        self.update_table(self.open_trade_table, df, self.open_trade_columns, [
            lambda x: x,  # Symbol
            lambda x: "BUY" if x == 1 else "SELL" if x == 3 else "COMBO" if x == 13 else "NONE",  # Maker_Type
            lambda x: f"{x * 10000:.0f}",  # OpenMargin
            lambda x: f"{x * 10000:.0f}",  # OpenTriggerMargin
            lambda x: f"{x * 10000:.0f}",  # OpenStopMargin
            lambda x: f"{x * 10000:.0f}",  # OpenAggression
            lambda x: f"{x * 10000:.0f}",  # OpenMarginWindow
            lambda x: f"₺{x:,.0f}",  # EXCHANGE PosTRY
            lambda x: f"{float(x):.4f}" if is_number(x) else "N/A",  # EXCHANGE PosCoin
            lambda x: f"{float(x):.4f}" if is_number(x) else "N/A",  # Binance PosCoin
            lambda x: f"₺{x:,.0f}",  # MaxPosTRY
            lambda x: f"₺{float(x):,.0f}" if is_number(x) else "N/A",  # CapacityTRY
            lambda x: x,  # BinanceTime
            lambda x: x,  # EXCHANGETime
            lambda x: f"{float(x):.0f}" if is_number(x) else "N/A",  # TimeDiff
            lambda x: f"{float(x):.0f}" if is_number(x) else "N/A",  # Binance Abs Time Diff (ms)
        ])
        
        # Update close trade table
        self.update_table(self.close_trade_table, df, self.close_trade_columns, [
            lambda x: x,  # Symbol
            lambda x: "BUY" if x == 1 else "SELL" if x == 3 else "COMBO" if x == 13 else "NONE",  # Maker_Type
            lambda x: f"{x * 10000:.0f}",  # CloseMargin
            lambda x: f"{x * 10000:.0f}",  # CloseTriggerMargin
            lambda x: f"{x * 10000:.0f}",  # CloseStopMargin
            lambda x: f"{x * 10000:.0f}",  # CloseAggression
            lambda x: f"{x * 10000:.0f}",  # CloseMarginWindow
            lambda x: f"₺{x:,.0f}",  # EXCHANGE PosTRY
            lambda x: f"{float(x):.4f}" if is_number(x) else "N/A",  # EXCHANGE PosCoin
            lambda x: f"{float(x):.4f}" if is_number(x) else "N/A",  # Binance PosCoin
            lambda x: x,  # BinanceTime
            lambda x: x,  # EXCHANGETime
            lambda x: f"{float(x):.0f}" if is_number(x) else "N/A",  # TimeDiff
        ])
        
        # Reapply search filters if there were any
        if open_search:
            self.filter_open_trade_table_without_updating_input(open_search)
        if close_search:
            self.filter_close_trade_table_without_updating_input(close_search)
        

    def filter_open_trade_table_without_updating_input(self, search_text):
        """Filter open trade table based on search text without updating the input field"""
        for row in range(self.open_trade_table.rowCount()):
            symbol_item = self.open_trade_table.item(row, 0)  # BaseSymbol is in the first column (index 0)
            if symbol_item:
                symbol = symbol_item.text()
                if search_text and search_text not in symbol:
                    self.open_trade_table.hideRow(row)
                else:
                    self.open_trade_table.showRow(row)

    def filter_close_trade_table_without_updating_input(self, search_text):
        """Filter close trade table based on search text without updating the input field"""
        for row in range(self.close_trade_table.rowCount()):
            symbol_item = self.close_trade_table.item(row, 0)  # BaseSymbol is in the first column (index 0)
            if symbol_item:
                symbol = symbol_item.text()
                if search_text and search_text not in symbol:
                    self.close_trade_table.hideRow(row)
                else:
                    self.close_trade_table.showRow(row)
        
    def filter_EXCHANGE_open_orders_without_updating_input(self, search_text):
        """Filter EXCHANGE open orders table based on search text without updating the input field"""
        for row in range(self.EXCHANGE_open_orders_table.rowCount()):
            symbol_item = self.EXCHANGE_open_orders_table.item(row, 2)  # Symbol is at the third column (index 2)
            if symbol_item:
                symbol = symbol_item.text()
                if search_text and search_text not in symbol:
                    self.EXCHANGE_open_orders_table.hideRow(row)
                else:
                    self.EXCHANGE_open_orders_table.showRow(row)
        
        
    def setup_table(self, table, headers):
        """Set up a table widget with the given headers"""
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        
        # Set table properties
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSortingEnabled(True)
        
        # Enable grid lines
        table.setShowGrid(True)
        table.setGridStyle(Qt.PenStyle.SolidLine)
        
        # Enable horizontal scrollbar
        table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        
        # Set horizontal header properties
        header = table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(False)  # Disable stretch to allow horizontal scrolling
        header.setDefaultAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        header.setHighlightSections(False)
        
        # Enable drag and drop for column reordering
        header.setSectionsMovable(True)
        
        # Set default column widths
        for i in range(len(headers)):
            table.setColumnWidth(i, 120)  # Set a reasonable default width
        
        # Set vertical header properties
        table.verticalHeader().setVisible(False)
        
        # Set font
        font = QFont("Arial", 9)
        table.setFont(font)
        
        # Set row height
        table.verticalHeader().setDefaultSectionSize(24)
        
        # Connect to header right-click for column filtering
        header.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        header.customContextMenuRequested.connect(lambda pos: self.show_header_menu(table, pos))
        
        # Connect to column moved signal to save column order
        header.sectionMoved.connect(lambda logical, oldVisual, newVisual: self.save_column_order(table))

    def save_column_order(self, table):
        """Save the current column order for the table"""
        # Get the table name
        table_name = ""
        if table == self.open_trade_table:
            table_name = "open_trade_table"
        elif hasattr(self, 'close_trade_table') and table == self.close_trade_table:
            table_name = "close_trade_table"
        elif hasattr(self, 'EXCHANGE_trade_table') and table == self.EXCHANGE_trade_table:
            table_name = "EXCHANGE_trade_table"
        elif hasattr(self, 'binance_trade_table') and table == self.binance_trade_table:
            table_name = "binance_trade_table"
        elif hasattr(self, 'EXCHANGE_open_orders_table') and table == self.EXCHANGE_open_orders_table:
            table_name = "EXCHANGE_open_orders_table"
        else:
            return
        
        # Get the current column order
        header = table.horizontalHeader()
        column_order = []
        for visual_index in range(header.count()):
            logical_index = header.logicalIndex(visual_index)
            column_name = table.horizontalHeaderItem(logical_index).text()
            column_order.append((logical_index, column_name))
        
        # Save to Redis
        try:
            order_json = json.dumps(column_order)
            self.redis.set(f"{table_name}_column_order", order_json)
            logging.info(f"Saved column order for {table_name}: {column_order}")
        except Exception as e:
            logging.error(f"Error saving column order: {e}")

    def restore_column_order(self, table):
        """Restore the saved column order for the table"""
        # Get the table name
        table_name = ""
        if table == self.open_trade_table:
            table_name = "open_trade_table"
        elif hasattr(self, 'close_trade_table') and table == self.close_trade_table:
            table_name = "close_trade_table"
        elif hasattr(self, 'EXCHANGE_trade_table') and table == self.EXCHANGE_trade_table:
            table_name = "EXCHANGE_trade_table"
        elif hasattr(self, 'binance_trade_table') and table == self.binance_trade_table:
            table_name = "binance_trade_table"
        elif hasattr(self, 'EXCHANGE_open_orders_table') and table == self.EXCHANGE_open_orders_table:
            table_name = "EXCHANGE_open_orders_table"
        else:
            return
        
        # Get the saved column order from Redis
        try:
            order_json = self.redis.get(f"{table_name}_column_order")
            if order_json:
                column_order = json.loads(order_json)
                
                # Restore the column order
                header = table.horizontalHeader()
                for i, (logical_index, _) in enumerate(column_order):
                    current_visual = header.visualIndex(logical_index)
                    if current_visual != i:
                        header.moveSection(current_visual, i)
                
                logging.info(f"Restored column order for {table_name}: {column_order}")
        except Exception as e:
            logging.error(f"Error restoring column order: {e}")

    def show_header_menu(self, table, pos):
        """Show context menu for table header with filtering options"""
        header = table.horizontalHeader()
        column_idx = header.logicalIndexAt(pos)
        
        if column_idx < 0:
            return
        
        column_name = table.horizontalHeaderItem(column_idx).text()
        
        # Create menu
        menu = QMenu(self)
        
        # Add column visibility submenu
        visibility_menu = menu.addMenu("Column Visibility")
        
        # Add "Show All" and "Hide All" actions
        show_all = QAction("Show All Columns", self)
        show_all.triggered.connect(lambda: self.toggle_all_columns(table, True))
        visibility_menu.addAction(show_all)
        
        hide_all = QAction("Show Essential Columns", self)
        hide_all.triggered.connect(lambda: self.toggle_all_columns(table, False))
        visibility_menu.addAction(hide_all)
        
        
        visibility_menu.addSeparator()
        
        # Get essential columns based on table type
        essential_columns = ["Symbol", "Margin", "Threshold"]
        if table == self.EXCHANGE_trade_table:
            essential_columns = ["Symbol", "OrderTime", "Status", "Side"]
        elif table == self.binance_trade_table:
            essential_columns = ["Symbol", "OrderTime", "Side"]
        
        # Add actions for each column with direct visibility toggle for history tables
        for i in range(table.columnCount()):
            col_name = table.horizontalHeaderItem(i).text()
            action = QAction(col_name, self)
            action.setCheckable(True)
            action.setChecked(not table.isColumnHidden(i))
            
            # Disable the action if it's an essential column
            if col_name in essential_columns:
                action.setEnabled(False)
                action.setToolTip("This column cannot be hidden")
            else:
                # Use a direct function for history tables
                if table == self.EXCHANGE_trade_table or table == self.binance_trade_table:
                    action.triggered.connect(lambda checked, t=table, col=i: self.toggle_history_column(t, col, checked))
                else:
                    # Use regular approach for other tables
                    action.triggered.connect(lambda checked, col=i: table.setColumnHidden(col, not checked))
            
            visibility_menu.addAction(action)
        
        
        # Show the menu at the header position
        menu.exec(table.mapToGlobal(pos))

    def show_column_menu_from_button(self, table):
        """Show column menu from button click (works even when all columns are hidden)"""
        menu = QMenu(self)
        
        # Add "Show All" action
        show_all = QAction("Show All Columns", self)
        show_all.triggered.connect(lambda: self.toggle_all_columns(table, True))
        menu.addAction(show_all)
        
        # Add "Hide All" action
        hide_all = QAction("Show Essential Columns", self)
        hide_all.triggered.connect(lambda: self.toggle_all_columns(table, False))
        menu.addAction(hide_all)
        
        
        menu.addSeparator()
        
        # Get essential columns based on table type
        essential_columns = ["Symbol", "Margin", "Threshold"]
        if table == self.EXCHANGE_trade_table or table == self.binance_trade_table:
            essential_columns = ["Symbol", "OrderTime", "Status", "Side"]
        
        # Add actions for each column with direct visibility toggle for history tables
        for i in range(table.columnCount()):
            column_name = table.horizontalHeaderItem(i).text()
            action = QAction(column_name, self)
            action.setCheckable(True)
            action.setChecked(not table.isColumnHidden(i))
            
            # Disable the action if it's an essential column
            if column_name in essential_columns:
                action.setEnabled(False)
                action.setToolTip("This column cannot be hidden")
            else:
                # Use a direct function for history tables
                if table == self.EXCHANGE_trade_table or table == self.binance_trade_table:
                    action.triggered.connect(lambda checked, t=table, col=i: self.toggle_history_column(t, col, checked))
                else:
                    # Use regular approach for other tables
                    action.triggered.connect(lambda checked, col=i: table.setColumnHidden(col, not checked))
            
            menu.addAction(action)
        
        # Show the menu at the button position
        menu.exec_(QCursor.pos())

    def toggle_all_columns(self, table, show):
        """Show or hide all columns in the table, keeping essential columns visible"""
        # Get essential columns based on table type
        if table == self.open_trade_table:
            essential_columns = ["BaseSymbol", "Maker_Type", "OpenMargin", "OpenTriggerMargin", "OpenStopMargin", "EXCHANGEPositionAmount_TRY"]
        elif table == self.close_trade_table:
            essential_columns = ["BaseSymbol", "Maker_Type", "CloseMargin", "CloseTriggerMargin", "CloseStopMargin", "EXCHANGEPositionAmount_TRY"]
        elif table == self.EXCHANGE_trade_table: 
            essential_columns = ["Symbol", "OrderTime", "Status", "Side"]
        elif table == self.binance_trade_table:
            essential_columns = ["Symbol", "OrderTime", "Side"]
        elif table == self.settings_table:
            essential_columns = self.settings_columns_essential
        
        # For all tables, use the direct approach that works for individual columns
        for i in range(table.columnCount()):
            column_name = table.horizontalHeaderItem(i).text()
            if column_name not in essential_columns:
                # Check if we're showing a previously hidden column
                was_hidden = table.isColumnHidden(i)
                
                # Directly set column visibility
                table.setColumnHidden(i, not show)
                
                # If we're showing a previously hidden column, set a default width
                if show and was_hidden:
                    logging.info(f"Setting default width for previously hidden column {i}: {column_name}")
                    table.setColumnWidth(i, 100)  # Set default width
                    
                    # Resize to content after a short delay
                    QTimer.singleShot(100, lambda col=i: self.resize_column_to_content(table, col))
            else:
                # Always show essential columns
                table.setColumnHidden(i, False)
        
        # Store the current visibility state for history tables
        if table == self.EXCHANGE_trade_table:
            self.EXCHANGE_column_state = [table.isColumnHidden(i) for i in range(table.columnCount())]
        elif table == self.binance_trade_table:
            self.binance_column_state = [table.isColumnHidden(i) for i in range(table.columnCount())]

    def resize_column_to_content(self, table, column):
        """Resize a specific column to fit its content"""
        current_width = table.columnWidth(column)
        logging.info(f"Resizing column {column} to content, current width: {current_width}")
        
        # Resize the column to fit its content
        table.resizeColumnToContents(column)
        
        # Get the new width
        new_width = table.columnWidth(column)
        logging.info(f"Column {column} resized to: {new_width}")
        
        # Verify visibility after resize
        column_name = table.horizontalHeaderItem(column).text()
        is_hidden = table.isColumnHidden(column)
        logging.info(f"Column {column} ({column_name}) visibility after resize: hidden={is_hidden}, width={new_width}")

    def update_maker_EXCHANGE_trade_history(self, df):
        if df is None or df.empty:
            return

        # Define visible columns with their formatters
        columns = maker_EXCHANGE_trade_history_columns
        formatters = [
            lambda x: x,  # ExecutionTime
            lambda x: x,  # OrderId
            lambda x: x,  # Symbol
            lambda x: x,  # Core
            lambda x: x,  # Side
            lambda x: f"₺{float(x)}" if pd.notna(x) else "",  # Price
            lambda x: f"{float(x)}" if pd.notna(x) else "",  # Amount
            lambda x: f"₺{float(x):,.0f}" if pd.notna(x) else "",  # AmountTRY
            lambda x: f"{float(x)*10000:.2f}" if pd.notna(x) else "",  # ExecutedMargin
        ]
        
        # Ensure all columns exist
        for col in columns:
            if col not in df.columns:
                df[col] = None
                
        # Save current sort order
        sort_column = self.EXCHANGE_trade_table.horizontalHeader().sortIndicatorSection()
        sort_order = self.EXCHANGE_trade_table.horizontalHeader().sortIndicatorOrder()
        
        # Save column widths only if not first update
        column_widths = []
        if hasattr(self, 'EXCHANGE_first_update_done') and self.EXCHANGE_first_update_done:
            for i in range(self.EXCHANGE_trade_table.columnCount()):
                column_widths.append(self.EXCHANGE_trade_table.columnWidth(i))
        
        # Save column visibility state only if not first update
        column_hidden = []
        if hasattr(self, 'EXCHANGE_first_update_done') and self.EXCHANGE_first_update_done:
            for i in range(self.EXCHANGE_trade_table.columnCount()):
                column_hidden.append(self.EXCHANGE_trade_table.isColumnHidden(i))
        # Temporarily disable sorting
        self.EXCHANGE_trade_table.setSortingEnabled(False)
        
        # Clear existing rows but keep the structure
        self.EXCHANGE_trade_table.setRowCount(0)
        
        # Add rows for each trade
        self.EXCHANGE_trade_table.setRowCount(len(df))
        
        # Get current time for highlighting recent trades
        current_time = time.time()
        
        # Populate table
        for row_idx, (_, trade) in enumerate(df.iterrows()):
            # Get the trade time and convert to timestamp
            execution_time_str = trade.get('ExecutionTime', '')
            try:
                # Parse the time string to get a timestamp - handle multiple formats
                if 'T' in execution_time_str and execution_time_str.endswith('Z'):
                    # ISO 8601 format with Z suffix: 2025-08-05T18:10:42.483Z or 2025-08-05T18:10:46Z
                    execution_time_str_clean = execution_time_str.rstrip('Z')
                    if '.' in execution_time_str_clean:
                        # With milliseconds
                        execution_time = datetime.fromisoformat(execution_time_str_clean)
                    else:
                        # Without milliseconds
                        execution_time = datetime.fromisoformat(execution_time_str_clean)
                elif 'T' in execution_time_str:
                    # ISO 8601 format without Z suffix
                    execution_time = datetime.fromisoformat(execution_time_str)
                else:
                    # Standard format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.mmm (with milliseconds)
                    if '.' in execution_time_str:
                        # With milliseconds (3 digits) - need to pad to microseconds (6 digits) for %f
                        parts = execution_time_str.split('.')
                        if len(parts) == 2:
                            milliseconds = parts[1]
                            # Pad milliseconds to 6 digits (microseconds)
                            microseconds = milliseconds.ljust(6, '0')[:6]
                            execution_time_str_padded = f"{parts[0]}.{microseconds}"
                            execution_time = datetime.strptime(execution_time_str_padded, "%Y-%m-%d %H:%M:%S.%f")
                        else:
                            execution_time = datetime.strptime(execution_time_str, "%Y-%m-%d %H:%M:%S")
                    else:
                        # Without milliseconds
                        execution_time = datetime.strptime(execution_time_str, "%Y-%m-%d %H:%M:%S")
                
                timestamp = execution_time.timestamp()
                logging.debug(f"Parsed execution time: {execution_time_str} -> {timestamp}")
            except (ValueError, TypeError) as e:
                logging.error(f"Error parsing execution time: {execution_time_str} - Error: {e}")
                timestamp = 0
            
            # Get the side of the trade
            side = trade.get('Side', '')
            
            # Check if the trade is recent
            is_recent = (current_time - timestamp) < self.highlight_duration
            
            for col_idx, (col, formatter) in enumerate(zip(columns, formatters)):
                value = trade.get(col)
                
                # Format the value
                if pd.notna(value):
                    formatted_value = formatter(value)
                else:
                    formatted_value = ""
                
                # Create appropriate table item
                if col in ["ExecutedMargin", "Amount", "Price", "AmountTRY"] and pd.notna(value):
                    # Use NumericTableWidgetItem for numeric values
                    try:
                        numeric_value = float(value)
                        item = NumericTableWidgetItem(str(formatted_value), numeric_value)
                        item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                    except (ValueError, TypeError):
                        item = QTableWidgetItem(str(formatted_value))
                else:
                    # Use regular QTableWidgetItem for non-numeric values
                    item = QTableWidgetItem(str(formatted_value))
                
                # Set background color for recent trades
                if is_recent:
                    bg_color = QColor(0, 100, 0) if side == 'BUY' else QColor(139, 0, 0)
                    item.setBackground(bg_color)
                    #item.setFont(QFont("Arial", 10, QFont.Bold))
                self.EXCHANGE_trade_table.setItem(row_idx, col_idx, item)
        
        # Resize columns to fit content on first update only
        if not hasattr(self, 'EXCHANGE_first_update_done') or not self.EXCHANGE_first_update_done:
            self.resize_columns_to_contents(self.EXCHANGE_trade_table)
            self.EXCHANGE_first_update_done = True
        else:
            # Restore column widths for subsequent updates
            for i, width in enumerate(column_widths):
                if i < self.EXCHANGE_trade_table.columnCount():
                    self.EXCHANGE_trade_table.setColumnWidth(i, width)
        
        # Restore column visibility
        for i, hidden in enumerate(column_hidden):
            if i < self.EXCHANGE_trade_table.columnCount():
                self.EXCHANGE_trade_table.setColumnHidden(i, hidden)
        
        # Re-enable sorting and restore previous sort
        self.EXCHANGE_trade_table.setSortingEnabled(True)
        if sort_column < self.EXCHANGE_trade_table.columnCount():
            self.EXCHANGE_trade_table.horizontalHeader().setSortIndicator(sort_column, sort_order)

    def update_maker_binance_trade_history(self, df):
        if df is None or df.empty:
            return
            
        # Define visible columns with their formatters
        columns = [
            "OrderTime", "Symbol", "Side", "Amount", "Price", 
            "Price_TRY", "Amount_TRY", "Amount_USDT", "Fee", "ExecutedMargin", "PNL", "USDTTRY_bid", "USDTTRY_ask"
        ]
        formatters = [
            lambda x: x if '.' in str(x) else str(x),  # OrderTime - preserve milliseconds if present
            lambda x: x,  # Symbol
            lambda x: x,  # Side
            lambda x: f"{float(x):,.0f}" if pd.notna(x) else "",  # Amount
            lambda x: f"${float(x):,.4f}" if pd.notna(x) else "",  # Price
            lambda x: f"₺{float(x):,.4f}" if pd.notna(x) else "",  # Price_TRY
            lambda x: f"₺{float(x):,.0f}" if pd.notna(x) else "",  # Amount_TRY
            lambda x: f"${float(x):,.1f}" if pd.notna(x) else "",  # Amount_USDT
            lambda x: f"₺{float(x):,.2f}" if pd.notna(x) else "",  # Fee
            lambda x: f"{float(x) * 10000:.2f}" if pd.notna(x) else "",  # ExecutedMargin
            lambda x: f"₺{float(x):,.2f}" if pd.notna(x) else "",  # PNL
            lambda x: f"{float(x):,.2f}" if pd.notna(x) else "",  # USDTTRY_bid
            lambda x: f"{float(x):,.2f}" if pd.notna(x) else "",  # USDTTRY_ask
        ]
        
        # Ensure all columns exist
        for col in columns:
            if col not in df.columns:
                df[col] = None
                
        # Save current sort order
        sort_column = self.binance_trade_table.horizontalHeader().sortIndicatorSection()
        sort_order = self.binance_trade_table.horizontalHeader().sortIndicatorOrder()
        
        # Save column widths only if not first update
        column_widths = []
        if hasattr(self, 'binance_first_update_done') and self.binance_first_update_done:
            for i in range(self.binance_trade_table.columnCount()):
                column_widths.append(self.binance_trade_table.columnWidth(i))
        
        # Save column visibility state only if not first update
        column_hidden = []
        if hasattr(self, 'binance_first_update_done') and self.binance_first_update_done:
            for i in range(self.binance_trade_table.columnCount()):
                column_hidden.append(self.binance_trade_table.isColumnHidden(i))
        
        # Temporarily disable sorting
        self.binance_trade_table.setSortingEnabled(False)
        
        # Clear existing rows
        self.binance_trade_table.setRowCount(0)
        
        # Add rows for each trade
        self.binance_trade_table.setRowCount(len(df))
        
        current_time = time.time()
        latest_done_trade = None
        
        # Populate table
        for row_idx, (_, trade) in enumerate(df.iterrows()):
            # Get the trade time and convert to timestamp
            order_time_str = trade.get('OrderTime', '')
            try:
                # Parse the time string to get a timestamp - handle both formats (with and without milliseconds)
                if '.' in order_time_str:
                    # With milliseconds: "2025-12-16 20:05:40.123"
                    parts = order_time_str.split('.')
                    if len(parts) == 2:
                        milliseconds = parts[1]
                        # Pad milliseconds to 6 digits (microseconds) if needed
                        microseconds = milliseconds.ljust(6, '0')[:6]
                        order_time_str_padded = f"{parts[0]}.{microseconds}"
                        order_time = datetime.strptime(order_time_str_padded, "%Y-%m-%d %H:%M:%S.%f")
                    else:
                        order_time = datetime.strptime(order_time_str, "%Y-%m-%d %H:%M:%S")
                else:
                    # Without milliseconds: "2025-12-16 20:05:40"
                    order_time = datetime.strptime(order_time_str, "%Y-%m-%d %H:%M:%S")
                timestamp = order_time.timestamp()
            except (ValueError, TypeError):
                timestamp = 0
            
            # Get the side of the trade
            side = trade.get('Side', '')
            
            # Check if the trade is recent
            is_recent = (current_time - timestamp) < self.highlight_duration
            
            for col_idx, (col, formatter) in enumerate(zip(columns, formatters)):
                value = trade.get(col)
                
                # Format the value
                if pd.notna(value):
                    formatted_value = formatter(value)
                else:
                    formatted_value = ""
                
                # Create appropriate table item
                numeric_columns = [
                    "Amount", "Price", "Price_TRY", "Amount_TRY", "Amount_USDT", "USDTTRY_bid", 
                    "USDTTRY_ask", "Fee", "ExecutedMargin", "PNL"
                ]
                
                if col in numeric_columns and pd.notna(value):
                    # Use NumericTableWidgetItem for numeric values
                    try:
                        numeric_value = float(value)
                        item = NumericTableWidgetItem(str(formatted_value), numeric_value)
                        item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                    except (ValueError, TypeError):
                        item = QTableWidgetItem(str(formatted_value))
                else:
                    # Use regular QTableWidgetItem for non-numeric values
                    item = QTableWidgetItem(str(formatted_value))
                
                # Only set custom background color if the trade is recent
                if is_recent:
                    bg_color = QColor(0, 100, 0) if side == 'BUY' else QColor(139, 0, 0)
                    item.setBackground(bg_color)
                
                self.binance_trade_table.setItem(row_idx, col_idx, item)
            
            # Track the latest completed trade
            if trade.get('Status') == 'DONE' and (latest_done_trade is None or timestamp > latest_done_trade.get('timestamp', 0)):
                latest_done_trade = trade
                latest_done_trade['timestamp'] = timestamp  # Add timestamp for comparison
        
        # Resize columns to fit content on first update only
        if not hasattr(self, 'binance_first_update_done') or not self.binance_first_update_done:
            self.resize_columns_to_contents(self.binance_trade_table)
            self.binance_first_update_done = True
        else:
            # Restore column widths for subsequent updates
            for i, width in enumerate(column_widths):
                if i < self.binance_trade_table.columnCount():
                    self.binance_trade_table.setColumnWidth(i, width)
        
        # Restore column visibility
        for i, hidden in enumerate(column_hidden):
            if i < self.binance_trade_table.columnCount():
                self.binance_trade_table.setColumnHidden(i, hidden)
    
        # Re-enable sorting and restore previous sort
        self.binance_trade_table.setSortingEnabled(True)
        if sort_column < self.binance_trade_table.columnCount():
            self.binance_trade_table.horizontalHeader().setSortIndicator(sort_column, sort_order)
    
        # Show latest trade in status bar if available
        if latest_done_trade is not None:
            trade_time = latest_done_trade.get('OrderTime', 'Unknown')
            symbol = latest_done_trade.get('Symbol', 'Unknown')
            side = latest_done_trade.get('Side', 'Unknown')
            amount = latest_done_trade.get('Amount', 0)
            price = latest_done_trade.get('Price', 0)
            
            status_msg = f"{trade_time} - Last BINANCE Trade: {symbol} {side} {amount} @ {price}"
            self.statusBar().showMessage(status_msg)

    def update_maker_move_history(self, df):
        # Get total count of move history records from Redis and update tab title first
        try:
            total_count = self.redis.llen('maker_move_history')
            logging.debug(f"Move history Redis count: {total_count}")
        except Exception as e:
            logging.error(f"Error getting move history count: {e}")
            total_count = 0
        
        # Update tab title with count (always update, even if df is empty)
        tab_text = f"Move History ({total_count})"
        if hasattr(self, 'trade_history_tabs') and hasattr(self, 'move_history_tab_index'):
            try:
                self.trade_history_tabs.setTabText(self.move_history_tab_index, tab_text)
                logging.debug(f"Updated move history tab title to: {tab_text}")
            except Exception as e:
                logging.error(f"Error updating move history tab title: {e}")
        
        if df is None or df.empty:
            return
            
        # Define visible columns with their formatters
        columns = [
            "MoveTime", "OrderID", "Symbol", "Core", "BinanceExecutedMargin", 
            "EXCHANGEExecutedMargin", "StopMargin", "MoveThreshold", "MarginDifference", "PNL", "Source"
        ]
        formatters = [
            lambda x: x,  # MoveTime
            lambda x: x,  # OrderID
            lambda x: x,  # Symbol
            lambda x: x,  # Core
            lambda x: f"{float(x):.2f}" if pd.notna(x) else "",  # BinanceExecutedMargin
            lambda x: f"{float(x):.2f}" if pd.notna(x) else "",  # EXCHANGEExecutedMargin
            lambda x: f"{float(x):.2f}" if pd.notna(x) else "",  # StopMargin
            lambda x: f"{int(x)}" if pd.notna(x) else "",  # MoveThreshold
            lambda x: f"{float(x):.2f}" if pd.notna(x) else "",  # MarginDifference
            lambda x: f"₺{float(x):,.2f}" if pd.notna(x) else "",  # PNL
            lambda x: x,  # Source
        ]
        
        # Ensure all columns exist
        for col in columns:
            if col not in df.columns:
                df[col] = None
                
        # Save current sort order
        sort_column = self.move_history_table.horizontalHeader().sortIndicatorSection()
        sort_order = self.move_history_table.horizontalHeader().sortIndicatorOrder()
        
        # Save column widths only if not first update
        column_widths = []
        if hasattr(self, 'move_history_first_update_done') and self.move_history_first_update_done:
            for i in range(self.move_history_table.columnCount()):
                column_widths.append(self.move_history_table.columnWidth(i))
        
        # Temporarily disable sorting
        self.move_history_table.setSortingEnabled(False)
        
        # Clear existing rows
        self.move_history_table.setRowCount(0)
        
        # Add rows for each move
        self.move_history_table.setRowCount(len(df))
        
        # Populate table
        for row_idx, (_, move) in enumerate(df.iterrows()):
            for col_idx, (col, formatter) in enumerate(zip(columns, formatters)):
                value = move.get(col)
                
                # Format the value
                if pd.notna(value):
                    formatted_value = formatter(value)
                else:
                    formatted_value = ""
                
                # Create appropriate table item
                numeric_columns = [
                    "BinanceExecutedMargin", "EXCHANGEExecutedMargin", "StopMargin", "MarginDifference", "PNL"
                ]
                
                if col in numeric_columns and pd.notna(value):
                    # Use NumericTableWidgetItem for numeric values
                    try:
                        numeric_value = float(value)
                        item = NumericTableWidgetItem(str(formatted_value), numeric_value)
                        item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                    except (ValueError, TypeError):
                        item = QTableWidgetItem(str(formatted_value))
                else:
                    # Use regular QTableWidgetItem for non-numeric values
                    item = QTableWidgetItem(str(formatted_value))
                
                # Highlight negative margin difference in red
                if col == "MarginDifference" and pd.notna(value) and float(value) < 0:
                    item.setBackground(QColor(139, 0, 0))  # Dark red
                elif col == "MarginDifference" and pd.notna(value) and float(value) >= 0:
                    item.setBackground(QColor(0, 100, 0))  # Dark green
                
                self.move_history_table.setItem(row_idx, col_idx, item)
        
        # Resize columns to fit content on first update only
        if not hasattr(self, 'move_history_first_update_done') or not self.move_history_first_update_done:
            self.resize_columns_to_contents(self.move_history_table)
            self.move_history_first_update_done = True
        else:
            # Restore column widths for subsequent updates
            for i, width in enumerate(column_widths):
                if i < self.move_history_table.columnCount():
                    self.move_history_table.setColumnWidth(i, width)
    
        # Re-enable sorting and restore previous sort
        self.move_history_table.setSortingEnabled(True)
        if sort_column < self.move_history_table.columnCount():
            self.move_history_table.horizontalHeader().setSortIndicator(sort_column, sort_order)

    def update_EXCHANGE_open_orders(self, df):
        """Update the EXCHANGE open orders table with new data"""
        if df is None:
            return

        try:
            # Ensure DataFrame has all required columns even if empty
            if df.empty:
                df = pd.DataFrame(columns=self.EXCHANGE_open_orders_columns)

            # Calculate totals
            order_count = len(df)
            buy_order_count = 0
            sell_order_count = 0
            
            if not df.empty and 'Side' in df.columns:

                buy_order_count = len(df[df['Side'] == 'BUY'])
                
                sell_order_count = len(df[df['Side'] == 'SELL'])
            
            # Update order count label with totals
            if hasattr(self, 'EXCHANGE_open_orders_label'):
                self.EXCHANGE_open_orders_label.setText(
                    f"EXCHANGE OPEN ORDERS (Buy: {buy_order_count} / Sell: {sell_order_count} / TOTAL: {order_count})"
                )
        except Exception as e:
            logging.error(f"Error updating order count: {e}")

        # Save current search text
        EXCHANGE_open_orders_search = self.EXCHANGE_open_orders_search_input.text() if hasattr(self, 'EXCHANGE_open_orders_search_input') else ""

        # Update EXCHANGE open orders table
        self.EXCHANGE_open_orders_table.setSortingEnabled(False)
        self.EXCHANGE_open_orders_table.setRowCount(0)
        
        # Get current time for highlighting recent orders
        current_time = time.time()
        
        # Update table with new data using proper numeric sorting
        self.EXCHANGE_open_orders_table.setSortingEnabled(False)
        self.EXCHANGE_open_orders_table.setRowCount(0)
        
        # Check if DataFrame is empty
        if df.empty:
            self.EXCHANGE_open_orders_table.setSortingEnabled(True)
            return
        
        for index, row in df.iterrows():
            try:
                row_position = self.EXCHANGE_open_orders_table.rowCount()
                self.EXCHANGE_open_orders_table.insertRow(row_position)
                
                # OrderTime
                self.EXCHANGE_open_orders_table.setItem(row_position, 0, QTableWidgetItem(str(row.get('OrderTime', ''))))
                
                # OrderId
                self.EXCHANGE_open_orders_table.setItem(row_position, 1, QTableWidgetItem(str(row.get('OrderId', ''))))
                
                # Symbol
                self.EXCHANGE_open_orders_table.setItem(row_position, 2, QTableWidgetItem(str(row.get('Symbol', ''))))
                
                # Core
                self.EXCHANGE_open_orders_table.setItem(row_position, 3, QTableWidgetItem(str(row.get('Core', ''))))
                
                # Side
                self.EXCHANGE_open_orders_table.setItem(row_position, 4, QTableWidgetItem(str(row.get('Side', ''))))
                
                # Price
                price_value = float(row.get('Price', 0)) if pd.notna(row.get('Price', 0)) else 0
                price_item = self.NumericTableWidgetItem(f"₺{price_value}", price_value)
                self.EXCHANGE_open_orders_table.setItem(row_position, 5, price_item)
                
                # Amount
                amount_value = float(row.get('Amount', 0)) if pd.notna(row.get('Amount', 0)) else 0
                amount_item = self.NumericTableWidgetItem(f"{amount_value}", amount_value)
                self.EXCHANGE_open_orders_table.setItem(row_position, 6, amount_item)
                
                # AmountTRY
                amount_try_value = float(row.get('AmountTRY', 0)) if pd.notna(row.get('AmountTRY', 0)) else 0
                amount_try_item = self.NumericTableWidgetItem(f"₺{amount_try_value:,.2f}", amount_try_value)
                self.EXCHANGE_open_orders_table.setItem(row_position, 7, amount_try_item)
                
                # TriggerMargin
                trigger_margin_value = float(row.get('TriggerMargin', 0)) if pd.notna(row.get('TriggerMargin', 0)) else 0
                trigger_margin_item = self.NumericTableWidgetItem(f"{trigger_margin_value*100:,.4f}%", trigger_margin_value*100)
                self.EXCHANGE_open_orders_table.setItem(row_position, 8, trigger_margin_item)
            except Exception as e:
                logging.error(f"Error processing row {index} in open orders table: {e}")
                continue
        
        # Re-enable sorting
        self.EXCHANGE_open_orders_table.setSortingEnabled(True)
        
        # Apply background colors for recent orders and side-based coloring
        for row in range(self.EXCHANGE_open_orders_table.rowCount()):
            order_time_item = self.EXCHANGE_open_orders_table.item(row, 0)
            side_item = self.EXCHANGE_open_orders_table.item(row, 4)
            
            if order_time_item and side_item:
                try:
                    # Parse the order time
                    order_time = datetime.strptime(order_time_item.text(), "%Y-%m-%d %H:%M:%S")
                    timestamp = order_time.timestamp()
                    
                    # Check if order is recent (within 5 seconds)
                    if current_time - timestamp <= 5:
                        # Set background color based on side for recent orders
                        bg_color = QColor(0, 100, 0) if side_item.text() == 'BUY' else QColor(139, 0, 0) if side_item.text() == 'SELL' else QColor(128, 0, 128)
                        
                        # Apply color to all cells in the row
                        for col in range(self.EXCHANGE_open_orders_table.columnCount()):
                            item = self.EXCHANGE_open_orders_table.item(row, col)
                            if item:
                                item.setBackground(bg_color)
                    else:
                        # Apply subtle background color based on side for older orders
                        if side_item.text() == 'BUY':
                            bg_color = QColor(0, 50, 0, 50)  # Light green with transparency
                        elif side_item.text() == 'SELL':
                            bg_color = QColor(50, 0, 0, 50)  # Light red with transparency
                        else:
                            bg_color = QColor(50, 0, 50, 50)  # Light purple with transparency
                        
                        # Apply subtle color to all cells in the row
                        for col in range(self.EXCHANGE_open_orders_table.columnCount()):
                            item = self.EXCHANGE_open_orders_table.item(row, col)
                            if item:
                                item.setBackground(bg_color)
                except (ValueError, TypeError):
                    continue
        
        # Reapply search filter if there was any
        if EXCHANGE_open_orders_search:
            self.filter_EXCHANGE_open_orders_without_updating_input(EXCHANGE_open_orders_search)

    def update_EXCHANGE_open_orders_without_updating_input(self, search_text):
        """Filter EXCHANGE open orders table based on search text without updating the input field"""
        for row in range(self.EXCHANGE_open_orders_table.rowCount()):
            symbol_item = self.EXCHANGE_open_orders_table.item(row, 2)  # Symbol is at the third column (index 2)
            if symbol_item:
                symbol = symbol_item.text()
                if search_text and search_text not in symbol:
                    self.EXCHANGE_open_orders_table.hideRow(row)
                else:
                    self.EXCHANGE_open_orders_table.showRow(row)
        
        
    def resize_columns_to_contents(self, table):
        """Resize columns to fit content"""
        table.resizeColumnsToContents()

    def update_balances_and_pnl(self):
        try:
            # Get arbitrage table from Redis
            pickled_data = self.redis.get('maker_arbitrage_table')
            
            if pickled_data:
                df = pickle.loads(pickled_data)
            else:
                df = None
                logging.warning("No data found in Redis for maker_arbitrage_table")
            
            if df is None or df.empty:
                self.statusBar().showMessage("No arbitrage data available")
                return
            
            # Save current sort settings
            sort_column = self.settings_table.horizontalHeader().sortIndicatorSection()
            sort_order = self.settings_table.horizontalHeader().sortIndicatorOrder()
            
            # Temporarily disable sorting
            self.settings_table.setSortingEnabled(False)
            
            EXCHANGE_balances = df.iloc[:, arbit_config_maker.col_EXCHANGE_PositionAmount_TRY_total]
            
            symbols = df.iloc[:, arbit_config_maker.col_Base_Symbol]
            
            # Update symbol table balances
            for row in range(self.settings_table.rowCount()):
                symbol_item = self.settings_table.item(row, 0)
                if symbol_item:
                    symbol = symbol_item.text()
                    # Find matching symbol in arbitrage data
                    symbol_idx = symbols[symbols == symbol].index[0] if symbol in symbols.values else None
                    if symbol_idx is not None:
                        # Get balance for this symbol
                        balance = EXCHANGE_balances.iloc[symbol_idx]
                        # Create balance item with proper formatting and alignment
                        # Use StableNumericTableWidgetItem instead of QTableWidgetItem
                        balance_item = self.StableNumericTableWidgetItem(f"₺{balance:,.0f}", balance, symbol)
                        balance_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                        
                        # The current position column
                        self.settings_table.setItem(row, self.current_position_TRY_index, balance_item)
            
            # Re-enable sorting
            self.settings_table.setSortingEnabled(True)
            
            # Restore sort column and order
            if sort_column >= 0 and sort_column < self.settings_table.columnCount():
                self.settings_table.sortItems(sort_column, sort_order)
            
             # Get the column indices from arbit_config
            binance_free_idx = arbit_config_maker.col_Binance_free_usdt
            EXCHANGE_free_idx = arbit_config_maker.col_EXCHANGE_free_TRY
            usdttry_bid_idx = arbit_config_maker.col_USDTTRY_bid

            # Get balances from the arbitrage table using pandas iloc
            binance_usdt = float(df.iloc[0, binance_free_idx])
            EXCHANGE_try = float(df.iloc[0, EXCHANGE_free_idx])
            usdttry_bid = float(df.iloc[0, usdttry_bid_idx])
            
            # Track EXCHANGE balance history for 10-second moving average (keep last 10 readings)
            if not hasattr(self, 'EXCHANGE_balance_history'):
                self.EXCHANGE_balance_history = []
            self.EXCHANGE_balance_history.append(EXCHANGE_try)
            if len(self.EXCHANGE_balance_history) > 10:
                self.EXCHANGE_balance_history.pop(0)  # Keep only last 10 readings
            
            # Calculate USDT value in TRY
            binance_try = binance_usdt * usdttry_bid
            
            # Update balances label
            self.balances_label.setText(
                f"""
                <div style="line-height: 1.4;">
                    Binance: ${binance_usdt:,.2f} (₺{binance_try:,.2f})<br>
                    EXCHANGE: ₺{EXCHANGE_try:,.2f}
                </div>
                """
            )
            
            # Get cumulative PNL and positions
            pnl = self.get_cumulative_pnl()
            tl_pos, total_volume, usdt_pos = self.get_TL_usdt_pos()
            
            # Update PNL labels
            self.pnl_label.setText(
                f"""
                <div style="line-height: 1.4;">
                    CumPNL: ₺{pnl:,.2f}<br>
                    Total Volume: ${total_volume:,.2f}
                </div>
                """
            )
            
            avg_usdttry = abs(tl_pos / usdt_pos) if usdt_pos != 0 else 0.0
            self.pnl2_label.setText(
                f"""
                <div style="line-height: 1.4;">
                    USDT_Pos: {usdt_pos:,.2f}<br>
                    AVG_USDTTRY: {avg_usdttry:,.2f}
                </div>
                """
            )
        except Exception as e:
            logging.error(f"Error updating balances and PNL: {e}")

    def get_cumulative_pnl(self):
        """Get cumulative PNL from trade history"""
        try:
            binance_trade_records = []
            binance_trade_records_data = self.redis.lrange('maker_BINANCE_trade_history', 0, -1)
            
            for record in binance_trade_records_data:
                binance_trade_records.append(json.loads(record))
            
            if binance_trade_records:
                binance_trade_records_df = pd.DataFrame(binance_trade_records)
                cumulative_pnl = binance_trade_records_df['PNL'].sum()
                return cumulative_pnl
            return 0.0
        except Exception as e:
            logging.error(f"Error calculating cumulative PNL: {e}")
            return 0.0
    
    def get_TL_usdt_pos(self):
        """Get TL position, total volume and USDT positions from trade history"""
        try:
            # First check if the key exists
            if not self.redis.exists('maker_BINANCE_trade_history'):
                return [0.0, 0.0, 0.0]
                
            trade_records_data = self.redis.lrange('maker_BINANCE_trade_history', 0, -1)
            binance_trade_records = [json.loads(record) for record in trade_records_data]
            
            if not binance_trade_records:
                return [0.0, 0.0, 0.0]
            
            TL_pos = 0.0
            total_volume = 0.0
            USDT_pos = 0.0
            trade_no = 0
            USDT_pos_cum = 0.0
            for trade in binance_trade_records:
                # Sum up Amount_USDT column for total volume
                if 'Amount_USDT' in trade:
                    total_volume += float(trade['Amount_USDT'])
                
                # Calculate TL position and USDT position
                filled_qty = float(trade['Amount'])
                filled_price = float(trade['Price'])    
                usdttry_bid = float(trade['USDTTRY_bid'])
                usdttry_ask = float(trade['USDTTRY_ask'])
                side = trade['Side']
                trade_no += 1
                if side == 'BUY':
                    USDT_pos = -1 * filled_qty * filled_price
                    TL_pos += filled_qty * filled_price * usdttry_ask
                    USDT_pos_cum += USDT_pos
                elif side == 'SELL':
                    USDT_pos = filled_qty * filled_price    
                    TL_pos += -1 * filled_qty * filled_price * usdttry_bid
                    USDT_pos_cum += USDT_pos
                else:
                    logging.error(f"Invalid side: {side}")
                    return [0.0, 0.0, 0.0]
                    
            return [TL_pos, total_volume, USDT_pos_cum]
            
        except Exception as e:
            logging.error(f"Error calculating TL position and total volume: {e}")
            logging.error(f"Error type: {type(e)}")
            return [0.0, 0.0, 0.0]
            
    def setup_settings_page(self):
        """Set up the settings page"""
        # Set Turkish locale for thousand separators (dots)
        turkish_locale = QLocale(QLocale.Language.Turkish, QLocale.Country.Turkey)
        
        # Create layout for settings page
        settings_layout = QVBoxLayout(self.settings_page)
        settings_layout.setContentsMargins(5, 5, 5, 5)
        settings_layout.setSpacing(10)
        
        # Create controls panel at the top
        controls_panel = QWidget()
        controls_layout = QHBoxLayout(controls_panel)
        controls_layout.setContentsMargins(5, 5, 5, 5)
        
        # Left side - Margin controls
        margin_group = QGroupBox("Margin Settings")
        margin_group.setStyleSheet("QGroupBox { font-weight: bold; border: 2px solid #555555; border-radius: 5px; margin-top: 1ex; padding-top: 10px; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; }")
        margin_layout = QGridLayout(margin_group)
        
        # Open Trigger Margin
        margin_layout.addWidget(QLabel("Buy Trigger Margin (bps):"), 0, 0)
        self.open_margin_input = QSpinBox()
        self.open_margin_input.setLocale(turkish_locale)
        self.open_margin_input.setRange(-1000, 1000)
        self.open_margin_input.setValue(40)
        self.open_margin_input.setGroupSeparatorShown(True)
        margin_layout.addWidget(self.open_margin_input, 0, 1)
        update_open_btn = QPushButton("Update")
        update_open_btn.clicked.connect(self.update_open_trigger_margin)
        margin_layout.addWidget(update_open_btn, 0, 2)
        
        # Combined update button for open margins that spans two rows
        update_open_combined_btn = QPushButton("Update Buy Margins\n(without exceptionals)")
        update_open_combined_btn.clicked.connect(self.update_open_trigger_margin_and_window)
        update_open_combined_btn.setMinimumHeight(50)  # Make it taller to span two rows
        margin_layout.addWidget(update_open_combined_btn, 0, 3, 2, 1)  # span 2 rows
        
        # Open Margin Window
        margin_layout.addWidget(QLabel("Buy Margin Window (bps):"), 1, 0)
        self.open_margin_window_input = QSpinBox()
        self.open_margin_window_input.setLocale(turkish_locale)
        self.open_margin_window_input.setRange(5, 100)
        self.open_margin_window_input.setValue(10)
        self.open_margin_window_input.setGroupSeparatorShown(True)
        margin_layout.addWidget(self.open_margin_window_input, 1, 1)
        update_open_margin_window_btn = QPushButton("Update")
        update_open_margin_window_btn.clicked.connect(self.update_open_margin_window)
        margin_layout.addWidget(update_open_margin_window_btn, 1, 2)

        # Close Trigger Margin
        margin_layout.addWidget(QLabel("Sell Trigger Margin (bps):"), 2, 0)
        self.close_margin_input = QSpinBox()
        self.close_margin_input.setLocale(turkish_locale)
        self.close_margin_input.setRange(-1000, 1000)
        self.close_margin_input.setValue(40)
        self.close_margin_input.setGroupSeparatorShown(True)
        margin_layout.addWidget(self.close_margin_input, 2, 1)
        update_close_btn = QPushButton("Update")
        update_close_btn.clicked.connect(self.update_close_trigger_margin)
        margin_layout.addWidget(update_close_btn, 2, 2)

        # Combined update button for close margins that spans two rows
        update_close_combined_btn = QPushButton("Update Sell Margins\n(without exceptionals)")
        update_close_combined_btn.clicked.connect(self.update_close_trigger_margin_and_window)
        update_close_combined_btn.setMinimumHeight(50)  # Make it taller to span two rows
        margin_layout.addWidget(update_close_combined_btn, 2, 3, 2, 1)  # span 2 rows

        # Close Margin Window
        margin_layout.addWidget(QLabel("Sell Margin Window (bps):"), 3, 0)
        self.close_margin_window_input = QSpinBox()
        self.close_margin_window_input.setLocale(turkish_locale)
        self.close_margin_window_input.setRange(5, 100)
        self.close_margin_window_input.setValue(10)
        self.close_margin_window_input.setGroupSeparatorShown(True)
        margin_layout.addWidget(self.close_margin_window_input, 3, 1)
        update_close_margin_window_btn = QPushButton("Update")
        update_close_margin_window_btn.clicked.connect(self.update_close_margin_window)
        margin_layout.addWidget(update_close_margin_window_btn, 3, 2)


        # Buy & SellAggression
        margin_layout.addWidget(QLabel("Buy/Sell Aggression (bps):"), 4, 0)
        self.open_aggression_input = QSpinBox()
        self.open_aggression_input.setLocale(turkish_locale)
        self.open_aggression_input.setRange(1, 20)
        self.open_aggression_input.setValue(2)
        self.open_aggression_input.setGroupSeparatorShown(True)
        margin_layout.addWidget(self.open_aggression_input, 4, 1)
       
        self.close_aggression_input = QSpinBox()
        self.close_aggression_input.setLocale(turkish_locale)
        self.close_aggression_input.setRange(1, 20)
        self.close_aggression_input.setValue(2)
        self.close_aggression_input.setGroupSeparatorShown(True)
        margin_layout.addWidget(self.close_aggression_input, 4, 2)

        update_buy_sell_aggressions_btn = QPushButton("Update")
        update_buy_sell_aggressions_btn.clicked.connect(self.update_buy_sell_aggressions)
        margin_layout.addWidget(update_buy_sell_aggressions_btn, 4, 3)

       

       

        # Move Threshold
        margin_layout.addWidget(QLabel("Move Threshold (bps):"), 5, 0)
        self.move_threshold_input = QSpinBox()
        self.move_threshold_input.setLocale(turkish_locale)
        self.move_threshold_input.setRange(1, 1000)
        self.move_threshold_input.setSingleStep(1)
        self.move_threshold_input.setGroupSeparatorShown(True)
        try:
            # Try to get initial value from Redis thresholds
            thresholds_data = self.redis.get('maker_arbitrage_thresholds')
            if thresholds_data:
                thresholds = json.loads(thresholds_data.decode('utf-8'))
                if thresholds and isinstance(thresholds, list) and len(thresholds) > 0:
                    initial_move_threshold = thresholds[0].get('MoveThreshold', 15)
                    self.move_threshold_input.setValue(int(initial_move_threshold))
                else:
                    self.move_threshold_input.setValue(15)
            else:
                    self.move_threshold_input.setValue(15)
        except Exception as e:
            logging.error(f"Error getting move threshold: {e}")
            self.move_threshold_input.setValue(15)
        margin_layout.addWidget(self.move_threshold_input, 5, 1)


        # Exceptional Margin
        self.exceptional_margin_checkbox = QCheckBox("Exceptional")
        margin_layout.addWidget(self.exceptional_margin_checkbox, 5, 2)
        update_move_and_exceptional_threshold_btn = QPushButton("Update")
        update_move_and_exceptional_threshold_btn.clicked.connect(self.update_move_and_exceptional_threshold)
        margin_layout.addWidget(update_move_and_exceptional_threshold_btn, 5, 3)

        # Middle - Global Settings
        global_group = QGroupBox("Global Settings")
        global_group.setStyleSheet("QGroupBox { font-weight: bold; border: 2px solid #555555; border-radius: 5px; margin-top: 1ex; padding-top: 10px; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; }")
        global_layout = QGridLayout(global_group)
        

        
        # Create checkbox to toggle
        self.auto_update_buy_checkbox = QCheckBox("Enable")
        self.auto_update_buy_checkbox.stateChanged.connect(self.toggle_auto_update_buy_order_amounts)
        
        # Load saved state from Redis
        try:
            saved_state = self.redis.get('maker_AutoUpdateBuyOrderEnabled')
            if saved_state and saved_state.decode('utf-8') == '1':
                self.auto_update_buy_checkbox.setChecked(True)
                self.auto_update_buy_indicator.setStyleSheet("background-color: orange; border: 1px solid #777777;")
        except Exception as e:
            logging.error(f"Error loading auto-update buy order state: {e}")
        
        # Load values from Redis for labels
        saved_buy_period = 10
        saved_target_tl = 750000
        saved_range_period = 15
        try:
            buy_period_data = self.redis.get('maker_AutoUpdateBuyOrderPeriod')
            if buy_period_data:
                saved_buy_period = int(buy_period_data.decode('utf-8'))
        except Exception as e:
            logging.error(f"Error loading auto-update buy order period: {e}")
        try:
            target_tl_data = self.redis.get('maker_TargetAccountFreeTL')
            if target_tl_data:
                saved_target_tl = float(target_tl_data.decode('utf-8'))
        except Exception as e:
            logging.error(f"Error loading target account free TL: {e}")
        try:
            range_period_data = self.redis.get('maker_AutoRangeMarginUpdatePeriod')
            if range_period_data:
                saved_range_period = int(range_period_data.decode('utf-8'))
        except Exception as e:
            logging.error(f"Error loading auto range margin update period: {e}")
        
        # Auto update buy order amounts
        auto_update_enabled = "Enabled" if self.auto_update_buy_checkbox.isChecked() else "Disabled"
        self.auto_update_buy_label = QLabel(f"Auto update buy order amounts: ({auto_update_enabled})")
        global_layout.addWidget(self.auto_update_buy_label, 0, 0)
        global_layout.addWidget(self.auto_update_buy_checkbox, 0, 1)
       

        # Auto update buy order amounts period
        self.auto_update_buy_period_label = QLabel(f"Auto update buy order amounts period (s): ({saved_buy_period})")
        global_layout.addWidget(self.auto_update_buy_period_label, 1, 0)
        self.auto_update_buy_period_input = QSpinBox()
        self.auto_update_buy_period_input.setLocale(turkish_locale)
        self.auto_update_buy_period_input.setRange(10, 60)
        self.auto_update_buy_period_input.setSingleStep(5)
        self.auto_update_buy_period_input.setValue(saved_buy_period)
        global_layout.addWidget(self.auto_update_buy_period_input, 1, 1)
        update_buy_period_btn = QPushButton("Update")
        update_buy_period_btn.clicked.connect(self.update_auto_update_buy_period)
        global_layout.addWidget(update_buy_period_btn, 1, 2)
        
        # Target Account Free TL
        formatted_target_tl = f"{int(saved_target_tl):,}".replace(',', '.')
        self.target_account_free_tl_label = QLabel(f"Target Account Free TL: ({formatted_target_tl})")
        global_layout.addWidget(self.target_account_free_tl_label, 2, 0)
        self.target_account_free_tl_input = QDoubleSpinBox()
        self.target_account_free_tl_input.setLocale(turkish_locale)
        self.target_account_free_tl_input.setRange(0, 1000000)
        self.target_account_free_tl_input.setSingleStep(5000)
        self.target_account_free_tl_input.setDecimals(0)
        self.target_account_free_tl_input.setValue(saved_target_tl)
        self.target_account_free_tl_input.setGroupSeparatorShown(True)
        global_layout.addWidget(self.target_account_free_tl_input, 2, 1)
        update_target_tl_btn = QPushButton("Update")
        update_target_tl_btn.clicked.connect(self.update_target_account_free_tl)
        global_layout.addWidget(update_target_tl_btn, 2, 2)
        
        # Auto Range Margin Update Period
        self.auto_range_margin_period_label = QLabel(f"Auto Range Margin Update Period (s): ({saved_range_period})")
        global_layout.addWidget(self.auto_range_margin_period_label, 3, 0)
        self.auto_update_interval_input = QSpinBox()
        self.auto_update_interval_input.setLocale(turkish_locale)
        self.auto_update_interval_input.setRange(10, 60)
        self.auto_update_interval_input.setSingleStep(5)
        self.auto_update_interval_input.setValue(saved_range_period)

        self.auto_update_interval_input.valueChanged.connect(self.update_auto_update_interval)
        global_layout.addWidget(self.auto_update_interval_input, 3, 1)
        update_range_period_btn = QPushButton("Update")
        update_range_period_btn.clicked.connect(self.update_auto_range_margin_period)
        global_layout.addWidget(update_range_period_btn, 3, 2)
        
        # Right side - Order Settings (reorganized to match image)
        order_group = QGroupBox("Order Settings")
        order_group.setStyleSheet("QGroupBox { font-weight: bold; border: 2px solid #555555; border-radius: 5px; margin-top: 1ex; padding-top: 10px; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; }")
        order_layout = QGridLayout(order_group)
        
        # Buy Order Amount TL (Norm)
        order_layout.addWidget(QLabel("Buy Order Amount TL (Norm):"), 0, 0)
        self.buy_order_amount_input = QSpinBox()
        self.buy_order_amount_input.setLocale(turkish_locale)
        self.buy_order_amount_input.setRange(5000, 200000)
        self.buy_order_amount_input.setSingleStep(5000)
        self.buy_order_amount_input.setValue(40000)
        self.buy_order_amount_input.setAlignment(Qt.AlignmentFlag.AlignRight)
        self.buy_order_amount_input.setGroupSeparatorShown(True)
        order_layout.addWidget(self.buy_order_amount_input, 0, 1, 1, 2)
        update_buy_order_amount_btn = QPushButton("Update")
        update_buy_order_amount_btn.clicked.connect(self.update_buy_order_amount)
        order_layout.addWidget(update_buy_order_amount_btn, 0, 3)
        
        # Buy Order Amount Limits TL (Min/Max)
        order_layout.addWidget(QLabel("Buy Order Amount Limits TL (Min/Max):"), 1, 0)
        self.min_buy_order_input = QSpinBox()
        self.min_buy_order_input.setLocale(turkish_locale)
        self.min_buy_order_input.setRange(5000, 100000)
        self.min_buy_order_input.setSingleStep(5000)
        self.min_buy_order_input.setValue(5000)
        self.min_buy_order_input.setGroupSeparatorShown(True)
        order_layout.addWidget(self.min_buy_order_input, 1, 1)
        self.max_buy_order_input = QSpinBox()
        self.max_buy_order_input.setLocale(turkish_locale)
        self.max_buy_order_input.setRange(5000, 200000)
        self.max_buy_order_input.setSingleStep(5000)
        self.max_buy_order_input.setValue(100000)
        self.max_buy_order_input.setGroupSeparatorShown(True)
        # Connect max buy order input to update buy order amount maximum
        self.max_buy_order_input.valueChanged.connect(self.update_buy_order_amount_maximum)
        order_layout.addWidget(self.max_buy_order_input, 1, 2)
        update_buy_order_amounts_btn = QPushButton("Update")
        update_buy_order_amounts_btn.clicked.connect(self.update_buy_order_amounts)
        order_layout.addWidget(update_buy_order_amounts_btn, 1, 3)
    
        # Sell Order Amount Limits TL (Min/Max)
        order_layout.addWidget(QLabel("Sell Order Amount Limits TL (Min/Max):"), 2, 0)
        self.min_sell_order_input = QSpinBox()
        self.min_sell_order_input.setLocale(turkish_locale)
        self.min_sell_order_input.setRange(1000, 100000)
        self.min_sell_order_input.setSingleStep(1000)
        self.min_sell_order_input.setValue(1000)
        self.min_sell_order_input.setGroupSeparatorShown(True)
        order_layout.addWidget(self.min_sell_order_input, 2, 1)
        self.max_sell_order_input = QSpinBox()
        self.max_sell_order_input.setLocale(turkish_locale)
        self.max_sell_order_input.setRange(0, 200000)
        self.max_sell_order_input.setSingleStep(5000)
        self.max_sell_order_input.setValue(120000)
        self.max_sell_order_input.setGroupSeparatorShown(True)
        order_layout.addWidget(self.max_sell_order_input, 2, 2)
        update_sell_order_amounts_btn = QPushButton("Update")
        update_sell_order_amounts_btn.clicked.connect(self.update_sell_order_amounts)
        order_layout.addWidget(update_sell_order_amounts_btn, 2, 3)
        
        # Sell Order Amount Ratio (Min/Max)
        order_layout.addWidget(QLabel("Sell Order Amount Ratio (Min/Max):"), 3, 0)
        self.sell_order_min_ratio_input = QDoubleSpinBox()
        self.sell_order_min_ratio_input.setRange(0.0, 1.0)
        self.sell_order_min_ratio_input.setSingleStep(0.1)
        self.sell_order_min_ratio_input.setDecimals(1)
        self.sell_order_min_ratio_input.setValue(0.4)
        self.sell_order_min_ratio_input.valueChanged.connect(self.validate_min_ratio_changed)
        order_layout.addWidget(self.sell_order_min_ratio_input, 3, 1)
        self.sell_order_max_ratio_input = QDoubleSpinBox()
        self.sell_order_max_ratio_input.setRange(0.0, 1.0)
        self.sell_order_max_ratio_input.setSingleStep(0.1)
        self.sell_order_max_ratio_input.setDecimals(1)
        self.sell_order_max_ratio_input.setValue(0.6)
        self.sell_order_max_ratio_input.valueChanged.connect(self.validate_max_ratio_changed)
        order_layout.addWidget(self.sell_order_max_ratio_input, 3, 2)
        sell_ratio_widget = QWidget()
        update_sell_ratios_btn = QPushButton("Update")
        update_sell_ratios_btn.clicked.connect(self.update_sell_ratios)
        order_layout.addWidget(update_sell_ratios_btn, 3, 3)
        
        # Max Position Amount (TL)
        order_layout.addWidget(QLabel("Max Position Amount (TL):"), 4, 0)
        self.max_position_input = QSpinBox()
        self.max_position_input.setLocale(turkish_locale)
        self.max_position_input.setRange(5000, 1000000)
        self.max_position_input.setSingleStep(5000)
        self.max_position_input.setValue(100000)
        self.max_position_input.setGroupSeparatorShown(True)
        order_layout.addWidget(self.max_position_input, 4, 1, 1, 2)  # Span columns 1 and 2
        update_max_btn = QPushButton("Update")
        update_max_btn.clicked.connect(self.update_max_position)
        order_layout.addWidget(update_max_btn, 4, 3)

        # Add groups to controls layout - 3 group boxes side by side
        controls_layout.addWidget(margin_group, 1)
        controls_layout.addWidget(global_group, 1)
        controls_layout.addWidget(order_group, 1)
        
        # Set fixed height for controls panel to prevent window expansion
        controls_panel.setFixedHeight(230)
        
        # Add controls panel to main layout
        settings_layout.addWidget(controls_panel)
        
        # Add some spacing between controls and filter panels
        settings_layout.addSpacing(10)

        # --- BOTTOM SECTION: TWO COLUMNS (Filter + Table) ---
        bottom_splitter = QSplitter(Qt.Orientation.Horizontal)
        bottom_splitter.setChildrenCollapsible(False)

        # --- COLUMN 1: FILTERS ---
        filter_widget = QWidget()
        filter_layout = QVBoxLayout(filter_widget)
        filter_layout.setContentsMargins(5, 5, 5, 5)
        
        # Filter group box
        filter_group = QGroupBox("Filters")
        filter_group.setStyleSheet("QGroupBox { font-weight: bold; border: 2px solid #555555; border-radius: 5px; margin-top: 1ex; padding-top: 10px; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; }")
        filter_group_layout = QVBoxLayout(filter_group)
        
        # Search box with clear button on the right
        filter_group_layout.addWidget(QLabel("Search Symbol:"))
        search_layout = QHBoxLayout()
        self.symbol_search_input = QLineEdit()
        self.symbol_search_input.setPlaceholderText("Type to search symbols...")
        self.symbol_search_input.textChanged.connect(self.filter_symbols)
        self.symbol_search_input.textEdited.connect(self.convert_to_uppercase)
        search_layout.addWidget(self.symbol_search_input)
        
        # Clear button for search (on the right of input)
        clear_btn = QPushButton("Clear")
        clear_btn.clicked.connect(lambda: self.symbol_search_input.clear())
        search_layout.addWidget(clear_btn)
        filter_group_layout.addLayout(search_layout)

        filter_group_layout.addSpacing(10)
        
        # Range Margins label with Select All checkbox on the right
        range_margins_layout = QHBoxLayout()
        range_margins_layout.addWidget(QLabel("Range Margins:"))
        range_margins_layout.addStretch()
        # Select All checkbox
        self.select_all_ranges_checkbox = QCheckBox("Select All")
        self.select_all_ranges_checkbox.stateChanged.connect(self.toggle_all_range_selections)
        range_margins_layout.addWidget(self.select_all_ranges_checkbox)
        filter_group_layout.addLayout(range_margins_layout)

        # Define the range boundaries (excluding "Select All")
        self.range_list = ["Up to 100K", "100K - 200K", "200K - 300K", "300K - 400K", "400K - 500K", "500K - 600K", "600K - 700K", "700K - 800K", "800K - 900K", "900K+"]
        self.range_boundaries = {
            "Up to 100K": (0, 100000),
            "100K - 200K": (100000, 200000),
            "200K - 300K": (200000, 300000),
            "300K - 400K": (300000, 400000),
            "400K - 500K": (400000, 500000),
            "500K - 600K": (500000, 600000),
            "600K - 700K": (600000, 700000),
            "700K - 800K": (700000, 800000),
            "800K - 900K": (800000, 900000),
            "900K+": (900000, 10000000)
        }
        
        # Initialize range margins storage (open, close in bps)
        self.range_margins = {}
        for range_name in self.range_list:
            self.range_margins[range_name] = {"open": 40, "close": 40}  # Default values
        
        # Create table widget for range inputs with row selection
        self.range_table = QTableWidget()
        self.range_table.setColumnCount(3)
        self.range_table.setHorizontalHeaderLabels(["Range", "Open", "Close"])
        self.range_table.setRowCount(len(self.range_list))
        self.range_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.range_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.range_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.range_table.setAlternatingRowColors(True)
        self.range_table.verticalHeader().setVisible(False)
        # Set height to accommodate all 10 ranges without scrollbar (approx 30px per row + header)
        self.range_table.setMinimumHeight(350)
        self.range_table.setMaximumHeight(600)
        
        # Set column widths to avoid horizontal scrolling
        header = self.range_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)  # Range column stretches
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)    # Open column fixed
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)    # Close column fixed
        self.range_table.setColumnWidth(1, 70)  # Open column width
        self.range_table.setColumnWidth(2, 70)  # Close column width
        
        # Store range input widgets for later access
        self.range_inputs = {}
        
        # Populate table with ranges and inputs
        for i, range_name in enumerate(self.range_list):
            # Range name (read-only)
            range_item = QTableWidgetItem(range_name)
            range_item.setFlags(range_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.range_table.setItem(i, 0, range_item)
            
            # Open margin input
            open_input = QSpinBox()
            open_input.setLocale(turkish_locale)
            open_input.setRange(-1000, 1000)
            open_input.setValue(self.range_margins[range_name]["open"])
            open_input.setGroupSeparatorShown(True)
            open_input.setMinimumWidth(65)
            open_input.setMaximumWidth(65)
            self.range_table.setCellWidget(i, 1, open_input)
            
            # Close margin input
            close_input = QSpinBox()
            close_input.setLocale(turkish_locale)
            close_input.setRange(-1000, 1000)
            close_input.setValue(self.range_margins[range_name]["close"])
            close_input.setGroupSeparatorShown(True)
            close_input.setMinimumWidth(65)
            close_input.setMaximumWidth(65)
            self.range_table.setCellWidget(i, 2, close_input)
            
            # Store inputs
            self.range_inputs[range_name] = {"open": open_input, "close": close_input}
        
        # Connect selection change to filter function
        self.range_table.itemSelectionChanged.connect(self.filter_symbols_by_position_ranges)
        
        # Enable toggle selection (clicking selected row deselects it)
        self.range_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        # Track mouse state for drag detection
        self.range_table_mouse_press_pos = None
        self.range_table_is_dragging = False
        # Override mouse events to enable toggle selection on click and drag selection
        self.range_table.mousePressEvent = self.range_table_mouse_press_event
        self.range_table.mouseMoveEvent = self.range_table_mouse_move_event
        self.range_table.mouseReleaseEvent = self.range_table_mouse_release_event
        
        filter_group_layout.addWidget(self.range_table)
        
        # Save Range Margins button
        save_range_margins_btn = QPushButton("Save Range Margins")
        save_range_margins_btn.clicked.connect(self.save_range_margins)
        filter_group_layout.addWidget(save_range_margins_btn)
        
        # Auto Update ALL checkbox
        self.auto_update_all_checkbox = QCheckBox("Auto Update ALL")
        self.auto_update_all_checkbox.setStyleSheet("QCheckBox { color: orange; }")
        self.auto_update_all_checkbox.stateChanged.connect(self.toggle_auto_update)
        filter_group_layout.addWidget(self.auto_update_all_checkbox)
        
        # Initialize auto-update timer
        self.auto_update_timer = QTimer()
        self.auto_update_timer.timeout.connect(self.auto_update_range_margins)
        
        # Load saved range margins from Redis
        try:
            saved_margins_data = self.redis.get('maker_range_margins')
            if saved_margins_data:
                saved_margins = json.loads(saved_margins_data.decode('utf-8'))
                # Update range_margins and input fields
                for range_name, margins in saved_margins.items():
                    if range_name in self.range_margins and range_name in self.range_inputs:
                        self.range_margins[range_name] = margins
                        self.range_inputs[range_name]["open"].setValue(margins.get("open", 40))
                        self.range_inputs[range_name]["close"].setValue(margins.get("close", 40))
                logging.info("Loaded saved range margins from Redis")
        except Exception as e:
            logging.warning(f"Could not load saved range margins: {e}")
        
        filter_layout.addWidget(filter_group)
        filter_layout.addStretch()  # Push everything to top
        
        # Set fixed width for filter column (wider to avoid horizontal scrollbar)
        filter_widget.setFixedWidth(350)
        bottom_splitter.addWidget(filter_widget)
        
        # --- COLUMN 2: TABLE ---
        table_widget = QWidget()
        table_layout = QVBoxLayout(table_widget)
        table_layout.setContentsMargins(5, 5, 5, 5)
        
        # Create symbol table
        self.settings_table = QTableWidget()
        self.settings_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.settings_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.settings_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.settings_table.setAlternatingRowColors(True)
        
        # Set up columns
        self.settings_columns = ["Symbol", "Open Trigger Margin (bps)", "Open Margin Window (bps)", "Open Aggression (bps)",
                   "Close Trigger Margin (bps)", "Close Margin Window (bps)", "Close Aggression (bps)", 
                   "Min Buy Order (TRY)", "Buy Order Amount (TRY)", "Max Buy Order (TRY)", 
                   "Min Sell Order (TRY)", "Max Sell Order (TRY)", 
                   "Min Sell Ratio", "Max Sell Ratio", "Max Position (TRY)", "Current Position (TRY)", 
                   "Target Account Free TL (TRY)", "Move Threshold (bps)", "Exceptional Margin"]
        self.settings_columns_essential = ["Symbol", "Open Trigger Margin (bps)", "Open Margin Window (bps)", "Close Trigger Margin (bps)", "Close Margin Window (bps)", "Current Position (TRY)"]
        self.symbol_column_index = 0
        
        # Define column indices for easy maintenance
        self.min_buy_order_column_index = 7
        self.buy_order_amount_column_index = 8
        self.max_buy_order_column_index = 9
        self.min_sell_order_column_index = 10
        self.max_sell_order_column_index = 11
        self.min_sell_ratio_column_index = 12
        self.max_sell_ratio_column_index = 13
        self.max_position_column_index = 14
        self.current_position_TRY_index = 15
        self.target_account_free_tl_index = 16
        self.move_threshold_column_index = 17
        self.exceptional_margin_column_index = 18
        
        # Use the setup_table method for consistent functionality
        self.setup_table(self.settings_table, self.settings_columns)
        
        # After the table is populated with data, switch to Interactive mode
        self.settings_table.model().rowsInserted.connect(self.make_columns_interactive)
        
        # Restore column order for settings table
        self.restore_column_order(self.settings_table)

        table_layout.addWidget(self.settings_table)
        bottom_splitter.addWidget(table_widget)
        
        # Set splitter proportions (filter column wider, table larger)
        bottom_splitter.setSizes([350, 800])
        
        # Add bottom splitter to main layout
        settings_layout.addWidget(bottom_splitter)
        
        # Load symbols
        self.load_maker_arbitrage_thresholds()
        logging.info("Loading EXCHANGE balances now!")

    def update_open_trigger_margin(self):
        """Update open trigger margin for selected symbols"""
        value = self.open_margin_input.value() / 10000
        self.update_selected_symbols('OpenTriggerMargin', value, refresh_table=False)
        self.update_stop_margins()
        # Don't change selection when updating parameters - preserve user's selection

    def update_stop_margins(self, symbols_list=None):
        """Update open and close stop margins for selected symbols or provided symbol list"""
        try:
            # Get arbitrage thresholds from Redis
            settings_data = self.redis.get('maker_arbitrage_thresholds')
            if not settings_data:
                return
                
            thresholds = json.loads(settings_data.decode('utf-8'))
            
            # Get symbols to update - use provided list or get selected symbols
            if symbols_list is not None:
                symbols_to_update = symbols_list
            else:
                symbols_to_update = self.get_selected_symbols()
            
            if not symbols_to_update:
                return
            
            # Update stop margins for symbols
            updated_count = 0
            
            if isinstance(thresholds, list):
                for threshold in thresholds:
                    symbol = threshold.get('Symbol', threshold.get('symbol', ''))
                    if symbol in symbols_to_update:
                        # Calculate stop margins
                        threshold['OpenStopMargin'] = threshold['OpenTriggerMargin'] - threshold['OpenMarginWindow']
                        threshold['CloseStopMargin'] = threshold['CloseTriggerMargin'] - threshold['CloseMarginWindow']
                        updated_count += 1
                        logging.info(f"Updated stop margins for {symbol}")
            elif isinstance(thresholds, dict):
                for symbol in symbols_to_update:
                    if symbol in thresholds:
                        # Calculate stop margins
                        thresholds[symbol]['OpenStopMargin'] = thresholds[symbol]['OpenTriggerMargin'] - thresholds[symbol]['OpenMarginWindow']
                        thresholds[symbol]['CloseStopMargin'] = thresholds[symbol]['CloseTriggerMargin'] - thresholds[symbol]['CloseMarginWindow']
                        updated_count += 1
                        logging.info(f"Updated stop margins for {symbol}")
            
            # Save updated thresholds back to Redis
            self.redis.set('maker_arbitrage_thresholds', json.dumps(thresholds))
            
            # Send command to update thresholds
            self.redis.publish('arbit_commands', b'update_thresholds')
            
            if updated_count > 0:
                logging.info(f"Updated stop margins for {updated_count} symbols")
                
        except Exception as e:
            logging.error(f"Error updating stop margins: {e}")

    def update_open_margin_window(self):
        """Update open margin window for selected symbols"""
        value = self.open_margin_window_input.value() / 10000
        self.update_selected_symbols('OpenMarginWindow', value, refresh_table=False)
        self.update_stop_margins()
        # Don't change selection when updating parameters - preserve user's selection

    def update_buy_sell_aggressions(self):
        """Update both buy (open) and sell (close) aggression for selected symbols"""
        open_value = 1 + self.open_aggression_input.value() / 10000
        close_value = 1 - self.close_aggression_input.value() / 10000
        self.update_selected_symbols('OpenAggression', open_value, refresh_table=False)
        self.update_selected_symbols('CloseAggression', close_value, refresh_table=False)
        # Don't change selection when updating parameters - preserve user's selection

    def update_close_trigger_margin(self):
        """Update close trigger margin for selected symbols"""
        value = self.close_margin_input.value() / 10000
        self.update_selected_symbols('CloseTriggerMargin', value, refresh_table=False)
        self.update_stop_margins()
        # Don't change selection when updating parameters - preserve user's selection

    def update_close_margin_window(self):
        """Update close margin window for selected symbols"""
        value = self.close_margin_window_input.value() / 10000
        self.update_selected_symbols('CloseMarginWindow', value, refresh_table=False)
        self.update_stop_margins()
        # Don't change selection when updating parameters - preserve user's selection

    def update_open_trigger_margin_and_window(self):
        """Update both open trigger margin and margin window for selected symbols"""
        trigger_value = self.open_margin_input.value() / 10000
        window_value = self.open_margin_window_input.value() / 10000
        
        # Update trigger margin (respect exceptional margin)
        self.update_selected_symbols('OpenTriggerMargin', trigger_value, respect_exceptional_margin=True, refresh_table=False)
        # Update margin window (respect exceptional margin)
        self.update_selected_symbols('OpenMarginWindow', window_value, respect_exceptional_margin=True, refresh_table=False)
        # Update stop margins
        self.update_stop_margins()

        # Don't change selection when updating parameters - preserve user's selection
        
        # Show success message
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.statusBar().showMessage(f"{current_time} - Updated Open Trigger Margin & Window for selected symbols")
        
    def update_close_trigger_margin_and_window(self):
        """Update both close trigger margin and margin window for selected symbols"""
        trigger_value = self.close_margin_input.value() / 10000
        window_value = self.close_margin_window_input.value() / 10000
        
        # Update trigger margin (respect exceptional margin)
        self.update_selected_symbols('CloseTriggerMargin', trigger_value, respect_exceptional_margin=True, refresh_table=False)
        # Update margin window (respect exceptional margin)
        self.update_selected_symbols('CloseMarginWindow', window_value, respect_exceptional_margin=True, refresh_table=False)
        # Update stop margins
        self.update_stop_margins()
        # Don't change selection when updating parameters - preserve user's selection
        
        # Show success message
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.statusBar().showMessage(f"{current_time} - Updated Close Trigger Margin & Window for selected symbols")
        
    def update_move_and_exceptional_threshold(self):
        """Update move threshold and set/reset exceptional margin flag for selected symbols"""
        try:
            # Get move threshold value
            move_threshold_value = self.move_threshold_input.value()
            
            # Get exceptional margin value from checkbox
            exceptional_value = self.exceptional_margin_checkbox.isChecked()
            
            # Update move threshold using existing helper function
            self.update_selected_symbols('MoveThreshold', move_threshold_value, refresh_table=False)
            
            # Update exceptional margin
            # Get arbitrage thresholds from Redis
            settings_data = self.redis.get('maker_arbitrage_thresholds')
            if not settings_data:
                QMessageBox.warning(self, "Warning", "No thresholds found")
                return False
                
            thresholds = json.loads(settings_data.decode('utf-8'))
            
            # Get selected symbols from the symbol table
            selected_symbols = self.get_selected_symbols()
            
            if not selected_symbols:
                QMessageBox.warning(self, "Warning", "No symbols selected")
                return False
            
            # Update only selected symbols
            updated_count = 0
            
            # Handle both list and dictionary formats
            if isinstance(thresholds, list):
                for threshold in thresholds:
                    symbol = threshold.get('Symbol', threshold.get('symbol', ''))
                    if symbol in selected_symbols:
                        threshold['ExceptionalMargin'] = exceptional_value
                        updated_count += 1
                        logging.info(f"Set ExceptionalMargin for {symbol}: {exceptional_value}")
            elif isinstance(thresholds, dict):
                for symbol in selected_symbols:
                    if symbol in thresholds:
                        thresholds[symbol]['ExceptionalMargin'] = exceptional_value
                        updated_count += 1
                        logging.info(f"Set ExceptionalMargin for {symbol}: {exceptional_value}")
            
            # Save updated thresholds back to Redis
            self.redis.set('maker_arbitrage_thresholds', json.dumps(thresholds))
            
            # Send command to update thresholds
            self.redis.publish('arbit_commands', b'update_thresholds')
            
            # Show success message in status bar
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            action = "Set" if exceptional_value else "Reset"
            self.statusBar().showMessage(f"{current_time} - Updated Move Threshold ({move_threshold_value}) and {action} Exceptional Margin for {updated_count} symbols: {', '.join(selected_symbols)}")
            
            # Update table cells directly (optimized - no full refresh)
            was_sorting_enabled = self.settings_table.isSortingEnabled()
            self.settings_table.setSortingEnabled(False)
            for symbol in selected_symbols:
                self.update_table_cell_for_symbol(symbol, 'ExceptionalMargin', exceptional_value)
            if was_sorting_enabled:
                self.settings_table.setSortingEnabled(True)
            
            return True
                
        except Exception as e:
            logging.error(f"Update move and exceptional threshold error: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to update move and exceptional threshold: {str(e)}")
            return False
            for symbol in selected_symbols:
                self.update_table_cell_for_symbol(symbol, 'ExceptionalMargin', exceptional_value)
            if was_sorting_enabled:
                self.settings_table.setSortingEnabled(True)
            
            return True
                
        except Exception as e:
            logging.error(f"Update move and exceptional threshold error: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to update move and exceptional threshold: {str(e)}")
            return False
        
    def update_buy_order_amount_maximum(self):
        """Update the maximum value of buy_order_amount_input based on max_buy_order_input"""
        max_value = self.max_buy_order_input.value()
        current_value = self.buy_order_amount_input.value()
        self.buy_order_amount_input.setMaximum(max_value)
        # If current value exceeds new maximum, adjust it
        if current_value > max_value:
            self.buy_order_amount_input.setValue(max_value)

    def update_buy_order_amount(self):
        """Update buy order amount for selected symbols"""
        value = self.buy_order_amount_input.value()
        self.update_selected_symbols('BuyOrderAmount_TRY', value, refresh_table=False)
        # Don't change selection when updating parameters - preserve user's selection

    def update_buy_order_amounts(self):
        """Update both min and max buy order amounts for selected symbols"""
        min_value = self.min_buy_order_input.value()
        max_value = self.max_buy_order_input.value()
        self.update_selected_symbols('MinBuyOrderAmount_TRY', min_value, refresh_table=False)
        self.update_selected_symbols('MaxBuyOrderAmount_TRY', max_value, refresh_table=False)
        # Don't change selection when updating parameters - preserve user's selection

    def update_sell_order_amounts(self):
        """Update both min and max sell order amounts for selected symbols"""
        min_value = self.min_sell_order_input.value()
        max_value = self.max_sell_order_input.value()
        self.update_selected_symbols('MinSellOrderAmount_TRY', min_value, refresh_table=False)
        self.update_selected_symbols('MaxSellOrderAmount_TRY', max_value, refresh_table=False)
        # Don't change selection when updating parameters - preserve user's selection

    def update_max_position(self):
        """Update maximum position amount for selected symbols"""
        value = self.max_position_input.value()
        self.update_selected_symbols('MaxPositionAmount_TRY', value, refresh_table=False)
        # Don't change selection when updating parameters - preserve user's selection

    def validate_min_ratio_changed(self):
        """When min ratio changes, ensure max ratio >= min ratio"""
        min_ratio = self.sell_order_min_ratio_input.value()
        max_ratio = self.sell_order_max_ratio_input.value()
        
        if max_ratio < min_ratio:
            # Adjust max ratio to be at least equal to min ratio
            self.sell_order_max_ratio_input.blockSignals(True)
            self.sell_order_max_ratio_input.setValue(min_ratio)
            self.sell_order_max_ratio_input.blockSignals(False)

    def validate_max_ratio_changed(self):
        """When max ratio changes, ensure max ratio >= min ratio"""
        min_ratio = self.sell_order_min_ratio_input.value()
        max_ratio = self.sell_order_max_ratio_input.value()
        
        if max_ratio < min_ratio:
            # Adjust max ratio to be at least equal to min ratio
            self.sell_order_max_ratio_input.blockSignals(True)
            self.sell_order_max_ratio_input.setValue(min_ratio)
            self.sell_order_max_ratio_input.blockSignals(False)

    def update_sell_ratios(self):
        """Update both min and max sell ratios for selected symbols"""
        # Validate before updating
        self.validate_min_ratio_changed()
        min_value = self.sell_order_min_ratio_input.value()
        max_value = self.sell_order_max_ratio_input.value()
        self.update_selected_symbols('MinSellRatio', min_value, refresh_table=False)
        self.update_selected_symbols('MaxSellRatio', max_value, refresh_table=False)
        # Don't change selection when updating parameters - preserve user's selection


    def update_position_gap_threshold(self):
        """Update position gap threshold for selected symbols"""
        value = self.position_gap_threshold_input.value()
        try:
            self.redis.set('maker_PositionGapThreshold_TRY', value)
            self.statusBar().showMessage(f"Updated position gap threshold: {value}TRY")

        except Exception as e:
            logging.error(f"Error updating position gap threshold: {e}")
            QMessageBox.critical(self, "Error", f"Error updating position gap threshold: {e}")


    def filter_symbols(self):
        """Filter symbols in the table based on search text"""
        search_text = self.symbol_search_input.text().upper()
        
        for row in range(self.settings_table.rowCount()):
            symbol_item = self.settings_table.item(row, 0)
            if symbol_item:
                symbol = symbol_item.text()
                # If search text is empty, show all rows
                if not search_text:
                    self.settings_table.setRowHidden(row, False)
                else:
                    # If search text exists, show only matching ones
                    self.settings_table.setRowHidden(row, search_text not in symbol)

    def make_columns_interactive(self):
        """Switch column resize mode to Interactive after data is loaded"""
        # Only do this once
        if hasattr(self, 'columns_made_interactive') and self.columns_made_interactive:
            return
            
        # Set all columns to Interactive mode
        header = self.settings_table.horizontalHeader()
        for i in range(self.settings_table.columnCount()):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.Interactive)
            
        # Mark as done
        self.columns_made_interactive = True
        
        # Disconnect the signal to avoid repeated calls
        self.settings_table.model().rowsInserted.disconnect(self.make_columns_interactive)

    def filter_symbols_by_position_ranges(self, preserve_selection=False):
        """Filter symbols in the table based on position size
        
        Args:
            preserve_selection: If True, preserve existing selection. If False (default), 
                              select rows matching position ranges (for "select by position" feature)
        """
        # Preserve current selection before filtering (supports multiple rows)
        selected_rows_before = set()
        selection_model = self.settings_table.selectionModel()
        for row in range(self.settings_table.rowCount()):
            if selection_model.isRowSelected(row, self.settings_table.rootIndex()):
                selected_rows_before.add(row)
        
        # Store current selection mode
        current_selection_mode = self.settings_table.selectionMode()
        
        # Temporarily set to MultiSelection to allow multiple row restoration
        self.settings_table.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)

        """Filter symbols in the table based on multiple selected position ranges"""
        # Get selected ranges from range table
        selected_ranges = []
        if hasattr(self, 'range_table'):
            selected_rows = self.range_table.selectionModel().selectedRows()
            for index in selected_rows:
                row = index.row()
                range_item = self.range_table.item(row, 0)
                if range_item:
                    selected_ranges.append(range_item.text())
        # Fallback to listbox if range_table doesn't exist yet
        elif hasattr(self, 'range_listbox'):
            selected_ranges = [item.text() for item in self.range_listbox.selectedItems()]
        
        if not selected_ranges:
            # If no ranges selected, show all symbols and deselect all rows
            for row in range(self.settings_table.rowCount()):
                self.settings_table.setRowHidden(row, False)
                
            # Clear all selections when no ranges are selected
            self.settings_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
            selection_model = self.settings_table.selectionModel()
            selection_model.clearSelection()
            return
        
        # Process each row and collect matching rows for selection
        matching_rows = []
        for row in range(self.settings_table.rowCount()):
           
            position_text = self.settings_table.item(row, self.current_position_TRY_index).text()
            
            try:
                # Remove ₺ prefix if present
                if position_text.startswith('₺'):
                    position_text = position_text[1:]
                
                # Remove comma separators
                position_text = position_text.replace(',', '')
                
                current_position = int(position_text)
                
                # Check if current position falls within any of the selected ranges
                should_show = False
                for range_text in selected_ranges:
                    if range_text in self.range_boundaries:
                        min_pos, max_pos = self.range_boundaries[range_text]
                        if min_pos <= current_position <= max_pos:
                            should_show = True
                            break
                
                # Show or hide the row based on whether it matches any selected range
                self.settings_table.setRowHidden(row, not should_show)
                
                # Collect matching rows for selection
                if should_show:
                    matching_rows.append(row)
                
            except (ValueError, AttributeError) as e:
                logging.warning(f"Error parsing position value '{position_text}' at row {row}: {e}")
                # Hide rows with invalid position data
                self.settings_table.setRowHidden(row, True)
        
        # Handle selection based on context
        self.settings_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        selection_model = self.settings_table.selectionModel()
        
        if preserve_selection:
            # Preserve existing selection (when called from parameter updates)
            # But deselect rows that no longer match any selected range
            for row in selected_rows_before:
                if row < self.settings_table.rowCount():
                    if row in matching_rows and not self.settings_table.isRowHidden(row):
                        # Keep selection if row still matches
                        index = self.settings_table.model().index(row, 0)
                        selection_model.select(index, QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)
                    else:
                        # Deselect if row no longer matches any range
                        index = self.settings_table.model().index(row, 0)
                        selection_model.select(index, QItemSelectionModel.SelectionFlag.Deselect | QItemSelectionModel.SelectionFlag.Rows)
        else:
            # Select rows matching position ranges (when user explicitly selects position ranges)
            # First clear all selections, then select only matching rows
            selection_model.clearSelection()
            for row in matching_rows:
                if row < self.settings_table.rowCount() and not self.settings_table.isRowHidden(row):
                    index = self.settings_table.model().index(row, 0)
                    selection_model.select(index, QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)

    def toggle_all_range_selections(self, state):
        """Toggle all range rows when Select All is checked/unchecked"""
        if hasattr(self, 'range_table'):
            if state == Qt.CheckState.Checked.value or state == 2:  # 2 is Qt.Checked
                self.range_table.selectAll()
            else:
                self.range_table.clearSelection()

    def save_range_margins(self):
        """Save the current range margin values from input fields"""
        try:
            for range_name in self.range_list:
                if range_name in self.range_inputs:
                    open_value = self.range_inputs[range_name]["open"].value()
                    close_value = self.range_inputs[range_name]["close"].value()
                    self.range_margins[range_name] = {"open": open_value, "close": close_value}
            
            # Save to Redis for persistence
            self.redis.set('maker_range_margins', json.dumps(self.range_margins))
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.statusBar().showMessage(f"{current_time} - Range margins saved successfully")
            logging.info(f"Range margins saved: {self.range_margins}")
            
            # If auto-update is enabled, immediately trigger an update instead of waiting for next period
            if hasattr(self, 'auto_update_timer') and self.auto_update_timer.isActive():
                logging.info("Auto-update enabled: Immediately updating range margins after save")
                self.auto_update_range_margins()
            
        except Exception as e:
            logging.error(f"Error saving range margins: {e}")
            QMessageBox.critical(self, "Error", f"Failed to save range margins: {str(e)}")

    def range_table_mouse_press_event(self, event):
        """Custom mouse press event - store position and selection state for drag detection"""
        self.range_table_mouse_press_pos = event.pos()
        self.range_table_is_dragging = False
        
        # Store selection state before default behavior
        item = self.range_table.itemAt(event.pos())
        self._range_table_press_item = item
        if item:
            selection_model = self.range_table.selectionModel()
            self._range_table_press_selection_state = {}
            for row in range(self.range_table.rowCount()):
                self._range_table_press_selection_state[row] = selection_model.isRowSelected(
                    row, self.range_table.rootIndex()
                )
        else:
            self._range_table_press_selection_state = {}
        
        # Use default behavior to allow drag selection
        QTableWidget.mousePressEvent(self.range_table, event)

    def range_table_mouse_move_event(self, event):
        """Custom mouse move event - detect drag operation"""
        if self.range_table_mouse_press_pos is not None:
            # Check if mouse has moved significantly (drag threshold: 3 pixels)
            drag_distance = (event.pos() - self.range_table_mouse_press_pos).manhattanLength()
            if drag_distance > 3:
                self.range_table_is_dragging = True
        # Use default behavior for drag selection
        QTableWidget.mouseMoveEvent(self.range_table, event)

    def range_table_mouse_release_event(self, event):
        """Custom mouse release event - handle toggle on single click"""
        # Use default behavior first
        QTableWidget.mouseReleaseEvent(self.range_table, event)
        
        # Only toggle if it was a single click (not a drag) on the same item
        if not self.range_table_is_dragging and hasattr(self, '_range_table_press_item'):
            release_item = self.range_table.itemAt(event.pos())
            
            # Check if press and release were on the same row in column 0 (range name)
            if (release_item and self._range_table_press_item and 
                release_item.row() == self._range_table_press_item.row() and
                release_item.column() == 0 and self._range_table_press_item.column() == 0):
                
                row = release_item.row()
                selection_model = self.range_table.selectionModel()
                
                # Check if this row was selected before the mouse press
                was_selected_before = self._range_table_press_selection_state.get(row, False)
                is_selected_now = selection_model.isRowSelected(row, self.range_table.rootIndex())
                
                # If row was already selected before click and is now selected (single click on selected row), toggle it off
                # Don't toggle if Ctrl was held (multi-select mode)
                if was_selected_before and is_selected_now and not (event.modifiers() & Qt.KeyboardModifier.ControlModifier):
                    selection_model.select(
                        self.range_table.model().index(row, 0),
                        QItemSelectionModel.SelectionFlag.Deselect | QItemSelectionModel.SelectionFlag.Rows
                    )
        
        # Reset drag state
        self.range_table_mouse_press_pos = None
        self.range_table_is_dragging = False
        if hasattr(self, '_range_table_press_selection_state'):
            delattr(self, '_range_table_press_selection_state')
        if hasattr(self, '_range_table_press_item'):
            delattr(self, '_range_table_press_item')

    def update_auto_update_interval(self):
        """Update the auto-update timer interval if it's currently running"""
        interval_seconds = self.auto_update_interval_input.value()
        # Update label
        self.auto_range_margin_period_label.setText(f"Auto Range Margin Update Period (s): ({interval_seconds})")
        if self.auto_update_all_checkbox.isChecked():
            interval_ms = interval_seconds * 1000
            self.auto_update_timer.stop()
            self.auto_update_timer.start(interval_ms)
            logging.info(f"Auto-update interval changed to {interval_seconds} seconds")

    def toggle_auto_update(self, state):
        """Enable/disable auto-update timer for range margins"""
        if state == Qt.CheckState.Checked.value or state == 2:  # 2 is Qt.Checked
            # Get interval from Global Settings input (convert seconds to milliseconds)
            interval_seconds = self.auto_update_interval_input.value()
            interval_ms = interval_seconds * 1000
            # Start timer with adjustable interval
            self.auto_update_timer.start(interval_ms)
            logging.info(f"Auto-update range margins enabled ({interval_seconds} second interval)")
            self.statusBar().showMessage(f"Auto-update range margins enabled ({interval_seconds}s interval)")
        else:
            # Stop timer
            self.auto_update_timer.stop()
            logging.info("Auto-update range margins disabled")
            self.statusBar().showMessage("Auto-update range margins disabled")
    
    def update_auto_update_buy_period(self):
        """Update the auto-update buy order amounts period"""
        try:
            period = self.auto_update_buy_period_input.value()
            # Save to Redis
            self.redis.set('maker_AutoUpdateBuyOrderPeriod', str(period))
            
            # Update label
            self.auto_update_buy_period_label.setText(f"Auto update buy order amounts period (s): ({period})")
            
            # Update timer if it's running
            if hasattr(self, 'auto_update_buy_timer') and self.auto_update_buy_timer.isActive():
                self.auto_update_buy_timer.stop()
                self.auto_update_buy_timer.start(period * 1000)
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.statusBar().showMessage(f"{current_time} - Auto-update buy order period updated to {period}s")
            logging.info(f"Auto-update buy order period updated to {period} seconds")
        except Exception as e:
            logging.error(f"Error updating auto-update buy order period: {e}")
            QMessageBox.critical(self, "Error", f"Failed to update auto-update buy order period: {str(e)}")
    
    def update_target_account_free_tl(self):
        """Update target account free TL"""
        try:
            target_tl = self.target_account_free_tl_input.value()
            # Save to Redis
            self.redis.set('maker_TargetAccountFreeTL', str(target_tl))
            
            # Update label
            formatted_target_tl = f"{int(target_tl):,}".replace(',', '.')
            self.target_account_free_tl_label.setText(f"Target Account Free TL: ({formatted_target_tl})")
            
            # Update all rows in settings table
            formatted_value = f"{int(target_tl):,}".replace(',', '.')
            for row in range(self.settings_table.rowCount()):
                self.settings_table.setItem(row, self.target_account_free_tl_index, QTableWidgetItem(formatted_value))
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.statusBar().showMessage(f"{current_time} - Target Account Free TL updated to {target_tl:,.0f}")
            logging.info(f"Target Account Free TL updated to {target_tl}")
        except Exception as e:
            logging.error(f"Error updating target account free TL: {e}")
            QMessageBox.critical(self, "Error", f"Failed to update target account free TL: {str(e)}")
    
    def update_auto_range_margin_period(self):
        """Update the auto range margin update period"""
        try:
            period = self.auto_update_interval_input.value()
            # Save to Redis
            self.redis.set('maker_AutoRangeMarginUpdatePeriod', str(period))
            
            # Update label
            self.auto_range_margin_period_label.setText(f"Auto Range Margin Update Period (s): ({period})")
            
            # Update timer if it's running
            if self.auto_update_all_checkbox.isChecked():
                self.auto_update_timer.stop()
                self.auto_update_timer.start(period * 1000)
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.statusBar().showMessage(f"{current_time} - Auto Range Margin Update Period updated to {period}s")
            logging.info(f"Auto Range Margin Update Period updated to {period} seconds")
        except Exception as e:
            logging.error(f"Error updating auto range margin period: {e}")
            QMessageBox.critical(self, "Error", f"Failed to update auto range margin period: {str(e)}")
    
    def toggle_auto_update_buy_order_amounts(self, state):
        """Enable/disable auto-update timer for buy order amounts"""
        try:
            # Update label
            auto_update_enabled = "Enabled" if (state == Qt.CheckState.Checked.value or state == 2) else "Disabled"
            self.auto_update_buy_label.setText(f"Auto update buy order amounts: ({auto_update_enabled})")
            
            if state == Qt.CheckState.Checked.value or state == 2:  # 2 is Qt.Checked
                # Get period from input (convert seconds to milliseconds)
                period_seconds = self.auto_update_buy_period_input.value()
                period_ms = period_seconds * 1000
                # Start timer
                if not hasattr(self, 'auto_update_buy_timer'):
                    self.auto_update_buy_timer = QTimer()
                    self.auto_update_buy_timer.timeout.connect(self.auto_update_buy_order_amounts)
                self.auto_update_buy_timer.start(period_ms)
                # Update indicator to orange
                self.auto_update_buy_indicator.setStyleSheet("background-color: orange; border: 1px solid #777777;")
                # Save state to Redis
                self.redis.set('maker_AutoUpdateBuyOrderEnabled', '1')
                logging.info(f"Auto-update buy order amounts enabled ({period_seconds} second interval)")
                self.statusBar().showMessage(f"Auto-update buy order amounts enabled ({period_seconds}s interval)")
            else:
                # Stop timer
                if hasattr(self, 'auto_update_buy_timer'):
                    self.auto_update_buy_timer.stop()
                # Update indicator to grey
                self.auto_update_buy_indicator.setStyleSheet("background-color: #555555; border: 1px solid #777777;")
                # Save state to Redis
                self.redis.set('maker_AutoUpdateBuyOrderEnabled', '0')
                logging.info("Auto-update buy order amounts disabled")
                self.statusBar().showMessage("Auto-update buy order amounts disabled")
        except Exception as e:
            logging.error(f"Error toggling auto-update buy order amounts: {e}")
    
    def auto_update_buy_order_amounts(self):
        """Automatically update buy order amounts based on 10-second moving average of free TL balance"""
        try:
            # Get thresholds from Redis
            thresholds_data = self.redis.get('maker_arbitrage_thresholds')
            if not thresholds_data:
                logging.warning("Cannot auto-update buy order amounts: thresholds not available")
                return
            
            thresholds = json.loads(thresholds_data.decode('utf-8'))
            if not isinstance(thresholds, list):
                logging.warning(f"update_buy_order_by_balance: thresholds is not a list (type: {type(thresholds)}), skipping update")
                return
            
            # Get limits from Redis (MinBuyOrderAmount_TRY and MaxBuyOrderAmount_TRY from first symbol or defaults)
            buy_order_lower_limit = 10000  # Default lower limit
            buy_order_upper_limit = 200000  # Default upper limit
            
            # Try to get limits from first symbol's thresholds
            if thresholds and isinstance(thresholds[0], dict):
                first_symbol = thresholds[0]
                buy_order_lower_limit = first_symbol.get('MinBuyOrderAmount_TRY', 10000)
                buy_order_upper_limit = first_symbol.get('MaxBuyOrderAmount_TRY', 200000)
            
            # Validate limits
            if buy_order_upper_limit <= buy_order_lower_limit:
                logging.error(f"Invalid limits - upper ({buy_order_upper_limit}) must be > lower ({buy_order_lower_limit}), using defaults")
                buy_order_lower_limit = 10000
                buy_order_upper_limit = 200000
            
            # Get target free TRY from Redis
            try:
                target_tl_data = self.redis.get('maker_TargetAccountFreeTL')
                target_free_try = float(target_tl_data.decode('utf-8')) if target_tl_data else 750000.0
            except Exception as e:
                logging.error(f"Error loading target account free TL: {e}")
                target_free_try = 750000.0
            
            # Initialize buy order amounts to a value within limits if not already done
            if not self.buy_order_initialized:
                for threshold in thresholds:
                    if isinstance(threshold, dict):
                        symbol = threshold.get('Symbol', '')
                        if not symbol:
                            continue
                        # Initialize to a value within limits
                        initial_value = max(buy_order_lower_limit, min(buy_order_upper_limit, 40000))
                        threshold['BuyOrderAmount_TRY'] = initial_value
                self.buy_order_initialized = True
                logging.info(f"Initialized all buy order amounts to {initial_value} TRY (within limits: {buy_order_lower_limit}-{buy_order_upper_limit})")
            
            # Need at least 10 readings for accurate 10-second moving average
            if not hasattr(self, 'EXCHANGE_balance_history') or len(self.EXCHANGE_balance_history) < 10:
                history_len = len(self.EXCHANGE_balance_history) if hasattr(self, 'EXCHANGE_balance_history') else 0
                logging.info(f"Not enough EXCHANGE balance readings yet: {history_len}/10 (need 10 for moving average)")
                return
            
            # Calculate 10-second moving average (using last 10 readings)
            try:
                avg_balance = sum(self.EXCHANGE_balance_history) / len(self.EXCHANGE_balance_history)
                if avg_balance < 0:
                    logging.warning(f"Negative average balance: {avg_balance}, skipping")
                    return
            except (ValueError, TypeError, ZeroDivisionError) as e:
                logging.error(f"Error calculating average balance: {e}")
                return
            
            # Determine if we should increment or decrement based on target
            # Logic: Less free TL (< target) → REDUCE buy orders (decrement)
            #        More free TL (> target) → INCREASE buy orders (increment)
            should_increment = avg_balance > target_free_try
            logging.info(f"EXCHANGE: avg_balance={avg_balance:,.0f} TL, target={target_free_try:,.0f} TL → {'INCREASE' if should_increment else 'REDUCE'} buy orders")
            
            updated_count = 0
            
            # Import multiplier calculation functions
            import arbit_config_maker_BTR as arbit_config
            
            # Update buy order amounts for all symbols
            for threshold in thresholds:
                if not isinstance(threshold, dict):
                    continue
                
                symbol = threshold.get('Symbol', '')
                if not symbol:
                    continue
                
                # Get current buy order amount with validation
                try:
                    current_buy_order = int(threshold.get('BuyOrderAmount_TRY', 40000))
                    # Ensure current value is within limits
                    current_buy_order = max(buy_order_lower_limit, min(buy_order_upper_limit, current_buy_order))
                except (ValueError, TypeError) as e:
                    logging.warning(f"Invalid BuyOrderAmount_TRY for {symbol}: {threshold.get('BuyOrderAmount_TRY')}, using default")
                    current_buy_order = max(buy_order_lower_limit, min(buy_order_upper_limit, 40000))
                
                # Calculate multiplier based on current buy_order_amount
                try:
                    if should_increment:
                        multiplier = arbit_config.calculate_increment_multiplier(current_buy_order)
                        action_str = "INCREASE"
                    else:
                        multiplier = arbit_config.calculate_decrement_multiplier(current_buy_order)
                        action_str = "REDUCE"
                    
                    # Validate multiplier
                    if multiplier <= 0 or not isinstance(multiplier, (int, float)):
                        logging.warning(f"Invalid multiplier for {symbol}: {multiplier}, skipping update")
                        continue
                except Exception as e:
                    logging.error(f"Error calculating multiplier for {symbol}: {e}")
                    continue
                
                # Calculate new buy order amount
                try:
                    new_buy_order = int(current_buy_order * multiplier)
                except (ValueError, TypeError, OverflowError) as e:
                    logging.error(f"Error calculating new_buy_order for {symbol}: {e}")
                    continue
                
                # Apply limits
                new_buy_order = max(buy_order_lower_limit, min(buy_order_upper_limit, new_buy_order))
                
                # Only update if there's a meaningful change (more than 1 TRY difference)
                if abs(new_buy_order - current_buy_order) > 1:
                    threshold['BuyOrderAmount_TRY'] = new_buy_order
                    updated_count += 1
                    logging.info(f"✅ {symbol}: {current_buy_order:,} → {new_buy_order:,} TRY ({action_str} by {multiplier:.3f}x)")
                else:
                    logging.debug(f"{symbol}: No change needed ({current_buy_order:,} TRY, multiplier={multiplier:.3f}x would give {new_buy_order:,})")
            
            if updated_count > 0:
                # Save updated thresholds back to Redis
                self.redis.set('maker_arbitrage_thresholds', json.dumps(thresholds))
                
                # Update table cells individually to preserve sorting
                was_sorting_enabled = self.settings_table.isSortingEnabled()
                self.settings_table.setSortingEnabled(False)
                
                for threshold in thresholds:
                    if isinstance(threshold, dict):
                        symbol = threshold.get('Symbol', '')
                        if symbol and 'BuyOrderAmount_TRY' in threshold:
                            new_buy_amount = threshold['BuyOrderAmount_TRY']
                            self.update_table_cell_for_symbol(symbol, 'BuyOrderAmount_TRY', new_buy_amount)
                
                if was_sorting_enabled:
                    self.settings_table.setSortingEnabled(True)
                
                # Send command to update thresholds
                self.redis.publish('arbit_commands', b'update_thresholds')
                
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                action = "increased" if should_increment else "decreased"
                target_status = f"above target ({target_free_try:,.0f})" if should_increment else f"below target ({target_free_try:,.0f})"
                message = f"{current_time} - Auto-updated buy order amounts for {updated_count} symbols"
                message += f" (Balance: {avg_balance:,.0f} TL {target_status}, {action})"
                self.statusBar().showMessage(message)
                logging.info(f"✅ update_buy_order_by_balance: Updated {updated_count} symbols (limits: {buy_order_lower_limit}-{buy_order_upper_limit} TRY)")
                logging.info(f"   Balance avg: {avg_balance:,.0f} TL ({target_status}) - {action} buy order amount for {updated_count} symbols")
            else:
                logging.info(f"⚠️ update_buy_order_by_balance: No updates needed (all changes < 1 TRY or no symbols to update)")
                
        except Exception as e:
            logging.error(f"Error in auto-update buy order amounts: {e}")
            import traceback
            logging.error(traceback.format_exc())

    def auto_update_range_margins(self):
        """Automatically update margins based on current positions in table - same mechanism as other update buttons"""
        try:
            if not hasattr(self, 'range_margins') or not self.range_margins:
                return
            
            # Get thresholds from Redis
            thresholds_data = self.redis.get('maker_arbitrage_thresholds')
            if not thresholds_data:
                return
            
            thresholds = json.loads(thresholds_data.decode('utf-8'))
            if not isinstance(thresholds, list):
                return
            
            updated_symbols = []
            excluded_symbols = []
            
            # Process each symbol in the table
            for row in range(self.settings_table.rowCount()):
                symbol_item = self.settings_table.item(row, 0)
                if not symbol_item:
                    continue
                
                symbol = symbol_item.text()
                
                # Get current position from table
                position_item = self.settings_table.item(row, self.current_position_TRY_index)
                if not position_item:
                    continue
                
                try:
                    position_text = position_item.text()
                    # Remove ₺ prefix if present
                    if position_text.startswith('₺'):
                        position_text = position_text[1:]
                    # Remove comma separators
                    position_text = position_text.replace(',', '')
                    
                    # Handle NaN values
                    if position_text.lower() in ['nan', 'none', '']:
                        continue
                    
                    current_position = int(position_text)
                    
                    # Find which range this position falls into
                    matching_range = None
                    for range_name, (min_pos, max_pos) in self.range_boundaries.items():
                        if min_pos <= current_position <= max_pos:
                            matching_range = range_name
                            break
                    
                    if not matching_range or matching_range not in self.range_margins:
                        continue
                    
                    # Get margin values for this range
                    open_margin_bps = self.range_margins[matching_range]["open"]
                    close_margin_bps = self.range_margins[matching_range]["close"]
                    
                    # Convert to decimal (bps to decimal: divide by 10000)
                    open_margin = open_margin_bps / 10000
                    close_margin = close_margin_bps / 10000
                    
                    # Find and update the threshold for this symbol
                    for threshold in thresholds:
                        if threshold.get('Symbol', '') == symbol:
                            # Skip symbols with exceptional margin set
                            if threshold.get('ExceptionalMargin', False):
                                excluded_symbols.append(symbol)
                                continue
                            
                            # Update margins (same as update_open_trigger_margin_and_window)
                            threshold['OpenTriggerMargin'] = open_margin
                            threshold['CloseTriggerMargin'] = close_margin
                            updated_symbols.append(symbol)
                            break
                
                except (ValueError, AttributeError) as e:
                    logging.warning(f"Error processing position for {symbol}: {e}")
                    continue
            
            if updated_symbols:
                # Save updated thresholds back to Redis
                self.redis.set('maker_arbitrage_thresholds', json.dumps(thresholds))
                
                # Update stop margins for all updated symbols (same as other update functions)
                self.update_stop_margins(symbols_list=updated_symbols)
                
                # Update table cells individually to preserve sorting (same as update_selected_symbols)
                was_sorting_enabled = self.settings_table.isSortingEnabled()
                self.settings_table.setSortingEnabled(False)
                
                # Create a mapping of symbol to margin values for efficient lookup
                symbol_margin_map = {}
                for row in range(self.settings_table.rowCount()):
                    symbol_item = self.settings_table.item(row, 0)
                    if not symbol_item:
                        continue
                    
                    symbol = symbol_item.text()
                    if symbol not in updated_symbols:
                        continue
                    
                    position_item = self.settings_table.item(row, self.current_position_TRY_index)
                    if not position_item:
                        continue
                    
                    try:
                        position_text = position_item.text().replace('₺', '').replace(',', '')
                        if position_text.lower() not in ['nan', 'none', '']:
                            current_position = int(position_text)
                            # Find matching range
                            for range_name, (min_pos, max_pos) in self.range_boundaries.items():
                                if min_pos <= current_position <= max_pos:
                                    if range_name in self.range_margins:
                                        open_margin_bps = self.range_margins[range_name]["open"]
                                        close_margin_bps = self.range_margins[range_name]["close"]
                                        open_margin = open_margin_bps / 10000
                                        close_margin = close_margin_bps / 10000
                                        symbol_margin_map[symbol] = (open_margin, close_margin)
                                    break
                    except (ValueError, AttributeError):
                        pass
                
                # Update table cells for all updated symbols
                for symbol in updated_symbols:
                    if symbol in symbol_margin_map:
                        open_margin, close_margin = symbol_margin_map[symbol]
                        self.update_table_cell_for_symbol(symbol, 'OpenTriggerMargin', open_margin)
                        self.update_table_cell_for_symbol(symbol, 'CloseTriggerMargin', close_margin)
                
                if was_sorting_enabled:
                    self.settings_table.setSortingEnabled(True)
                
                # Send command to update thresholds
                self.redis.publish('arbit_commands', b'update_thresholds')
                
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                message = f"{current_time} - Auto-updated margins for {len(updated_symbols)} symbols"
                if excluded_symbols:
                    message += f" (Excluded {len(excluded_symbols)} exceptional)"
                self.statusBar().showMessage(message)
                logging.info(f"{current_time} - Auto-updated margins for {len(updated_symbols)} symbols")
                
        except Exception as e:
            logging.error(f"Error in auto-update range margins: {e}")
            import traceback
            logging.error(traceback.format_exc())

    def load_maker_arbitrage_thresholds(self):
        """Load arbitrage thresholds from Redis"""
        try:
            # Save current position values BEFORE clearing the table
            # This prevents showing zero when the table is rebuilt
            saved_positions = {}
            for row in range(self.settings_table.rowCount()):
                symbol_item = self.settings_table.item(row, 0)
                position_item = self.settings_table.item(row, self.current_position_TRY_index)
                if symbol_item and position_item:
                    symbol = symbol_item.text()
                    try:
                        # Extract numeric value from position item
                        if hasattr(position_item, 'numeric_value'):
                            # StableNumericTableWidgetItem
                            saved_positions[symbol] = position_item.numeric_value
                        else:
                            # Regular QTableWidgetItem - extract from text
                            position_text = position_item.text().replace('₺', '').replace(',', '').strip()
                            if position_text and position_text.lower() not in ['nan', 'none', '']:
                                saved_positions[symbol] = float(position_text)
                    except (ValueError, AttributeError):
                        pass
            
            # Clear the table
            self.settings_table.setRowCount(0)
            
            # Get all thresholds
            thresholds_data = self.redis.get('maker_arbitrage_thresholds')
            thresholds = json.loads(thresholds_data.decode('utf-8'))
            thresholds_count = len(thresholds)
            logging.info(f"Thresholds is a list with {thresholds_count} items")
            
            # Update the Symbol column header with count
            symbol_header = QTableWidgetItem(f"Symbol ({thresholds_count})")
            self.settings_table.setHorizontalHeaderItem(0, symbol_header)
            
            # Process list format
            self.settings_table.setSortingEnabled(False)
            
            for i, item in enumerate(thresholds):
                if not isinstance(item, dict):
                    continue
                    
                row = self.settings_table.rowCount()
                self.settings_table.insertRow(row)
                
                # Symbol
                symbol = item.get('Symbol', item.get('symbol', f"Unknown_{i}"))
                self.settings_table.setItem(row, 0, QTableWidgetItem(str(symbol)))
                
                # Open Trigger Margin
                open_margin = item.get('OpenTriggerMargin', 0)
                if isinstance(open_margin, float) and open_margin < 1:
                    open_margin = int(round(open_margin * 10000, 1))  # Convert to basis points
                self.settings_table.setItem(row, 1, QTableWidgetItem(str(open_margin)))
                
                # Open Margin Window
                open_margin_window = item.get('OpenMarginWindow', 0)
                if isinstance(open_margin_window, float) and open_margin_window < 1:
                    open_margin_window = int(round(open_margin_window * 10000, 1))  # Convert to basis points
                self.settings_table.setItem(row, 2, QTableWidgetItem(str(open_margin_window)))
                
                # Open Aggression
                open_aggression = item.get('OpenAggression', 0)
                # OpenAggression is stored as decimal (e.g., 1.0002), display as is
                self.settings_table.setItem(row, 3, QTableWidgetItem(str(open_aggression)))
                
                # Close Trigger Margin
                close_margin = item.get('CloseTriggerMargin', 0)
                if isinstance(close_margin, float) and close_margin < 1:
                    close_margin = int(round(close_margin * 10000, 1))  # Convert to basis points
                self.settings_table.setItem(row, 4, QTableWidgetItem(str(close_margin)))
                
                # Close Margin Window
                close_margin_window = item.get('CloseMarginWindow', 0)
                if isinstance(close_margin_window, float) and close_margin_window < 1:
                    close_margin_window = int(round(close_margin_window * 10000, 1))  # Convert to basis points
                self.settings_table.setItem(row, 5, QTableWidgetItem(str(close_margin_window)))
                
                # Close Aggression
                close_aggression = item.get('CloseAggression', 0)
                # CloseAggression is stored as decimal (e.g., 0.9996), display as is (NOT basis points)
                self.settings_table.setItem(row, 6, QTableWidgetItem(str(close_aggression)))
                
                # Min Buy Order Amount
                min_buy_order = item.get('MinBuyOrderAmount_TRY', 1000)
                self.settings_table.setItem(row, self.min_buy_order_column_index, QTableWidgetItem(str(min_buy_order)))
                
                # Buy Order Amount (Normal)
                buy_order_amount = item.get('BuyOrderAmount_TRY', 1000)
                self.settings_table.setItem(row, self.buy_order_amount_column_index, QTableWidgetItem(str(buy_order_amount)))
                
                # Max Buy Order
                max_buy_order = item.get('MaxBuyOrderAmount_TRY', 100000)
                self.settings_table.setItem(row, self.max_buy_order_column_index, QTableWidgetItem(str(max_buy_order)))
                
                # Min Sell Order Amount
                min_sell_order = item.get('MinSellOrderAmount_TRY', 1000)
                self.settings_table.setItem(row, self.min_sell_order_column_index, QTableWidgetItem(str(min_sell_order)))
                
                # Max Sell Order
                max_sell_order = item.get('MaxSellOrderAmount_TRY', 100000)
                self.settings_table.setItem(row, self.max_sell_order_column_index, QTableWidgetItem(str(max_sell_order)))
                
                # Min Sell Ratio
                min_sell_ratio = item.get('MinSellRatio', 0.0)
                self.settings_table.setItem(row, self.min_sell_ratio_column_index, QTableWidgetItem(str(min_sell_ratio)))
                
                # Max Sell Ratio
                max_sell_ratio = item.get('MaxSellRatio', 1.0)
                self.settings_table.setItem(row, self.max_sell_ratio_column_index, QTableWidgetItem(str(max_sell_ratio)))
                
                # Max position
                max_position = item.get('MaxPositionAmount_TRY', 100000)
                self.settings_table.setItem(row, self.max_position_column_index, QTableWidgetItem(str(max_position)))

                #Current Position - use saved value if available, otherwise use value from Redis
                current_position = item.get('CurrentPositionAmount_TRY', None)
                
                # If we have a saved position for this symbol, use it (prevents showing zero)
                if symbol in saved_positions:
                    saved_position = saved_positions[symbol]
                    # Only use saved position if Redis value is 0/None or if saved position is significantly different
                    if current_position is None or current_position == 0:
                        # Use saved position if Redis has no value
                        current_position = saved_position
                    elif abs(saved_position - current_position) < 0.01:
                        # Values are essentially the same, use saved to avoid flicker
                        current_position = saved_position
                    # Otherwise, use the Redis value (it's a real update)
                elif current_position is None:
                    current_position = 0
                
                # Always set the position item (use saved value to prevent zero flash)
                balance_item = self.StableNumericTableWidgetItem(f"₺{current_position:,.0f}", current_position, symbol)
                balance_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                self.settings_table.setItem(row, self.current_position_TRY_index, balance_item)

                # Target Account Free TL
                try:
                    target_tl_data = self.redis.get('maker_TargetAccountFreeTL')
                    target_account_free_tl = float(target_tl_data.decode('utf-8')) if target_tl_data else 750000
                except Exception as e:
                    logging.error(f"Error loading target account free TL: {e}")
                    target_account_free_tl = 750000
                formatted_target_tl = f"{int(target_account_free_tl):,}".replace(',', '.')
                self.settings_table.setItem(row, self.target_account_free_tl_index, QTableWidgetItem(formatted_target_tl))

                # Move Threshold
                move_threshold = item.get('MoveThreshold', 15)
                # MoveThreshold is already in basis points, no conversion needed
                self.settings_table.setItem(row, self.move_threshold_column_index, QTableWidgetItem(str(move_threshold)))
                
                # Exceptional Margin
                exceptional_margin = item.get('ExceptionalMargin', False)
                # Create a sortable item for the boolean value (False sorts before True)
                exceptional_margin_item = BooleanTableWidgetItem(exceptional_margin)
                self.settings_table.setItem(row, self.exceptional_margin_column_index, exceptional_margin_item)
                # Create widget with checkbox for display
                exceptional_margin_widget = QWidget()
                exceptional_margin_checkbox = QCheckBox()
                exceptional_margin_checkbox.setChecked(exceptional_margin)
                exceptional_margin_checkbox.setEnabled(False)  # Make it read-only
                exceptional_margin_layout = QHBoxLayout(exceptional_margin_widget)
                exceptional_margin_layout.addWidget(exceptional_margin_checkbox)
                exceptional_margin_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                exceptional_margin_layout.setContentsMargins(0, 0, 0, 0)
                self.settings_table.setCellWidget(row, self.exceptional_margin_column_index, exceptional_margin_widget)
            
            self.settings_table.setSortingEnabled(True)
            logging.info(f"Successfully populated table with {self.settings_table.rowCount()} rows")
            
        except Exception as e:
            logging.error(f"Error loading arbitrage thresholds: {e}")
            import traceback
            logging.error(traceback.format_exc())
    
    def create_default_thresholds(self, symbols):
        
        default_thresholds = []
        
        for symbol in symbols:
            default_thresholds.append({
                'Symbol': symbol,
                'OpenTriggerMargin': 0.005,
                'CloseTriggerMargin': 0.005,
                'OpenStopMargin': 0.003,
                'CloseStopMargin': 0.003,
                'OpenAggression': 1.0002,
                'CloseAggression': 0.9998,
                'OpenMarginWindow': 0.002,
                'CloseMarginWindow': 0.002,
                'Maker_Type': 0,
                'MinBuyOrderAmount_TRY': 1000,
                'BuyOrderAmount_TRY': 1000,
                'MaxBuyOrderAmount_TRY': 100000,
                'MinSellOrderAmount_TRY': 1000,
                'MaxSellOrderAmount_TRY': 100000,
                'MinSellRatio': 0.0,
                'MaxSellRatio': 1.0,
                'MaxPositionAmount_TRY': 100000,
                'MoveThreshold': 15,
                'ExceptionalMargin': False
            })
        
        return default_thresholds

        
    def toggle_all_current_symbols(self, state):
        """Toggle all checkboxes in current symbols table"""
        checked = (state == 2)  # Qt.CheckState.Checked is 2
        for row in range(self.current_symbols_table.rowCount()):
            checkbox = self.current_symbols_table.cellWidget(row, 0).findChild(QCheckBox)
            if checkbox:
                checkbox.setChecked(checked)

    def toggle_all_available_symbols(self, state):
        """Toggle all checkboxes in available symbols table"""
        checked = (state == 2)  # Qt.CheckState.Checked is 2
        for row in range(self.available_symbols_table.rowCount()):
            checkbox = self.available_symbols_table.cellWidget(row, 0).findChild(QCheckBox)
            if checkbox:
                checkbox.setChecked(checked)

    def handle_current_checkbox_change(self):
        """Handle individual checkbox state changes in current symbols table"""
        # Check if all are checked
        all_checked = True
        for row in range(self.current_symbols_table.rowCount()):
            checkbox = self.current_symbols_table.cellWidget(row, 0).findChild(QCheckBox)
            if checkbox and not checkbox.isChecked():
                all_checked = False
                break
        
        # Update "Select All" checkbox without triggering its signal
        self.select_all_current.blockSignals(True)
        self.select_all_current.setChecked(all_checked)
        self.select_all_current.blockSignals(False)

    def handle_available_checkbox_change(self):
        """Handle individual checkbox state changes in available symbols table"""
        # Check if all are checked
        all_checked = True
        for row in range(self.available_symbols_table.rowCount()):
            checkbox = self.available_symbols_table.cellWidget(row, 0).findChild(QCheckBox)
            if checkbox and not checkbox.isChecked():
                all_checked = False
                break
        
        # Update "Select All" checkbox without triggering its signal
        self.select_all_available.blockSignals(True)
        self.select_all_available.setChecked(all_checked)
        self.select_all_available.blockSignals(False)

    def load_symbol_lists(self):
        """Load current and available symbols"""
        try:
            
            
            # Get current symbols from Redis
            symbols_data = self.redis.get('EXCHANGE_Symbol_List')
            self.current_symbols = json.loads(symbols_data.decode('utf-8')) if symbols_data else []
            
            # Get available symbols from Redis
            available_symbols_data = self.redis.get('maker_fetched_symbols')
            available_symbols = []
            if available_symbols_data:
                try:
                    available_symbols = json.loads(available_symbols_data.decode('utf-8'))
                    available_symbols = sorted(list(set(available_symbols) - set(self.current_symbols)))
                except json.JSONDecodeError:
                    available_symbols = []
            
            # Get the arbitrage table for positions
            pickled_data = self.redis.get('maker_arbitrage_table')
            position_amounts = {}
            if pickled_data:
                arb_table = pickle.loads(pickled_data)
                for _, row in arb_table.iterrows():
                    if 'BaseSymbol' in row and 'EXCHANGEPositionAmount_TRY' in row:
                        symbol = row['BaseSymbol']
                        position_amounts[symbol] = row['EXCHANGEPositionAmount_TRY']
            
            # Reset "Select All" checkboxes
            self.select_all_current.blockSignals(True)
            self.select_all_current.setChecked(False)
            self.select_all_current.blockSignals(False)
            
            self.select_all_available.blockSignals(True)
            self.select_all_available.setChecked(False)
            self.select_all_available.blockSignals(False)
            
            # Update current symbols table
            self.current_symbols_table.setRowCount(len(self.current_symbols))
            for i, symbol in enumerate(self.current_symbols):
                # Checkbox
                checkbox = QCheckBox()
                checkbox.stateChanged.connect(self.handle_current_checkbox_change)
                checkbox_widget = QWidget()
                checkbox_layout = QHBoxLayout(checkbox_widget)
                checkbox_layout.addWidget(checkbox)
                checkbox_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                checkbox_layout.setContentsMargins(0, 0, 0, 0)
                self.current_symbols_table.setCellWidget(i, 0, checkbox_widget)
                
                # Symbol
                symbol_item = QTableWidgetItem(symbol)
                self.current_symbols_table.setItem(i, 1, symbol_item)
                
                # Position
                position = position_amounts.get(symbol, 0)
                position_item = QTableWidgetItem(f"₺{position:,.2f}")
                position_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                self.current_symbols_table.setItem(i, 2, position_item)
            
            # Update available symbols table
            self.available_symbols_table.setRowCount(len(available_symbols))
            for i, symbol in enumerate(available_symbols):
                # Checkbox
                checkbox = QCheckBox()
                checkbox.stateChanged.connect(self.handle_available_checkbox_change)
                checkbox_widget = QWidget()
                checkbox_layout = QHBoxLayout(checkbox_widget)
                checkbox_layout.addWidget(checkbox)
                checkbox_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                checkbox_layout.setContentsMargins(0, 0, 0, 0)
                self.available_symbols_table.setCellWidget(i, 0, checkbox_widget)
                
                # Symbol
                symbol_item = QTableWidgetItem(symbol)
                self.available_symbols_table.setItem(i, 1, symbol_item)
                
                # Position
                position = position_amounts.get(symbol, 0)
                position_item = QTableWidgetItem(f"₺{position:,.2f}")
                position_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                self.available_symbols_table.setItem(i, 2, position_item)
            
            # Update the table counts after loading
            self.update_header_counts()
            self.load_maker_arbitrage_thresholds()
            logging.info("Loading EXCHANGE balances now vol2!")
            
        except Exception as e:
            logging.error(f"Error loading symbol lists: {e}")
            self.raise_error_popup(f"Failed to load symbols: {e}")

    def update_header_counts(self):
        """Update the counts in the headers"""
        current_count = self.current_symbols_table.rowCount()
        available_count = self.available_symbols_table.rowCount()
        
        self.left_header.setText(f"Current Symbols ({current_count})")
        self.right_header.setText(f"Available Symbols ({available_count})")

    def add_selected_symbols_to_list(self):
        """Add selected symbols to the current symbols list"""
        try:
            # Get selected symbols using checkboxes
            symbols_to_add = []
            for row in range(self.available_symbols_table.rowCount()):
                checkbox = self.available_symbols_table.cellWidget(row, 0).findChild(QCheckBox)
                if checkbox and checkbox.isChecked():
                    symbol = self.available_symbols_table.item(row, 1).text()
                    symbols_to_add.append(symbol)
            
            if not symbols_to_add:
                QMessageBox.warning(self, "Warning", "No symbols selected")
                return
            
            # Get current symbols from Redis
            symbols_data = self.redis.get('EXCHANGE_Symbol_List')
            self.current_symbols = json.loads(symbols_data.decode('utf-8')) if symbols_data else []
            
            # Add new symbols
            self.current_symbols.extend(symbols_to_add)
            self.current_symbols = sorted(list(set(self.current_symbols)))  # Remove duplicates and sort

            # Add new symbols
            symbols_to_add_exceptional = ['LUNA', 'BEAM', 'PEPE', 'BONK', 'SHIB', 'FLOKI']
            self.current_symbols.extend(symbols_to_add_exceptional)
            self.current_symbols.extend(symbols_to_add)
            self.current_symbols = sorted(list(set(self.current_symbols)))  # Remove duplicates and sort
            #Add USDT at the end of the list
            self.current_symbols.append('USDT')

            
            # Save back to Redis
            self.redis.set('EXCHANGE_Symbol_List', json.dumps(self.current_symbols))
            
            # Reset and rebuild arbitrage thresholds with all current symbols
            self.reset_and_rebuild_thresholds()
            
            # Reload symbols
            self.load_symbol_lists()
            
            QMessageBox.information(self, "Success", f"Added {len(symbols_to_add)} symbol(s) and rebuilt thresholds")
            
        except Exception as e:
            logging.error(f"Error adding symbols: {e}")
            QMessageBox.critical(self, "Error", f"Failed to add symbols: {e}")

    def reset_and_rebuild_thresholds(self):
        """Reset and rebuild arbitrage thresholds with all current symbols"""
        try:
            # Create a direct Redis connection
            r = self.redis
            
            logging.info("Resetting and rebuilding arbitrage thresholds")
            
            # Get existing thresholds to preserve values for existing symbols
            existing_thresholds = {}
            thresholds_data = r.get('maker_arbitrage_thresholds')
            if thresholds_data:
                try:
                    thresholds = json.loads(thresholds_data.decode('utf-8'))
                    for threshold in thresholds:
                        if 'Symbol' in threshold:
                            existing_thresholds[threshold['Symbol']] = threshold
                    logging.info(f"Preserved settings for {len(existing_thresholds)} existing symbols")
                except Exception as e:
                    logging.warning(f"Could not parse existing thresholds: {e}")
            
            # Create new thresholds list
            new_thresholds = []
            
            # Add all symbols with preserved or default values
            for symbol in self.current_symbols:
                if symbol in existing_thresholds:
                    # Use existing values
                    new_thresholds.append(existing_thresholds[symbol])
                    logging.info(f"Preserved existing settings for {symbol}")
                else:
                    # Use default values
                    new_thresholds.append({
                        'Symbol': symbol,
                        'OpenTriggerMargin': 0.004,
                        'CloseTriggerMargin': 0.004,
                        'Maker_Type': 0,
                        'OpenMarginWindow': 0.001,
                        'CloseMarginWindow': 0.001,
                        'OpenStopMargin': 0.003,
                        'CloseStopMargin': 0.003,
                        'OpenAggression': 1.0002,
                        'CloseAggression': 0.9998,
                        'MinBuyOrderAmount_TRY': 1000,
                        'BuyOrderAmount_TRY': 1000,
                        'MaxBuyOrderAmount_TRY': 100000,
                        'MinSellOrderAmount_TRY': 1000,
                        'MaxSellOrderAmount_TRY': 100000,
                        'MaxPositionAmount_TRY': 100000,
                        'MoveThreshold': 15
                    })
                    logging.info(f"Added default settings for {symbol}")
            
            # Save new thresholds to Redis
            r.set('maker_arbitrage_thresholds', json.dumps(new_thresholds))
            logging.info(f"Saved {len(new_thresholds)} threshold entries to Redis")
            
            # Verify the update
            verification = r.get('maker_arbitrage_thresholds')
            if verification:
                try:
                    verified_thresholds = json.loads(verification.decode('utf-8'))
                    logging.info(f"Verification successful, found {len(verified_thresholds)} entries")
                    
                    # Check if all symbols are in the verified thresholds
                    verified_symbols = {t.get('Symbol') for t in verified_thresholds if 'Symbol' in t}
                    missing_symbols = set(self.current_symbols) - verified_symbols
                    if missing_symbols:
                        logging.error(f"Verification failed: {len(missing_symbols)} symbols missing")
                    else:
                        logging.info("All symbols verified in thresholds")
                except Exception as verify_error:
                    logging.error(f"Verification error: {verify_error}")
            
            self.load_maker_arbitrage_thresholds()

            arbitrage_state = self.redis.get('maker_arbitrage_state')
            if arbitrage_state == b'running':
                self.redis.publish('arbit_commands', b'update_thresholds')
            
            
        except Exception as e:
            logging.error(f"Reset and rebuild error: {e}")
            QMessageBox.critical(self, "Error", f"Failed to reset and rebuild thresholds: {e}")


    def remove_selected_symbols_from_list(self):
        """Remove selected symbols from the current symbols list"""
        try:
            # Get selected symbols
            selected_rows = []
            selected_symbols = []
            
            for row in range(self.current_symbols_table.rowCount()):
                checkbox = self.current_symbols_table.cellWidget(row, 0).findChild(QCheckBox)
                if checkbox and checkbox.isChecked():
                    selected_rows.append(row)
                    symbol_item = self.current_symbols_table.item(row, 1)
                    if symbol_item:
                        selected_symbols.append(symbol_item.text())
            
            if not selected_symbols:
                QMessageBox.warning(self, "Warning", "No symbols selected")
                return
            
            # Get current symbol list from Redis
            symbols_data = self.redis.get('EXCHANGE_Symbol_List')
            if not symbols_data:
                QMessageBox.warning(self, "Warning", "No symbols found in Redis")
                return
                
            current_symbols = json.loads(symbols_data.decode('utf-8'))
            
            # Remove selected symbols
            updated_symbols = [s for s in current_symbols if s not in selected_symbols]
            
            # Save updated list back to Redis
            self.redis.set('EXCHANGE_Symbol_List', json.dumps(updated_symbols))
            
            # Publish update notification
            self.redis.publish('arbit_commands', b'update_symbols')
            
            # Remove from maker_arbitrage_thresholds
            thresholds_data = self.redis.get('maker_arbitrage_thresholds')
            if thresholds_data:
                thresholds = json.loads(thresholds_data.decode('utf-8'))
                
                # Handle both list and dictionary formats
                if isinstance(thresholds, list):
                    # For list format, filter out the selected symbols
                    thresholds = [item for item in thresholds if item.get('Symbol', item.get('symbol', '')) not in selected_symbols]
                elif isinstance(thresholds, dict):
                    # For dictionary format, remove the selected symbols
                    for symbol in selected_symbols:
                        if symbol in thresholds:
                            del thresholds[symbol]
                
                # Save updated thresholds back to Redis
                self.redis.set('maker_arbitrage_thresholds', json.dumps(thresholds))
            
            # Reload symbol lists
            self.load_symbol_lists()
            
            # Show success message
            QMessageBox.information(self, "Success", f"Removed {len(selected_symbols)} symbols")
            
        except Exception as e:
            logging.error(f"Error removing symbols: {e}")
            import traceback
            logging.error(traceback.format_exc())
            QMessageBox.critical(self, "Error", f"Failed to remove symbols: {e}")

    def check_system_status(self):
        """Check the status of the arbitrage system and update UI accordingly"""
        try:
            # Check if the arbitrage process is running
            arbitrage_state = self.redis.get('maker_arbitrage_state')
            enable_orders_state = self.redis.get('maker_enable_orders')

            if enable_orders_state != self.prev_enable_orders_state:
                if enable_orders_state == b'1':
                    self.orders_control_btn.setText("Orders Enabled\n(press to disable)")
                    self.orders_control_btn.setStyleSheet("""
                            QPushButton {background-color: green;}
                            QPushButton:hover {background-color: orange;}
                        """)
                elif enable_orders_state == b'0':
                    self.orders_control_btn.setText("Orders Disabled\n(press to enable)")
                    self.orders_control_btn.setStyleSheet("""
                            QPushButton {background-color: darkred;}
                            QPushButton:hover {background-color: red;}
                        """)
                self.orders_control_btn.setEnabled(True)

            if arbitrage_state != self.prev_arbitrage_state:
                if arbitrage_state == b'running':
                    logging.info("System running... updating UI")
                    self.system_control_btn.setEnabled(True)
                    self.system_control_btn.setText("SYSTEM RUNNING \n (press to STOP)")
                    self.system_control_btn.setStyleSheet("""
                            QPushButton {background-color: green;}
                            QPushButton:hover {background-color: orange;}
                        """)
                    
                    # Disable Symbol Management tab when system is running
                    self.tab_widget.setTabEnabled(3, False)  # Index 3 is Symbol Management tab (after removing Manual Trading)
                    self.tab_widget.setTabToolTip(3, "Symbol Management is disabled while the system is running")
                    self.update_balance_btn.setEnabled(True)

                    self.open_enable_orders_btn.setEnabled(True)
                    self.open_enable_orders_btn.setStyleSheet("""
                            QPushButton {background-color: green;}
                            QPushButton:hover {background-color: orange;}
                        """)
                    self.open_disable_orders_btn.setEnabled(True)
                    self.open_disable_orders_btn.setStyleSheet("""
                            QPushButton {background-color: darkred;}
                            QPushButton:hover {background-color: red;}
                        """)
                    
                    self.close_enable_orders_btn.setEnabled(True)
                    self.close_enable_orders_btn.setStyleSheet("""
                            QPushButton {background-color: green;}
                            QPushButton:hover {background-color: orange;}
                        """)
                    self.close_disable_orders_btn.setEnabled(True)
                    self.close_disable_orders_btn.setStyleSheet("""
                            QPushButton {background-color: darkred;}
                            QPushButton:hover {background-color: red;}
                        """)
                    
                    self.open_enable_combo_orders_btn.setEnabled(True)
                    self.open_enable_combo_orders_btn.setStyleSheet("""
                            QPushButton {background-color: darkblue;}
                            QPushButton:hover {background-color: blue;}
                        """)
                    

                elif arbitrage_state == b'stopped':
                    logging.info("System stopped... updating UI")
                    self.system_control_btn.setEnabled(True)
                    self.system_control_btn.setText("SYSTEM STOPPED \n (press to START)")
                    self.system_control_btn.setStyleSheet("""
                            QPushButton {background-color: darkred;}
                            QPushButton:hover { background-color: orange; }
                        """)
                    
                    # Enable Symbol Management tab when system is stopped
                    self.tab_widget.setTabEnabled(3, True)  # Index 3 is Symbol Management tab (after removing Manual Trading)
                    self.tab_widget.setTabToolTip(3, "")
                    
                    #self.update_balance_btn.setEnabled(False)

                    self.open_enable_orders_btn.setEnabled(False)
                    self.open_enable_orders_btn.setStyleSheet("""
                            QPushButton {color: gray;}
                        """)
                    self.open_disable_orders_btn.setEnabled(False)
                    self.open_disable_orders_btn.setStyleSheet("""
                            QPushButton {color: gray;}
                        """)
                    self.close_enable_orders_btn.setEnabled(False)
                    self.close_enable_orders_btn.setStyleSheet("""
                            QPushButton {color: gray;}
                        """)
                    self.close_disable_orders_btn.setEnabled(False)
                    self.close_disable_orders_btn.setStyleSheet("""
                            QPushButton {color: gray;}
                        """)
                    
                    self.open_enable_combo_orders_btn.setEnabled(False)
                    self.open_enable_combo_orders_btn.setStyleSheet("""
                            QPushButton {color: gray;}
                        """)
                    
            
            self.prev_arbitrage_state = arbitrage_state
            self.prev_enable_orders_state = enable_orders_state
            
        except Exception as e:
            logging.error(f"Error checking system status: {e}")

    def update_health_status(self):
        """Update health status lights from Redis every 2 seconds"""
        try:
            # Read Common_Data_Health from Redis (only updated by script1)
            # Format: [CS_data, EXCHANGE_data] where 1=healthy, 0=unhealthy
            data_health_data = self.redis.get('Common_Data_Health')
            if data_health_data:
                try:
                    data_health = json.loads(data_health_data.decode('utf-8'))
                    cs_data_healthy = data_health[0] == 1 if len(data_health) > 0 else False
                    EXCHANGE_data_healthy = data_health[1] == 1 if len(data_health) > 1 else False
                    
                    # Update CS Binance Data light (Yellow/Red)
                    if cs_data_healthy:
                        self.cs_data_light.setStyleSheet("background-color: yellow; border-radius: 7px;")
                    else:
                        self.cs_data_light.setStyleSheet("background-color: red; border-radius: 7px;")
                    
                    # Update EXCHANGE Data light (Yellow/Red)
                    if EXCHANGE_data_healthy:
                        self.EXCHANGE_data_light.setStyleSheet("background-color: yellow; border-radius: 7px;")
                    else:
                        self.EXCHANGE_data_light.setStyleSheet("background-color: red; border-radius: 7px;")
                except Exception as e:
                    logging.error(f"Error parsing Common_Data_Health: {e}")
            else:
                # No data available - set to gray
                self.cs_data_light.setStyleSheet("background-color: gray; border-radius: 7px;")
                self.EXCHANGE_data_light.setStyleSheet("background-color: gray; border-radius: 7px;")
            
            # Read order health from all scripts and aggregate
            # For EXCHANGE Order: check if any script has EXCHANGE send/listen sockets healthy
            # For CS Binance Order: check if any script has CS order socket healthy
            EXCHANGE_order_healthy = False
            cs_order_healthy = False
            
            for script_id in range(1, 9):  # Scripts 1-8
                order_health_key = f"Script{script_id}_Order_Health"
                order_health_data = self.redis.get(order_health_key)
                if order_health_data:
                    try:
                        order_health = json.loads(order_health_data.decode('utf-8'))
                        # Format: [CS_order_socket, EXCHANGE_send_order_socket, EXCHANGE_listen_socket]
                        if len(order_health) >= 3:
                            # CS Order Socket (index 0)
                            if order_health[0] == 1:
                                cs_order_healthy = True
                            # EXCHANGE Send Order Socket (index 1) OR EXCHANGE Listen Socket (index 2)
                            if order_health[1] == 1 or order_health[2] == 1:
                                EXCHANGE_order_healthy = True
                    except Exception as e:
                        logging.error(f"Error parsing {order_health_key}: {e}")
            
            # Update CS Binance Order light (Yellow/Red)
            if cs_order_healthy:
                self.cs_order_light.setStyleSheet("background-color: yellow; border-radius: 7px;")
            else:
                self.cs_order_light.setStyleSheet("background-color: red; border-radius: 7px;")
            
            # Update EXCHANGE Order light (Yellow/Red)
            if EXCHANGE_order_healthy:
                self.EXCHANGE_order_light.setStyleSheet("background-color: yellow; border-radius: 7px;")
            else:
                self.EXCHANGE_order_light.setStyleSheet("background-color: red; border-radius: 7px;")
                
        except Exception as e:
            logging.error(f"Error updating health status: {e}")

    def show_health_details(self):
        """Show detailed health status window with all scripts"""
        try:
            # Create health details window
            health_window = QMainWindow(self)
            health_window.setWindowTitle("Health Status Details")
            health_window.setGeometry(100, 100, 800, 600)
            
            # Create central widget and layout
            central_widget = QWidget()
            health_window.setCentralWidget(central_widget)
            layout = QVBoxLayout(central_widget)
            
            # Create table for health status
            health_table = QTableWidget()
            health_table.setColumnCount(6)
            health_table.setHorizontalHeaderLabels([
                "Component", "BINANCE Data", "EXCHANGE Data", 
                "CS Order Socket", "EXCHANGE Send Order", "EXCHANGE Listen Socket"
            ])
            health_table.horizontalHeader().setStretchLastSection(True)
            
            # Read and display data health (from Common_Data_Health)
            data_health_data = self.redis.get('Common_Data_Health')
            data_health_row = 0
            if data_health_data:
                try:
                    data_health = json.loads(data_health_data.decode('utf-8'))
                    health_table.insertRow(data_health_row)
                    health_table.setItem(data_health_row, 0, QTableWidgetItem("Common Data Health"))
                    health_table.setItem(data_health_row, 1, QTableWidgetItem("✓" if data_health[0] == 1 else "✗"))
                    health_table.setItem(data_health_row, 2, QTableWidgetItem("✓" if data_health[1] == 1 else "✗"))
                    health_table.setItem(data_health_row, 3, QTableWidgetItem("N/A"))
                    health_table.setItem(data_health_row, 4, QTableWidgetItem("N/A"))
                    health_table.setItem(data_health_row, 5, QTableWidgetItem("N/A"))
                    data_health_row += 1
                except Exception as e:
                    logging.error(f"Error parsing Common_Data_Health: {e}")
            
            # Read and display order health from all scripts
            for script_id in range(1, 9):
                order_health_key = f"Script{script_id}_Order_Health"
                order_health_data = self.redis.get(order_health_key)
                if order_health_data:
                    try:
                        order_health = json.loads(order_health_data.decode('utf-8'))
                        health_table.insertRow(data_health_row)
                        health_table.setItem(data_health_row, 0, QTableWidgetItem(f"Script {script_id} Order Health"))
                        health_table.setItem(data_health_row, 1, QTableWidgetItem("N/A"))
                        health_table.setItem(data_health_row, 2, QTableWidgetItem("N/A"))
                        if len(order_health) >= 3:
                            health_table.setItem(data_health_row, 3, QTableWidgetItem("✓" if order_health[0] == 1 else "✗"))
                            health_table.setItem(data_health_row, 4, QTableWidgetItem("✓" if order_health[1] == 1 else "✗"))
                            health_table.setItem(data_health_row, 5, QTableWidgetItem("✓" if order_health[2] == 1 else "✗"))
                        else:
                            health_table.setItem(data_health_row, 3, QTableWidgetItem("N/A"))
                            health_table.setItem(data_health_row, 4, QTableWidgetItem("N/A"))
                            health_table.setItem(data_health_row, 5, QTableWidgetItem("N/A"))
                        data_health_row += 1
                    except Exception as e:
                        logging.error(f"Error parsing {order_health_key}: {e}")
            
            # Resize columns to content
            health_table.resizeColumnsToContents()
            
            # Add table to layout
            layout.addWidget(health_table)
            
            # Add refresh button
            refresh_btn = QPushButton("Refresh")
            refresh_btn.clicked.connect(lambda: self.refresh_health_details(health_table))
            layout.addWidget(refresh_btn)
            
            # Show window
            health_window.show()
            
        except Exception as e:
            logging.error(f"Error showing health details: {e}")
            QMessageBox.critical(self, "Error", f"Failed to show health details: {e}")

    def refresh_health_details(self, health_table):
        """Refresh the health details table"""
        try:
            # Clear table
            health_table.setRowCount(0)
            
            # Read and display data health (from Common_Data_Health)
            data_health_data = self.redis.get('Common_Data_Health')
            data_health_row = 0
            if data_health_data:
                try:
                    data_health = json.loads(data_health_data.decode('utf-8'))
                    health_table.insertRow(data_health_row)
                    health_table.setItem(data_health_row, 0, QTableWidgetItem("Common Data Health"))
                    health_table.setItem(data_health_row, 1, QTableWidgetItem("✓" if data_health[0] == 1 else "✗"))
                    health_table.setItem(data_health_row, 2, QTableWidgetItem("✓" if data_health[1] == 1 else "✗"))
                    health_table.setItem(data_health_row, 3, QTableWidgetItem("N/A"))
                    health_table.setItem(data_health_row, 4, QTableWidgetItem("N/A"))
                    health_table.setItem(data_health_row, 5, QTableWidgetItem("N/A"))
                    data_health_row += 1
                except Exception as e:
                    logging.error(f"Error parsing Common_Data_Health: {e}")
            
            # Read and display order health from all scripts
            for script_id in range(1, 9):
                order_health_key = f"Script{script_id}_Order_Health"
                order_health_data = self.redis.get(order_health_key)
                if order_health_data:
                    try:
                        order_health = json.loads(order_health_data.decode('utf-8'))
                        health_table.insertRow(data_health_row)
                        health_table.setItem(data_health_row, 0, QTableWidgetItem(f"Script {script_id} Order Health"))
                        health_table.setItem(data_health_row, 1, QTableWidgetItem("N/A"))
                        health_table.setItem(data_health_row, 2, QTableWidgetItem("N/A"))
                        if len(order_health) >= 3:
                            health_table.setItem(data_health_row, 3, QTableWidgetItem("✓" if order_health[0] == 1 else "✗"))
                            health_table.setItem(data_health_row, 4, QTableWidgetItem("✓" if order_health[1] == 1 else "✗"))
                            health_table.setItem(data_health_row, 5, QTableWidgetItem("✓" if order_health[2] == 1 else "✗"))
                        else:
                            health_table.setItem(data_health_row, 3, QTableWidgetItem("N/A"))
                            health_table.setItem(data_health_row, 4, QTableWidgetItem("N/A"))
                            health_table.setItem(data_health_row, 5, QTableWidgetItem("N/A"))
                        data_health_row += 1
                    except Exception as e:
                        logging.error(f"Error parsing {order_health_key}: {e}")
            
            # Resize columns to content
            health_table.resizeColumnsToContents()
            
        except Exception as e:
            logging.error(f"Error refreshing health details: {e}")

    def toggle_history_column(self, table, column_index, show):
        """Toggle visibility of a column in a history table with special handling"""
        logging.info(f"Toggling column {column_index} visibility to {show} in history table")
        
        # Get column name for logging
        column_name = table.horizontalHeaderItem(column_index).text()
        logging.info(f"Column name: {column_name}")
        
        # Save current column width
        current_width = table.columnWidth(column_index)
        logging.info(f"Current column width: {current_width}")
        
        # If the column is currently hidden and we're showing it, 
        # make sure it has a reasonable width
        if table.isColumnHidden(column_index) and show and current_width < 10:
            # Set a default width for the column
            table.setColumnWidth(column_index, 100)
            logging.info(f"Setting default width to 100 for previously hidden column")
        
        # Disable updates temporarily
        table.setUpdatesEnabled(False)
        
        # Set column visibility
        table.setColumnHidden(column_index, not show)
        
        # If showing, explicitly resize the column to content
        if show:
            table.resizeColumnToContents(column_index)
            new_width = table.columnWidth(column_index)
            logging.info(f"Resized column to content, new width: {new_width}")
            
            # If the width is too small, set a minimum width
            if new_width < 50:
                table.setColumnWidth(column_index, 100)
                logging.info(f"Width too small, setting to 100")
        
        # Force update with multiple approaches
        table.setUpdatesEnabled(True)
        table.horizontalHeader().viewport().update()
        table.viewport().update()
        table.update()
        
        # Force layout update
        table.horizontalHeader().updateGeometry()
        table.updateGeometry()
        
        # Process events to ensure updates are applied
        QApplication.processEvents()
        
        # Verify column visibility
        is_hidden = table.isColumnHidden(column_index)
        current_width = table.columnWidth(column_index)
        logging.info(f"Column {column_index} hidden status after toggle: {is_hidden}, width: {current_width}")
        
        # Store the current visibility state
        if table == self.EXCHANGE_trade_table:
            self.EXCHANGE_column_state = [table.isColumnHidden(i) for i in range(table.columnCount())]
        elif table == self.binance_trade_table:
            self.binance_column_state = [table.isColumnHidden(i) for i in range(table.columnCount())]
        
        # Create a timer to check if column visibility is still correct after a short delay
        QTimer.singleShot(100, lambda: self.verify_column_visibility(table, column_index, show))

    def verify_column_visibility(self, table, column_index, should_be_visible):
        """Verify that a column's visibility is correct after a delay"""
        column_name = table.horizontalHeaderItem(column_index).text()
        is_hidden = table.isColumnHidden(column_index)
        
        logging.info(f"Verifying column {column_index} ({column_name}) visibility after delay")
        logging.info(f"Should be visible: {should_be_visible}, Is hidden: {is_hidden}")
        
        if should_be_visible and is_hidden:
            logging.info(f"Column {column_index} should be visible but is hidden, forcing visible")
            table.setColumnHidden(column_index, False)
            table.update()
        elif not should_be_visible and not is_hidden:
            logging.info(f"Column {column_index} should be hidden but is visible, forcing hidden")
            table.setColumnHidden(column_index, True)
            table.update()

    def convert_to_uppercase(self, text):
        """Convert search input to uppercase"""
        cursor_pos = self.symbol_search_input.cursorPosition()
        self.symbol_search_input.blockSignals(True)
        self.symbol_search_input.setText(text.upper())
        self.symbol_search_input.setCursorPosition(cursor_pos)
        self.symbol_search_input.blockSignals(False)

    def fetch_common_symbols(self):
        """Fetch common symbols between Binance and EXCHANGE"""
        try:
            # Initialize exchanges
            binance = ccxt.binance({
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'future'
                }
            })
            
            EXCHANGE = ccxt.EXCHANGE({
                'enableRateLimit': True
            })

            # Step 1: Load Binance markets
            self.right_header_status.setText("Loading Binance symbols...")
            QApplication.processEvents()
            binance_markets = binance.load_markets()
            self.right_header_status.setText("Binance symbols loaded")
            QApplication.processEvents()

            # Step 2: Load EXCHANGE markets
            self.right_header_status.setText("Loading EXCHANGE symbols...")
            QApplication.processEvents()
            EXCHANGE_markets = EXCHANGE.load_markets()
            self.right_header_status.setText("EXCHANGE symbols loaded")
            QApplication.processEvents()

            # Step 3: Find common symbols
            self.right_header_status.setText("Finding common symbols...")
            QApplication.processEvents()

            # Filter active markets
            active_binance_markets = {k: v for k, v in binance_markets.items() 
                                    if v['active'] and v['contract']}
            bases_binance = [v['base'] for v in active_binance_markets.values()]

            active_EXCHANGE_markets = {k: v for k, v in EXCHANGE_markets.items() 
                                    if v['active']}
            bases_EXCHANGE = [v['base'] for v in active_EXCHANGE_markets.values()]

            # Find common bases
            common_bases = set(bases_binance).intersection(set(bases_EXCHANGE))
            
            # Step 4: Get market data using bulk fetch
            self.right_header_status.setText("Fetching market data in bulk...")
            QApplication.processEvents()
            market_data = self.fetch_market_data_bulk(EXCHANGE)
            self.right_header_status.setText("Market data fetched")
            QApplication.processEvents()

            # Create final list with volume data
            common_symbols_with_data = []
            for base in common_bases:
                volume_try = market_data.get(base, {}).get('volume_try', 0)
                common_symbols_with_data.append({
                    'symbol': base,
                    'volume_try': volume_try
                })

            # Store volume data
            self.volumes = {item['symbol']: item['volume_try'] 
                           for item in common_symbols_with_data}

            self.right_header_status.setText("Symbols fetched successfully")
            QApplication.processEvents()
            
            return sorted(list(common_bases))

        except Exception as e:
            self.right_header_status.setText("Error fetching symbols")
            QApplication.processEvents()
            logging.error(f"Error fetching symbols: {e}")
            QMessageBox.critical(self, "Error", f"Failed to fetch symbols: {e}")
            return []

    def fetch_market_data_bulk(self, exchange):
        """Fetch market data using bulk ticker fetch"""
        try:
            # Get all active markets
            markets = exchange.fetch_markets()
            active_symbols = [m['symbol'] for m in markets if m['active']]
            
            # Fetch all tickers in bulk
            tickers = exchange.fetch_tickers(active_symbols)
            
            # Process the data
            market_data = {}
            for symbol, ticker in tickers.items():
                base = symbol.split('/')[0]  # Extract base symbol
                market_data[base] = {
                    'volume_try': ticker['quoteVolume']
                }
            
            return market_data

        except Exception as e:
            logging.error(f"Error in bulk market data fetch: {e}")
            return {}

    def fetch_and_update_symbols(self):
        """Fetch symbols and update the available symbols list"""
        try:
            # Fetch new symbols
            new_symbols = self.fetch_common_symbols()
            
            if not new_symbols:
                QMessageBox.warning(self, "Warning", "No symbols fetched")
                return
            
            # Update Redis
            self.redis.set('maker_fetched_symbols', json.dumps(new_symbols))
            
            # Reload symbols
            self.load_symbol_lists()
            
            QMessageBox.information(self, "Success", f"Fetched {len(new_symbols)} symbols")
            
            # Update the table counts after fetching
            self.update_header_counts()
            
        except Exception as e:
            logging.error(f"Error fetching symbols: {e}")
            QMessageBox.critical(self, "Error", f"Failed to fetch symbols: {e}")

    
    def on_tab_changed(self, index):
        """Handle tab changes."""
        logging.info(f"Tab {index} selected")

    def get_selected_symbols(self):
        """Get list of selected symbols"""
        selected_symbols = []
        for row in range(self.settings_table.rowCount()):
            # Check if the row is selected using the selection model
            if self.settings_table.selectionModel().isRowSelected(row, self.settings_table.rootIndex()):
                symbol_item = self.settings_table.item(row, self.symbol_column_index)
                if symbol_item:
                    symbol = symbol_item.text()
                    selected_symbols.append(symbol)
        return selected_symbols

    def update_table_cell_for_symbol(self, symbol, field_name, new_value):
        """Update a specific cell in the settings table for a given symbol (optimized - no full refresh)
        
        Args:
            symbol: The symbol to update
            field_name: The field name to update
            new_value: The new value to set
        """
        # Map field names to column indices
        field_to_column = {
            'OpenTriggerMargin': 1,
            'OpenMarginWindow': 2,
            'OpenAggression': 3,
            'CloseTriggerMargin': 4,
            'CloseMarginWindow': 5,
            'CloseAggression': 6,
            'MinBuyOrderAmount_TRY': self.min_buy_order_column_index,
            'BuyOrderAmount_TRY': self.buy_order_amount_column_index,
            'MaxBuyOrderAmount_TRY': self.max_buy_order_column_index,
            'MinSellOrderAmount_TRY': self.min_sell_order_column_index,
            'MaxSellOrderAmount_TRY': self.max_sell_order_column_index,
            'MinSellRatio': self.min_sell_ratio_column_index,
            'MaxSellRatio': self.max_sell_ratio_column_index,
            'MaxPositionAmount_TRY': self.max_position_column_index,
            'CurrentPositionAmount_TRY': self.current_position_TRY_index,
            'MoveThreshold': self.move_threshold_column_index,
            'ExceptionalMargin': self.exceptional_margin_column_index,
        }
        
        column_index = field_to_column.get(field_name)
        if column_index is None:
            return False
        
        # Find the row for this symbol
        for row in range(self.settings_table.rowCount()):
            symbol_item = self.settings_table.item(row, 0)
            if symbol_item and symbol_item.text() == symbol:
                # Format the value based on field type
                if field_name == 'ExceptionalMargin':
                    # Handle exceptional margin checkbox widget
                    widget = self.settings_table.cellWidget(row, column_index)
                    if widget:
                        checkbox = widget.findChild(QCheckBox)
                        if checkbox:
                            checkbox.setChecked(bool(new_value))
                    # Also update the underlying item for sorting
                    item = self.settings_table.item(row, column_index)
                    if item:
                        item.setData(Qt.ItemDataRole.EditRole, bool(new_value))
                else:
                    # Format numeric values (convert to basis points if needed, but NOT for sell ratios and aggressions)
                    # Sell ratios and aggressions should always be displayed as decimals
                    if field_name in ['MinSellRatio', 'MaxSellRatio', 'OpenAggression', 'CloseAggression']:
                        # Always display these as decimal values without multiplication
                        formatted_value = str(new_value)
                    elif isinstance(new_value, float) and new_value < 1:
                        formatted_value = str(int(round(new_value * 10000, 1)))
                    else:
                        formatted_value = str(new_value)
                    
                    # Update the cell item
                    item = self.settings_table.item(row, column_index)
                    if item:
                        item.setText(formatted_value)
                    else:
                        self.settings_table.setItem(row, column_index, QTableWidgetItem(formatted_value))
                
                return True
        
        return False

    def update_selected_symbols(self, field_name, new_value, respect_exceptional_margin=False, refresh_table=True):
        """Update thresholds for selected symbols
        
        Args:
            field_name: The field to update
            new_value: The new value to set
            respect_exceptional_margin: If True, exclude symbols with exceptional margin set.
                                      Only used for margin-related updates (Open/Close Margins, Update All Margins)
        """
        try:
            # Get arbitrage thresholds from Redis
            settings_data = self.redis.get('maker_arbitrage_thresholds')
            if not settings_data:
                QMessageBox.warning(self, "Warning", "No thresholds found")
                return False
                
            thresholds = json.loads(settings_data.decode('utf-8'))
            
            # Get selected symbols from the symbol table
            selected_symbols = self.get_selected_symbols()
            
            if not selected_symbols:
                QMessageBox.warning(self, "Warning", "No symbols selected")
                return False
            
            # Update only selected symbols, optionally excluding exceptional margin coins
            updated_count = 0
            excluded_symbols = []
            updated_symbols = []
            
            # Handle both list and dictionary formats
            if isinstance(thresholds, list):
                for threshold in thresholds:
                    symbol = threshold.get('Symbol', threshold.get('symbol', ''))
                    if symbol in selected_symbols:
                        # Check if this symbol has exceptional margin set (only if respect_exceptional_margin is True)
                        if respect_exceptional_margin:
                            exceptional_margin = threshold.get('ExceptionalMargin', False)
                            if exceptional_margin:
                                excluded_symbols.append(symbol)
                                logging.info(f"Skipped {symbol}: {field_name} (exceptional margin set)")
                                continue
                        
                        threshold[field_name] = new_value
                        updated_count += 1
                        updated_symbols.append(symbol)
                        logging.info(f"Updated {symbol}: {field_name} = {new_value}")
            elif isinstance(thresholds, dict):
                for symbol in selected_symbols:
                    if symbol in thresholds:
                        # Check if this symbol has exceptional margin set (only if respect_exceptional_margin is True)
                        if respect_exceptional_margin:
                            exceptional_margin = thresholds[symbol].get('ExceptionalMargin', False)
                            if exceptional_margin:
                                excluded_symbols.append(symbol)
                                logging.info(f"Skipped {symbol}: {field_name} (exceptional margin set)")
                                continue
                        
                        thresholds[symbol][field_name] = new_value
                        updated_count += 1
                        updated_symbols.append(symbol)
                        logging.info(f"Updated {symbol}: {field_name} = {new_value}")
            
            # Save updated thresholds back to Redis
            self.redis.set('maker_arbitrage_thresholds', json.dumps(thresholds))
            
            # Send command to update thresholds
            self.redis.publish('arbit_commands', b'update_thresholds')
            
            # Update table cells directly (optimized - no full refresh)
            if not refresh_table:
                # Disable sorting temporarily for faster updates
                was_sorting_enabled = self.settings_table.isSortingEnabled()
                self.settings_table.setSortingEnabled(False)
                
                for symbol in updated_symbols:
                    self.update_table_cell_for_symbol(symbol, field_name, new_value)
                
                # Re-enable sorting if it was enabled
                if was_sorting_enabled:
                    self.settings_table.setSortingEnabled(True)
            
            # Show success message in status bar
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message = f"{current_time} - Updated {updated_count} symbols"
            if excluded_symbols:
                message += f" (Excluded {len(excluded_symbols)} exceptional: {', '.join(excluded_symbols)})"
            else:
                message += f": {', '.join(selected_symbols)}"
            self.statusBar().showMessage(message)
            
            # Remember current search text and visible rows
            search_text = self.symbol_search_input.text()
            
            # Reapply search filter if there was one
            if search_text:
                self.symbol_search_input.setText(search_text)
                self.filter_symbols()
            
            return True
                
        except Exception as e:
            logging.error(f"Update error: {str(e)}")
            QMessageBox.critical(self, "Error", f"Failed to update symbols: {str(e)}")
            return False

    # Update the convert_to_uppercase method to handle both search inputs
    def convert_to_uppercase(self, text):
        """Convert search input to uppercase"""
        sender = self.sender()
        sender.blockSignals(True)
        sender.setText(text.upper())
        sender.blockSignals(False)

    def toggle_maker_type(self, type):
        try:  
            thresholds_data = self.redis.get('maker_arbitrage_thresholds')
            if not thresholds_data:
                return
            
            # Get selected symbols from the appropriate table based on which button was clicked
            symbols = []
            sender = self.sender()
            
            if sender == self.open_enable_orders_btn or sender == self.open_enable_combo_orders_btn or sender == self.open_disable_orders_btn:
                # Get selected symbols from open trade table
                for row in range(self.open_trade_table.rowCount()):
                    if self.open_trade_table.selectionModel().isRowSelected(row, self.open_trade_table.rootIndex()):
                        symbol_item = self.open_trade_table.item(row, 0)
                        if symbol_item:
                            symbols.append(symbol_item.text())
            elif sender == self.close_enable_orders_btn or sender == self.close_disable_orders_btn:
                # Get selected symbols from close trade table
                for row in range(self.close_trade_table.rowCount()):
                    if self.close_trade_table.selectionModel().isRowSelected(row, self.close_trade_table.rootIndex()):
                        symbol_item = self.close_trade_table.item(row, 0)
                        if symbol_item:
                            symbols.append(symbol_item.text())
                
            logging.info(f"Selected symbols: {symbols}")
            
            if not symbols:
                QMessageBox.warning(self, "Warning", "No symbols selected")
                return
            
            thresholds = json.loads(thresholds_data.decode('utf-8'))
            updated_count = 0
            
            for symbol in symbols:
                for threshold in thresholds:        
                    if threshold.get('Symbol') == symbol:
                        current_type = threshold.get('Maker_Type', 0)
                        if type == 9:
                            threshold['Maker_Type'] = 0
                        elif current_type == 0:
                            threshold['Maker_Type'] = type
                        updated_count += 1

            self.redis.set('maker_arbitrage_thresholds', json.dumps(thresholds))
            self.redis.publish('arbit_commands', b'update_thresholds')
            
            # Show success message
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.statusBar().showMessage(f"{current_time} - Updated {updated_count} symbols with maker type {type}")
            
        except Exception as e:
            logging.error(f"Error updating maker type: {e}")
            QMessageBox.critical(self, "Error", f"Failed to update maker type: {e}")
            

        except Exception as e:
            print(f"Error in toggle function: {e}")

    def update_table(self, table, df, columns, formatters):
        """Update a table with data from a DataFrame"""
        try:
            table_name = "open_trade" if table == self.open_trade_table else "close_trade" if table == self.close_trade_table else "unknown"
            #logging.info(f"=== Starting table update for {table_name} ===")
            
            # DISCONNECT SELECTION TRACKING DURING UPDATE TO PREVENT CLEARING
            if table == self.open_trade_table:
                try:
                    self.open_trade_table.selectionModel().selectionChanged.disconnect(self.manual_track_open_selection)
                except:
                    pass  # Already disconnected
                self.user_interacting = False  # Reset flag for table refresh
            elif table == self.close_trade_table:
                try:
                    self.close_trade_table.selectionModel().selectionChanged.disconnect(self.manual_track_close_selection)
                except:
                    pass  # Already disconnected
                self.user_interacting = False  # Reset flag for table refresh
            
            # SAVE CURRENT SELECTION BEFORE ANY TABLE OPERATIONS
            current_selection = set()
            if table == self.open_trade_table:
                for row in range(table.rowCount()):
                    if table.selectionModel().isRowSelected(row, table.rootIndex()):
                        symbol_item = table.item(row, 0)
                        if symbol_item:
                            current_selection.add(symbol_item.text())
                if current_selection:
                    self.previous_open_selection = current_selection.copy()
            elif table == self.close_trade_table:
                for row in range(table.rowCount()):
                    if table.selectionModel().isRowSelected(row, table.rootIndex()):
                        symbol_item = table.item(row, 0)
                        if symbol_item:
                            current_selection.add(symbol_item.text())
                if current_selection:
                    self.previous_close_selection = current_selection.copy()
            
            # Ensure DataFrame has all required columns, fill missing ones with None
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            
            # Filter DataFrame to only include specified columns
            filtered_df = df[columns].copy()
            
            # Set row count
            table.setRowCount(len(filtered_df))
            
            # Populate table
            for row_idx, (_, row) in enumerate(filtered_df.iterrows()):
                # Add data columns (no checkbox column anymore)
                col_offset = 0
                for col_idx, (col, formatter) in enumerate(zip(columns, formatters)):
                    try:
                        value = row[col]
                        formatted_value = formatter(value)
                        
                        # Ensure BaseSymbol is always treated as string
                        if col == 'BaseSymbol':
                            formatted_value = str(formatted_value) if formatted_value is not None else ""
                            item = QTableWidgetItem(formatted_value)
                        # Handle columns that might be strings but need numeric formatting
                        elif col in ['EXCHANGEPositionAmount_coin', 'BinancePositionAmount_coin', 'CapacityGap_TRY']:
                            try:
                                # Try to convert to float for numeric operations
                                if isinstance(value, str):
                                    numeric_value = float(value)
                                else:
                                    numeric_value = float(value) if value is not None else 0.0
                                item = NumericTableWidgetItem(formatted_value, numeric_value)
                                item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                            except (ValueError, TypeError):
                                # If conversion fails, treat as string
                                formatted_value = str(formatted_value) if formatted_value is not None else ""
                                item = QTableWidgetItem(formatted_value)
                        # Create appropriate table item based on column type
                        elif col in ['Amount', 'Price', 'TriggerMargin', 'OpenMargin', 'CloseMargin', 
                                   'MaxPositionAmount_TRY', 
                                   'BinancePositionAmount_usdt', 'BinancePositionAmount_TRY',
                                   'EXCHANGEPositionAmount_usdt', 'EXCHANGEPositionAmount_TRY',
                                   'OpenTriggerMargin', 'CloseTriggerMargin', 'OpenStopMargin', 'CloseStopMargin',
                                   'OpenAggression', 'CloseAggression', 'OpenMarginWindow', 'CloseMarginWindow',
                                   'Binance_TimeDiff', 'binance_absolute_time_diff']:  # CRITICAL: Must be numeric for proper sorting
                            try:
                                # Handle None/NaN values - use a large number for sorting so they appear last
                                if value is None or (isinstance(value, float) and (pd.isna(value) or np.isnan(value))):
                                    numeric_value = float('inf')  # Sort N/A values to the end
                                else:
                                    numeric_value = float(value)
                                item = NumericTableWidgetItem(formatted_value, numeric_value)
                                item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                            except (ValueError, TypeError):
                                # If conversion fails, use inf for sorting (appears last)
                                item = NumericTableWidgetItem(formatted_value, float('inf'))
                                item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                        else:
                            item = QTableWidgetItem(formatted_value)

                        # Set background color only for the maker_type cell
                        if col == 'Maker_Type' and (table == self.open_trade_table or table == self.close_trade_table):
                            maker_type = value
                            if maker_type == 1:  # Buy maker
                                item.setBackground(QColor(0, 100, 0))  # Dark green
                            elif maker_type == 3:  # Sell maker
                                item.setBackground(QColor(100, 0, 0))  # Dark red
                            elif maker_type == 13:  # Combo maker
                                item.setBackground(QColor(0, 0, 100))  # Dark blue

                        table.setItem(row_idx, col_idx + col_offset, item)
                    except Exception as e:
                        logging.error(f"Error formatting value for {col}: {e}")
                        item = QTableWidgetItem("")
                        table.setItem(row_idx, col_idx + col_offset, item)
            
            # Re-enable sorting first
            table.setSortingEnabled(True)
            
            # SMART RESTORATION - Preserve Qt's selection model
            if table == self.open_trade_table:
                if self.previous_open_selection and not self.is_restoring_open:
                    self.is_restoring_open = True
                    # Use a longer delay to ensure table is fully updated
                    QTimer.singleShot(10, lambda: self.restore_selection(table, self.previous_open_selection))
                # RECONNECT SELECTION TRACKING
                try:
                    self.open_trade_table.selectionModel().selectionChanged.connect(self.manual_track_open_selection)
                except:
                    pass  # Already connected
            elif table == self.close_trade_table:
                if self.previous_close_selection and not self.is_restoring_close:
                    self.is_restoring_close = True
                    # Use a longer delay to ensure table is fully updated
                    QTimer.singleShot(10, lambda: self.restore_selection(table, self.previous_close_selection))
                # RECONNECT SELECTION TRACKING
                try:
                    self.close_trade_table.selectionModel().selectionChanged.connect(self.manual_track_close_selection)
                except:
                    pass  # Already connected
           
        except Exception as e:
            logging.error(f"Error updating table: {e}")

    def manual_track_close_selection(self):
        """Manually track close trade selection changes - let Qt handle the selection logic"""
        try:
            # Skip tracking if we're in the middle of restoring
            if self.is_restoring_close:
                return
                
            # Simply capture the current selection from Qt
            current_selection = set()
            for row in range(self.close_trade_table.rowCount()):
                if self.close_trade_table.selectionModel().isRowSelected(row, self.close_trade_table.rootIndex()):
                    symbol_item = self.close_trade_table.item(row, 0)
                    if symbol_item:
                        symbol = symbol_item.text()
                        current_selection.add(symbol)
            
            # Update our tracking with Qt's selection
            if current_selection:
                self.current_close_selection = current_selection.copy()
                self.previous_close_selection = self.current_close_selection.copy()
                # Synchronize selection with open trade table
                self.synchronize_selection_to_open_table(current_selection)
                #logging.info(f"Qt selection updated for close trade: {self.previous_close_selection}")
            else:
                # If no current selection, clear the global tracking
                self.current_close_selection.clear()
                
                # Only restore if we have a previous selection and we're not already restoring
                # if self.previous_close_selection and not self.is_restoring_close:
                #     logging.info(f"RESTORING close selection: {self.previous_close_selection}")
                # elif self.previous_close_selection:
                #     logging.info(f"NOT RESTORING close selection - already restoring or no previous selection")
                # else:
                #     logging.info(f"NO PREVIOUS CLOSE SELECTION TO RESTORE")
                
                # Always try to restore if we have a previous selection and we're not already restoring
                if self.previous_close_selection and not self.is_restoring_close and not self.current_close_selection:
                    #logging.info(f"RESTORING close selection: {self.previous_close_selection}")
                    self.is_restoring_close = True
                    # Use a short delay to let Qt finish its operations
                    QTimer.singleShot(15, lambda: self.restore_selection(self.close_trade_table, self.previous_close_selection))
        except Exception as e:
            logging.error(f"Error in manual close selection tracking: {e}")
    
    def manual_track_open_selection(self):
        """Manually track open trade selection changes - let Qt handle the selection logic"""
        try:
            # Skip tracking if we're in the middle of restoring
            if self.is_restoring_open:
                return
                
            # Simply capture the current selection from Qt
            current_selection = set()
            for row in range(self.open_trade_table.rowCount()):
                if self.open_trade_table.selectionModel().isRowSelected(row, self.open_trade_table.rootIndex()):
                    symbol_item = self.open_trade_table.item(row, 0)
                    if symbol_item:
                        symbol = symbol_item.text()
                        current_selection.add(symbol)
            
            # Update our tracking with Qt's selection
            if current_selection:
                self.current_open_selection = current_selection.copy()
                self.previous_open_selection = self.current_open_selection.copy()
                # Synchronize selection with close trade table
                self.synchronize_selection_to_close_table(current_selection)
                #logging.info(f"Qt selection updated for open trade: {self.previous_open_selection}")
            else:
                # If no current selection, clear the global tracking
                self.current_open_selection.clear()
                
                # Only restore if we have a previous selection and we're not already restoring
                # if self.previous_open_selection and not self.is_restoring_open:
                #     logging.info(f"RESTORING open selection: {self.previous_open_selection}")
                # elif self.previous_open_selection:
                #     logging.info(f"NOT RESTORING open selection - already restoring or no previous selection")
                # else:
                #     logging.info(f"NO PREVIOUS OPEN SELECTION TO RESTORE")
                
                # Always try to restore if we have a previous selection and we're not already restoring
                if self.previous_open_selection and not self.is_restoring_open and not self.current_open_selection:
                    #logging.info(f"RESTORING open selection: {self.previous_open_selection}")
                    self.is_restoring_open = True
                    # Use a short delay to let Qt finish its operations
                    QTimer.singleShot(15, lambda: self.restore_selection(self.open_trade_table, self.previous_open_selection))
        except Exception as e:
            logging.error(f"Error in manual open selection tracking: {e}")

    
    def restore_selection(self, table, selected_symbols):
        """Restore selection based on symbol names"""
        try:
            if not selected_symbols:
                return
                
            # Clear current selection
            table.clearSelection()
            
            # Select rows that match the saved symbols
            selected_count = 0
            selection_model = table.selectionModel()
            
            for row in range(table.rowCount()):
                symbol_item = table.item(row, 0)  # Symbol is in first column
                if symbol_item:
                    symbol = symbol_item.text()
                    if symbol in selected_symbols:
                        # Use QItemSelectionModel to add to selection instead of replacing
                        index = table.model().index(row, 0)
                        selection_model.select(index, QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)
                        selected_count += 1
                        #logging.info(f"SELECTING ROW {row} for symbol {symbol}")
            
            if selected_count > 0:
                table_name = "open_trade" if table == self.open_trade_table else "close_trade" if table == self.close_trade_table else "unknown"
                #logging.info(f"Restored {selected_count} rows for {table_name}: {selected_symbols}")
            else:
                table_name = "open_trade" if table == self.open_trade_table else "close_trade" if table == self.close_trade_table else "unknown"
                logging.info(f"Failed to restore any rows for {table_name}: {selected_symbols}")
            
            # Reset restoration flags
            if table == self.open_trade_table:
                self.is_restoring_open = False
            elif table == self.close_trade_table:
                self.is_restoring_close = False
                        
        except Exception as e:
            logging.error(f"Error restoring selection: {e}")
            import traceback
            logging.error(traceback.format_exc())
            
            # Reset restoration flags on error too
            if table == self.open_trade_table:
                self.is_restoring_open = False
            elif table == self.close_trade_table:
                self.is_restoring_close = False
    
    def synchronize_selection_to_open_table(self, symbols):
        """Synchronize selection to open trade table"""
        try:
            # Temporarily disable selection tracking to prevent infinite loop
            self.is_restoring_open = True
            
            # Clear current selection
            self.open_trade_table.clearSelection()
            
            # Select matching symbols in open trade table
            selection_model = self.open_trade_table.selectionModel()
            for row in range(self.open_trade_table.rowCount()):
                symbol_item = self.open_trade_table.item(row, 0)
                if symbol_item and symbol_item.text() in symbols:
                    index = self.open_trade_table.model().index(row, 0)
                    selection_model.select(index, QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)
            
            # Update current selection tracking and preserve previous selection
            self.current_open_selection = symbols.copy()
            self.previous_open_selection = symbols.copy()
            
            # Re-enable selection tracking after a short delay
            QTimer.singleShot(50, lambda: setattr(self, 'is_restoring_open', False))
            
        except Exception as e:
            logging.error(f"Error synchronizing selection to open table: {e}")
            self.is_restoring_open = False
    
    def synchronize_selection_to_close_table(self, symbols):
        """Synchronize selection to close trade table"""
        try:
            # Temporarily disable selection tracking to prevent infinite loop
            self.is_restoring_close = True
            
            # Clear current selection
            self.close_trade_table.clearSelection()
            
            # Select matching symbols in close trade table
            selection_model = self.close_trade_table.selectionModel()
            for row in range(self.close_trade_table.rowCount()):
                symbol_item = self.close_trade_table.item(row, 0)
                if symbol_item and symbol_item.text() in symbols:
                    index = self.close_trade_table.model().index(row, 0)
                    selection_model.select(index, QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows)
            
            # Update current selection tracking and preserve previous selection
            self.current_close_selection = symbols.copy()
            self.previous_close_selection = symbols.copy()
            
            # Re-enable selection tracking after a short delay
            QTimer.singleShot(50, lambda: setattr(self, 'is_restoring_close', False))
            
        except Exception as e:
            logging.error(f"Error synchronizing selection to close table: {e}")
            self.is_restoring_close = False
    




    class NumericTableWidgetItem(QTableWidgetItem):
        """Custom QTableWidgetItem that sorts numerically"""
        def __init__(self, text, value):
            super().__init__(str(text))
            self.numeric_value = value
            
        def __lt__(self, other):
            if isinstance(other, NumericTableWidgetItem):
                # Simple numeric comparison of the stored numeric values
                return self.numeric_value < other.numeric_value
            return super().__lt__(other)

    class StableNumericTableWidgetItem(QTableWidgetItem):
        """A QTableWidgetItem that sorts by numeric value first, then by symbol name for stable sorting."""
        def __init__(self, text, value, symbol):
            super().__init__(str(text))
            # Ensure all values are native Python types, not NumPy types
            self.value = float(value) if value is not None else 0.0  # Convert to native Python float
            self.symbol = str(symbol) if symbol is not None else ""  # Convert to native Python string
            
        def __lt__(self, other):
            if isinstance(other, ArbitrageMonitor.StableNumericTableWidgetItem):
                # First compare by numeric value
                if self.value != other.value:
                    return self.value < other.value
                else:
                    # If numeric values are equal (like both are 0), sort by symbol name alphabetically
                    # Changed from > to < for proper alphabetical sorting
                    return self.symbol < other.symbol
            return super().__lt__(other)

    def cleanup_button_cache(self):
        """Clean up invalid buttons from the cache"""
        keys_to_remove = []
        for key, btn in self.button_cache.items():
            if not btn or not btn.isWidgetType() or btn.parent() is None:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.button_cache[key]

class DetachableTabWidget(QTabWidget):
    """A tab widget that allows tabs to be detached and reattached"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.tabBar().setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tabBar().customContextMenuRequested.connect(self.showTabContextMenu)
        self.detached_tabs = {}  # Keep track of detached tabs: {tab_index: window}
        
    def showTabContextMenu(self, point):
        """Show context menu for tabs"""
        index = self.tabBar().tabAt(point)
        if index < 0:
            return
            
        menu = QMenu(self)
        
        # Check if this tab is already detached
        if index in self.detached_tabs and self.detached_tabs[index]:
            action = QAction("Attach Tab", self)
            action.triggered.connect(lambda: self.attachTab(index))
        else:
            action = QAction("Detach Tab", self)
            action.triggered.connect(lambda: self.detachTab(index))
            
        menu.addAction(action)
        menu.exec(self.tabBar().mapToGlobal(point))
        
    def detachTab(self, index):
        """Detach the tab at the given index into a separate window"""
        # Get tab information before removing it
        tab_text = self.tabText(index)
        tab_content = self.widget(index)
        
        if not tab_content:
            logging.info("No content to detach")
            return
        
        # Debug: Print the type of the widget being detached
        logging.info(f"Detaching tab: {tab_text}, Content type: {type(tab_content)}")
        
        # Remove the tab from the tab widget
        self.removeTab(index)
        
        # Create a new window for the detached tab
        detached_tab = QMainWindow(self)
        detached_tab.setWindowTitle(tab_text)
        
        # Set the tab content as the central widget of the new window
        tab_content.setParent(None)  # Remove parent to avoid issues
        detached_tab.setCentralWidget(tab_content)
        
        # Ensure the widget and its children are visible
        tab_content.setVisible(True)
        for child in tab_content.findChildren(QWidget):
            child.setVisible(True)
        
        # Force update to ensure the widget is redrawn
        tab_content.update()
        tab_content.repaint()
        
        # Set a reasonable size and position for the new window
        detached_tab.setGeometry(300, 300, 800, 600)
        
        # Handle window close event to reattach the tab
        detached_tab.closeEvent = lambda event, idx=index, content=tab_content, text=tab_text: self.handleDetachedTabClose(event, idx, content, text)
        
        # Store reference to the detached window
        self.detached_tabs[index] = detached_tab
        
        # Show the detached window
        detached_tab.show()

    def handleDetachedTabClose(self, event, index, content, text):
        """Handle the close event of a detached tab window"""
        # Add the content back to the tab widget
        self.insertTab(index, content, text)
        
        # Remove the reference
        self.detached_tabs[index] = None
        
        # Accept the close event
        event.accept()
        
    def attachTab(self, index):
        """Reattach a previously detached tab"""
        if index not in self.detached_tabs or not self.detached_tabs[index]:
            return
            
        # Get the detached window
        window = self.detached_tabs[index]
        
        # Get the content widget
        content = window.centralWidget()
        
        # Set parent back to None before re-adding to tab widget
        content.setParent(None)
        
        # Add the content back to the tab widget
        self.insertTab(index, content, self.tabText(index))
        
        # Close the detached window
        window.close()
        
        # Remove the reference
        self.detached_tabs[index] = None
        
    def handleDetachedTabClose(self, event, index, content, text):
        """Handle the close event of a detached tab window"""
        # Add the content back to the tab widget
        self.insertTab(index, content, text)
        
        # Remove the reference
        self.detached_tabs[index] = None
        
        # Accept the close event
        event.accept()

def main():
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle('Fusion')
    
    # Delete maker_arbitrage_table from Redis before starting GUI
    # This ensures a fresh start with the correct schema (handles schema changes)
    try:
        redis_client = redis.Redis(host='localhost', port=6379)
        redis_client.delete('maker_arbitrage_table')
        logging.info("Deleted maker_arbitrage_table from Redis before starting GUI")
    except Exception as e:
        logging.warning(f"Could not delete maker_arbitrage_table from Redis: {e}")
        # Continue anyway - the GUI will handle schema mismatches gracefully
    
    # Create and show the main window
    window = ArbitrageMonitor()
    window.show()
    
    sys.exit(app.exec())  # Changed from exec_() to exec()

if __name__ == "__main__":
    main()