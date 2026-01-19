#!/usr/bin/env python3
"""
Generate 10 arbit_core_maker_BTR_X.py scripts (X = 1 to 10) from template.
Each script handles 29 symbols (290 total / 10 scripts = 29 per script).
"""

import shutil
import re
import json
from pathlib import Path

TEMPLATE_FILE = "arbit_core_maker_BTR_1.py"
NUM_SCRIPTS = 10
TOTAL_SYMBOLS = 290
SYMBOLS_PER_SCRIPT = TOTAL_SYMBOLS // NUM_SCRIPTS  # 29

def load_symbols_from_json():
    """Load symbols from common_symbol_info.json and create symbol groups."""
    try:
        with open("common_symbol_info.json", 'r') as f:
            data = json.load(f)
        
        symbols = data.get('symbols', [])
        
        # Extract base symbols and create Binance Futures symbol list
        base_symbols = []
        binance_futures_symbols = []
        
        for symbol_info in symbols:
            base_symbol = symbol_info.get('base_symbol', '')
            binance_futures_symbol = symbol_info.get('binance_futures_symbol', '')
            
            if base_symbol and binance_futures_symbol:
                base_symbols.append(base_symbol)
                binance_futures_symbols.append(binance_futures_symbol)
        
        # Create symbol groups (29 symbols per script)
        symbol_groups = []
        for i in range(NUM_SCRIPTS):
            start_idx = i * SYMBOLS_PER_SCRIPT
            end_idx = start_idx + SYMBOLS_PER_SCRIPT
            group = binance_futures_symbols[start_idx:end_idx]
            symbol_groups.append(group)
        
        # Create combined_symbol_list (all base symbols)
        combined_symbol_list = base_symbols
        
        return symbol_groups, combined_symbol_list, binance_futures_symbols
        
    except Exception as e:
        print(f"Error loading symbols: {e}")
        return None, None, None


def generate_symbol_groups_code(symbol_groups, combined_symbol_list):
    """Generate Python code for symbol groups and combined list."""
    # Format symbol groups
    groups_code = "binance_symbol_groups = [\n"
    for i, group in enumerate(symbol_groups):
        groups_code += f"    {group},\n"  # List of symbols
    groups_code += "]\n"
    
    # Format combined list
    combined_code = f"combined_symbol_list = {combined_symbol_list}\n"
    
    return groups_code, combined_code


def generate_maker_scripts():
    """Generate 10 arbit_core_maker_BTR_X.py scripts."""
    template_path = Path(TEMPLATE_FILE)
    
    if not template_path.exists():
        print(f"Error: Template file {TEMPLATE_FILE} not found")
        return False
    
    # Load symbols and create groups
    symbol_groups, combined_symbol_list, binance_futures_symbols = load_symbols_from_json()
    
    if not symbol_groups or not combined_symbol_list:
        print("Error: Failed to load symbols from common_symbol_info.json")
        return False
    
    print(f"Loaded {len(combined_symbol_list)} symbols")
    print(f"Created {len(symbol_groups)} symbol groups ({SYMBOLS_PER_SCRIPT} symbols each)")
    
    # Read template
    with open(template_path, 'r', encoding='utf-8') as f:
        template_content = f.read()
    
    # Generate symbol groups code to add to config
    groups_code, combined_code = generate_symbol_groups_code(symbol_groups, combined_symbol_list)
    
    # Generate each script
    for script_id in range(1, NUM_SCRIPTS + 1):
        output_file = f"arbit_core_maker_BTR_{script_id}.py"
        script_content = template_content
        
        # Replace SCRIPT_ID line - match the exact pattern from template
        # Pattern: SCRIPT_ID = 1  # Change this to 1, 2, 3, or 4 for different scripts
        script_content = re.sub(
            r'SCRIPT_ID = \d+\s+#\s*Change this to.*?for different scripts',
            f'SCRIPT_ID = {script_id}  # Script {script_id} of {NUM_SCRIPTS}',
            script_content,
            count=1
        )
        
        # Also handle any already-generated format with "Script X of Y"
        script_content = re.sub(
            r'SCRIPT_ID = \d+\s+#\s*Script \d+ of \d+.*?Script \d+ of \d+.*?Change this to.*?for different scripts',
            f'SCRIPT_ID = {script_id}  # Script {script_id} of {NUM_SCRIPTS}',
            script_content
        )
        
        # Final cleanup - remove any duplicate "Script X of Y" patterns
        script_content = re.sub(
            r'(SCRIPT_ID = \d+\s+#\s*Script \d+ of \d+)\s+Script \d+ of \d+.*?Change this to.*?for different scripts',
            r'\1',
            script_content
        )
        
        # Update SCRIPT_ID validation to allow 1-10
        script_content = re.sub(
            r'if SCRIPT_ID not in \[.*?\]:',
            f'if SCRIPT_ID not in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:',
            script_content
        )
        
        # Update error message
        script_content = re.sub(
            r'logging\.error\(f"Invalid SCRIPT_ID: \{SCRIPT_ID\}. Must be 1, 2, 3, 4"\)',
            f'logging.error(f"Invalid SCRIPT_ID: {{SCRIPT_ID}}. Must be 1-10")',
            script_content
        )
        
        # Update CPU_CORES mapping (extend to 10 scripts)
        # Cores 0-2 reserved for C++ clients, scripts use cores 3-12
        cpu_cores_code = """CPU_CORES = {
    1: 3,   # Script 1 -> CPU Core 3 (Cores 0-2 reserved for C++ clients)
    2: 4,   # Script 2 -> CPU Core 4
    3: 5,   # Script 3 -> CPU Core 5
    4: 6,   # Script 4 -> CPU Core 6
    5: 7,   # Script 5 -> CPU Core 7
    6: 8,   # Script 6 -> CPU Core 8
    7: 9,   # Script 7 -> CPU Core 9
    8: 10,  # Script 8 -> CPU Core 10
    9: 11,  # Script 9 -> CPU Core 11
    10: 12, # Script 10 -> CPU Core 12
}"""
        
        script_content = re.sub(
            r'CPU_CORES = \{.*?\n\}',
            cpu_cores_code,
            script_content,
            flags=re.DOTALL
        )
        
        # Update comment about CPU cores
        script_content = re.sub(
            r'# Python arbitrage scripts use cores 3-10 \(8 scripts, 1 per core, isolated\)',
            f'# Python arbitrage scripts use cores 3-12 (10 scripts, 1 per core, isolated)',
            script_content
        )
        
        # Handle BINANCE_ws_uri_ticker - use modulo to cycle through available URIs
        script_content = re.sub(
            r'BINANCE_ws_uri_ticker = arbit_config\.BINANCE_ws_uris_ticker\[SCRIPT_ID - 1\]',
            f'# Use modulo to cycle through available URIs if less than 10\n'
            f'BINANCE_ws_uri_ticker = arbit_config.BINANCE_ws_uris_ticker[(SCRIPT_ID - 1) % len(arbit_config.BINANCE_ws_uris_ticker)] if hasattr(arbit_config, \'BINANCE_ws_uris_ticker\') and len(arbit_config.BINANCE_ws_uris_ticker) > 0 else None',
            script_content
        )
        
        # Handle BINANCE_ws_uri_depth similarly
        script_content = re.sub(
            r'BINANCE_ws_uri_depth = arbit_config\.BINANCE_ws_uris_depth\[SCRIPT_ID - 1\]',
            f'# Use modulo to cycle through available URIs if less than 10\n'
            f'BINANCE_ws_uri_depth = arbit_config.BINANCE_ws_uris_depth[(SCRIPT_ID - 1) % len(arbit_config.BINANCE_ws_uris_depth)] if hasattr(arbit_config, \'BINANCE_ws_uris_depth\') and len(arbit_config.BINANCE_ws_uris_depth) > 0 else None',
            script_content
        )
        
        # Update import to use new BinTR reader
        script_content = re.sub(
            r'from python_BTR_shared_memory_reader import SharedMemoryReader, run_shared_memory_reader',
            f'from python_BinTR_orderbook_reader import BinTROrderbookReader, run_bintr_shared_memory_reader as run_shared_memory_reader',
            script_content
        )
        
        # Write generated script
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        # Make executable
        Path(output_file).chmod(0o755)
        
        print(f"Generated: {output_file} (Script ID: {script_id}, {len(symbol_groups[script_id-1])} symbols)")
    
    # Also update arbit_config_maker_BTR.py to include symbol groups
    print("\nUpdating arbit_config_maker_BTR.py with symbol groups...")
    update_config_with_symbol_groups(symbol_groups, combined_symbol_list, binance_futures_symbols)
    
    print(f"\n✓ Generated {NUM_SCRIPTS} arbit_core_maker_BTR scripts")
    print(f"  Each script handles {SYMBOLS_PER_SCRIPT} symbols")
    print(f"  Total symbols: {TOTAL_SYMBOLS}")
    
    return True


def update_config_with_symbol_groups(symbol_groups, combined_symbol_list, binance_futures_symbols):
    """Add symbol groups and combined_symbol_list to arbit_config_maker_BTR.py."""
    config_file = Path("arbit_config_maker_BTR.py")
    
    if not config_file.exists():
        print(f"Warning: {config_file} not found, skipping symbol groups update")
        return
    
    with open(config_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if symbol groups already exist
    if 'binance_symbol_groups' in content:
        print("  Symbol groups already exist in config, updating them...")
        # Remove old groups and add new ones
        content = re.sub(r'binance_symbol_groups = \[.*?\]', '', content, flags=re.DOTALL)
        content = re.sub(r'combined_symbol_list = \[.*?\]', '', content, flags=re.DOTALL)
    
    # Find where to insert (after binance_symbol_list definition)
    insert_point = content.find('if not BinanceTR_symbol_list or not binance_symbol_list:')
    
    if insert_point == -1:
        print("  Could not find insertion point in config file")
        return
    
    # Generate symbol groups code
    groups_lines = ["\n# Symbol groups for distributed processing (10 scripts, 29 symbols each)\n"]
    groups_lines.append("# Generated by generate_maker_scripts.py\n")
    groups_lines.append("binance_symbol_groups = [\n")
    for group in symbol_groups:
        groups_lines.append(f"    {group},\n")
    groups_lines.append("]\n\n")
    
    # Combined symbol list (base symbols)
    base_symbols = [s.replace('USDT', '') for s in binance_futures_symbols]
    # Handle exceptional symbols
    base_symbols = [s.replace('LUNA2', 'LUNA').replace('BEAMX', 'BEAM') for s in base_symbols]
    base_symbols = [s.replace('1000PEPE', 'PEPE').replace('1000BONK', 'BONK').replace('1000SHIB', 'SHIB').replace('1000FLOKI', 'FLOKI') for s in base_symbols]
    
    groups_lines.append(f"# Combined symbol list (all base symbols, {len(base_symbols)} total)\n")
    groups_lines.append(f"# Generated by generate_maker_scripts.py\n")
    groups_lines.append(f"combined_symbol_list = {base_symbols}\n\n")
    
    # Add placeholder URIs if they don't exist
    if 'BINANCE_ws_uris_ticker' not in content:
        groups_lines.append("# Placeholder WebSocket URIs (update these with actual values)\n")
        groups_lines.append("# BINANCE_ws_uris_ticker = [\n")
        groups_lines.append("#     'wss://fstream.binance.com/ws',  # Add your WebSocket URIs here\n")
        groups_lines.append("#     # Add more URIs as needed\n")
        groups_lines.append("# ]\n")
        groups_lines.append("# BINANCE_ws_uris_depth = [\n")
        groups_lines.append("#     'wss://fstream.binance.com/ws',  # Add your WebSocket URIs here\n")
        groups_lines.append("#     # Add more URIs as needed\n")
        groups_lines.append("# ]\n")
        groups_lines.append("# BinTR_HFT_uri = 'wss://ws-api.binance.com:443/ws-api/v3'  # Update with actual BinTR WebSocket URI\n\n")
    
    # Insert before the warning check
    new_content = (
        content[:insert_point] +
        ''.join(groups_lines) +
        content[insert_point:]
    )
    
    with open(config_file, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"  ✓ Updated {config_file} with symbol groups and combined_symbol_list")


if __name__ == "__main__":
    print("=" * 80)
    print("Generating 10 arbit_core_maker_BTR scripts")
    print("=" * 80)
    
    success = generate_maker_scripts()
    
    if success:
        print("\n" + "=" * 80)
        print("SUCCESS: All scripts generated!")
        print("=" * 80)
        print("\nNext steps:")
        print("1. Review the generated scripts")
        print("2. Update arbit_config_maker_BTR.py if needed (BINANCE_ws_uris_ticker, etc.)")
        print("3. Test each script individually")
    else:
        print("\n" + "=" * 80)
        print("ERROR: Script generation failed!")
        print("=" * 80)
