#!/usr/bin/env python3
"""
CS Host Health Check and Coin Distribution Module

This module provides functions for:
- Health checking CS hosts
- Getting all available hosts from CS_INSTRUMENTS_MAP
- Distributing instruments evenly across healthy hosts
"""

import asyncio
import logging
from typing import List, Dict, Tuple, Set


async def health_check_cs_host(host_str: str, timeout: float = 5.0, script_id: int = None) -> bool:
    """
    Perform a health check on the CS host by attempting a TCP connection.
    Returns True if host is reachable, False otherwise.
    
    Args:
        host_str: Host string in format "host:port"
        timeout: Timeout in seconds for the connection attempt
        script_id: Optional script ID for logging context
        
    Returns:
        True if host is reachable, False otherwise
    """
    try:
        host, port = host_str.split(":")
        port = int(port)
        
        # Try to establish a TCP connection
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        log_prefix = f"Script {script_id}: " if script_id else ""
        logging.info(f"{log_prefix}Health check passed for {host_str}")
        return True
    except asyncio.TimeoutError:
        log_prefix = f"Script {script_id}: " if script_id else ""
        logging.warning(f"{log_prefix}Health check timeout for {host_str}")
        return False
    except Exception as e:
        log_prefix = f"Script {script_id}: " if script_id else ""
        logging.warning(f"{log_prefix}Health check failed for {host_str}: {e}")
        return False


def get_all_available_hosts(cs_instruments_map: Dict, instrument_ids: List[int] = None) -> List[str]:
    """
    Get all unique hosts from CS_INSTRUMENTS_MAP.
    If instrument_ids is provided, only get hosts for those specific instruments.
    
    Args:
        cs_instruments_map: Dictionary mapping instrument_id to instrument info
        instrument_ids: Optional list of instrument IDs to filter by (only get hosts for these instruments)
        
    Returns:
        Sorted list of unique host strings in format "host:port"
    """
    all_hosts = set()
    if cs_instruments_map:
        # If instrument_ids provided, only check those instruments
        instruments_to_check = instrument_ids if instrument_ids else cs_instruments_map.keys()
        
        for instrument_id in instruments_to_check:
            if instrument_id not in cs_instruments_map:
                continue
            info = cs_instruments_map[instrument_id]
            hosts = info.get("hosts", [])
            for host_obj in hosts:
                if isinstance(host_obj, dict):
                    host_ip = host_obj.get("host") or host_obj.get("ip", "")
                    port = host_obj.get("port", "")
                    if host_ip and port:
                        all_hosts.add(f"{host_ip}:{port}")
                elif isinstance(host_obj, str):
                    all_hosts.add(host_obj)
    return sorted(list(all_hosts))


async def check_hosts_health(all_hosts: List[str], timeout: float = 5.0, script_id: int = None) -> Tuple[List[str], List[str]]:
    """
    Perform health checks on all hosts and return healthy and unhealthy hosts.
    
    Args:
        all_hosts: List of host strings to check
        timeout: Timeout in seconds for each health check
        script_id: Optional script ID for logging context
        
    Returns:
        Tuple of (healthy_hosts, unhealthy_hosts)
    """
    healthy_hosts = []
    unhealthy_hosts = []
    
    log_prefix = f"Script {script_id}: " if script_id else ""
    logging.info(f"{log_prefix}Performing health checks on {len(all_hosts)} hosts...")
    for host in all_hosts:
        is_healthy = await health_check_cs_host(host, timeout, script_id)
        if is_healthy:
            healthy_hosts.append(host)
            logging.info(f"{log_prefix}✓ Host {host} is healthy")
        else:
            unhealthy_hosts.append(host)
            logging.warning(f"{log_prefix}✗ Host {host} failed health check")
    
    return healthy_hosts, unhealthy_hosts


def distribute_instruments_to_hosts(
    instrument_ids: List[int],
    healthy_hosts: List[str],
    script_id: int = None
) -> Dict[str, List[int]]:
    """
    Distribute instruments evenly across healthy hosts.
    
    Args:
        instrument_ids: List of instrument IDs to distribute
        healthy_hosts: List of healthy host strings
        script_id: Optional script ID for logging context
        
    Returns:
        Dictionary mapping host_str to list of instrument_ids assigned to that host
    """
    if not healthy_hosts:
        return {}
    
    if not instrument_ids:
        return {host: [] for host in healthy_hosts}
    
    num_hosts = len(healthy_hosts)
    instruments_per_host = len(instrument_ids) // num_hosts
    remainder = len(instrument_ids) % num_hosts
    
    log_prefix = f"Script {script_id}: " if script_id else ""
    logging.info(f"{log_prefix}Will subscribe to {len(instrument_ids)} instruments across {num_hosts} healthy host(s)")
    logging.info(f"{log_prefix}Distributing ~{instruments_per_host} instruments per host (remainder: {remainder})")
    
    distribution = {}
    start_idx = 0
    
    for i, host in enumerate(healthy_hosts):
        # Calculate how many instruments this host gets
        count = instruments_per_host + (1 if i < remainder else 0)
        end_idx = start_idx + count
        host_instruments = instrument_ids[start_idx:end_idx]
        
        distribution[host] = host_instruments
        logging.info(f"{log_prefix}Host {host} will subscribe to {len(host_instruments)} instruments: {host_instruments[:5]}{'...' if len(host_instruments) > 5 else ''}")
        
        start_idx = end_idx
    
    # Verify distribution: total should equal input
    total_distributed = sum(len(instruments) for instruments in distribution.values())
    if total_distributed != len(instrument_ids):
        logging.error(f"{log_prefix}Distribution error: {total_distributed} instruments distributed but {len(instrument_ids)} expected!")
    else:
        logging.info(f"{log_prefix}Distribution verified: {total_distributed} instruments distributed across {len(healthy_hosts)} hosts")
    
    return distribution


async def get_healthy_hosts_with_distribution(
    cs_instruments_map: Dict,
    instrument_ids: List[int],
    timeout: float = 5.0,
    script_id: int = None
) -> Dict[str, List[int]]:
    """
    Get all available hosts, perform health checks, and distribute instruments.
    This is a convenience function that combines all steps.
    
    Args:
        cs_instruments_map: Dictionary mapping instrument_id to instrument info
        instrument_ids: List of instrument IDs to distribute
        timeout: Timeout in seconds for health checks
        script_id: Optional script ID for logging context
        
    Returns:
        Dictionary mapping healthy host_str to list of instrument_ids assigned to that host
    """
    # Get all available hosts (only for the instruments we're distributing)
    all_hosts = get_all_available_hosts(cs_instruments_map, instrument_ids)
    
    log_prefix = f"Script {script_id}: " if script_id else ""
    if not all_hosts:
        logging.warning(f"{log_prefix}No available hosts found in CS_INSTRUMENTS_MAP")
        return {}
    
    logging.info(f"{log_prefix}Found {len(all_hosts)} available host(s): {all_hosts}")
    
    # Perform health checks
    healthy_hosts, unhealthy_hosts = await check_hosts_health(all_hosts, timeout, script_id)
    
    if not healthy_hosts:
        logging.error(f"{log_prefix}No healthy hosts found!")
        return {}
    
    logging.info(f"{log_prefix}{len(healthy_hosts)}/{len(all_hosts)} hosts are healthy: {healthy_hosts}")
    
    # Distribute instruments
    distribution = distribute_instruments_to_hosts(instrument_ids, healthy_hosts, script_id)
    
    return distribution