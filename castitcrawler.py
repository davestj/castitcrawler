#!/usr/bin/env python3.11
"""
Icecast YP.xml Crawler - Discovery Tool
======================================
Author: David St. James (davestj@gmail.com)
Version: 2.0.0
Created: 2025-05-1
Updated: 2025-05-13

Description:
-----------
This script crawls the Icecast YP.xml directory and discovers all available
streaming stations. It performs basic connectivity tests and checks for the
presence of status pages (status-json.xsl or status.xsl), then inserts valid
stations into a MySQL database for further processing by the sanitizer script.

Features:
---------
- Parallel processing with configurable worker threads
- Connection validation via socket and HTTP tests
- Follows HTTP redirects (301/302)
- Handles SSL/self-signed certificates gracefully
- International TLD support (global coverage)
- Progress tracking with countdown timer
- Comprehensive statistics and reporting

Requirements:
------------
- Python 3.11+
- MySQL database (configured in config.yaml)
- Required packages: requests, mysql-connector-python, pyyaml, beautifulsoup4

Usage:
------
# Dry run (test connectivity):
./castitcrawler.py --dry-run

# Full crawl with status fetching:
./castitcrawler.py --live-run --fetch-status --threads 20

# Background execution with logging:
nohup ./castitcrawler.py --live-run --fetch-status > ./icecast-crawl-$(date +%Y%m%d).log 2>&1 &

Configuration:
-------------
Edit config.yaml to set database credentials and other parameters.

Performance Notes:
-----------------
- Default 20 worker threads (adjust based on system capacity)
- Processes ~15-30 stations per second
- Expected runtime: 10-20 minutes for 15,000 stations

Copyright (c) 2025 David St. James. All rights reserved.
"""

import argparse
import requests
import xml.etree.ElementTree as ET
import mysql.connector
import yaml
import json
from urllib.parse import urlparse, urljoin
from datetime import datetime, timedelta
import os
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import socket
from threading import Lock
import signal
import traceback
import urllib3

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
YP_URL = "https://dir.xiph.org/yp.xml"
CACHE_FILE = "yp.xml"
CACHE_MAX_AGE_MINUTES = 25
BATCH_SIZE = 100
MAX_WORKERS = 20  # Number of concurrent workers
CONNECTION_TIMEOUT = 5  # Timeout for connection tests
SOCKET_TIMEOUT = 3  # Timeout for socket connection test
MAX_REDIRECTS = 3  # Maximum number of redirects to follow

# --- CONFIG LOAD ---
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

DB_CONFIG = config["database"]

# Global variables for statistics and progress tracking
stats_lock = Lock()
transaction_lock = Lock()
transaction_counter = 0
shutdown_event = False

stats = {
    'total': 0,
    'processed': 0,
    'remaining': 0,
    'valid_stations': 0,
    'skipped_no_url': 0,
    'failed_connection': 0,
    'successful_connection': 0,
    'has_status_page': 0,
    'no_status_page': 0,
    'inserted': 0,
    'failed_insert': 0,
    'duplicates': 0,
    'mount_count': 0,
    'redirects_followed': 0,
    'ssl_connections': 0,
    'start_time': None,
    'end_time': None
}


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_event
    log("Shutdown signal received. Finishing current tasks...", "WARN")
    shutdown_event = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_transaction_id():
    """Get a unique transaction ID counting down"""
    global transaction_counter
    with transaction_lock:
        tid = transaction_counter
        transaction_counter -= 1
        return tid


def log(message, level="INFO", transaction_id=None):
    """Centralized logging function with timestamps and transaction ID"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tid_str = f"[TID:{transaction_id}] " if transaction_id is not None else ""
    print(f"[{timestamp}] [{level}] {tid_str}{message}", flush=True)


def update_stats(key, value=1):
    """Thread-safe stats update"""
    with stats_lock:
        stats[key] += value
        if key == 'processed':
            stats['remaining'] -= value


def check_db_connection():
    """Test database connectivity"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.close()
        log("Database connection successful.", "INFO")
        return True
    except Exception as e:
        log(f"Database connection failed: {e}", "ERROR")
        return False


def follow_redirects(url, max_redirects=MAX_REDIRECTS):
    """Follow HTTP redirects and return the final URL"""
    try:
        session = requests.Session()
        session.verify = False  # Allow self-signed certificates

        for i in range(max_redirects):
            response = session.head(url, timeout=CONNECTION_TIMEOUT, allow_redirects=False)

            if response.status_code in [301, 302, 303, 307, 308]:
                new_url = response.headers.get('Location')
                if new_url:
                    # Handle relative redirects
                    new_url = urljoin(url, new_url)
                    log(f"Following redirect: {url} -> {new_url}", "DEBUG")
                    url = new_url
                    update_stats('redirects_followed')
                else:
                    break
            else:
                break

        return url
    except Exception as e:
        log(f"Error following redirects for {url}: {str(e)}", "DEBUG")
        return url


def test_status_page_availability(base_url, tid):
    """Check if the station has a valid status page (status-json.xsl or status.xsl)"""
    status_pages = ['/status-json.xsl', '/status.xsl']

    for page in status_pages:
        try:
            url = base_url + page
            response = requests.get(url, timeout=CONNECTION_TIMEOUT, verify=False)

            if response.status_code == 200:
                # Basic validation that it's actually a status page
                if page == '/status-json.xsl':
                    try:
                        json.loads(response.text)
                        log(f"Found valid JSON status page at {url}", "DEBUG", tid)
                        return True
                    except json.JSONDecodeError:
                        log(f"Invalid JSON at {url}", "DEBUG", tid)
                else:  # status.xsl
                    if '<h3 class="mount">' in response.text or 'server_version' in response.text:
                        log(f"Found valid HTML status page at {url}", "DEBUG", tid)
                        return True

        except Exception as e:
            log(f"Error checking {url}: {str(e)}", "DEBUG", tid)

    return False


def test_station_connection(url, tid):
    """Test if a station is reachable and has valid status pages"""
    try:
        # Follow any redirects first
        final_url = follow_redirects(url)
        parsed = urlparse(final_url)

        if not parsed.scheme or not parsed.hostname:
            log(f"Invalid URL after redirect: {final_url}", "DEBUG", tid)
            return False, None

        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == 'https' else 80)

        if parsed.scheme == 'https':
            update_stats('ssl_connections')

        # First try a socket connection (basic connectivity test)
        log(f"Testing socket connection to {host}:{port}", "DEBUG", tid)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(SOCKET_TIMEOUT)
        try:
            sock.connect((host, port))
            sock.close()
            log(f"Socket connection successful to {host}:{port}", "DEBUG", tid)
        except (socket.timeout, socket.error) as e:
            log(f"Socket connection failed to {host}:{port}: {e}", "DEBUG", tid)
            return False, final_url

        # Check for status pages
        base_url = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            base_url += f":{parsed.port}"

        if test_status_page_availability(base_url, tid):
            log(f"Station has valid status page at {base_url}", "INFO", tid)
            update_stats('has_status_page')
            return True, final_url
        else:
            log(f"No valid status page found at {base_url}", "DEBUG", tid)
            update_stats('no_status_page')
            # Still return True if the server is reachable, let sanitizer handle the rest
            return True, final_url

    except requests.exceptions.Timeout:
        log(f"Connection timeout to {url}", "DEBUG", tid)
        return False, url
    except Exception as e:
        log(f"Connection test failed for {url}: {str(e)}", "DEBUG", tid)
        return False, url


def fetch_yp_xml(force_refresh=False):
    """Download or use cached YP.xml file"""
    if os.path.exists(CACHE_FILE) and not force_refresh:
        age_minutes = (time.time() - os.path.getmtime(CACHE_FILE)) / 60
        if age_minutes < CACHE_MAX_AGE_MINUTES:
            log(f"Using cached yp.xml (age {age_minutes:.1f} minutes)")
            with open(CACHE_FILE, 'rb') as f:
                return f.read()
    try:
        log("Fetching fresh yp.xml from source...")
        response = requests.get(YP_URL, timeout=20)
        response.raise_for_status()
        with open(CACHE_FILE, 'wb') as f:
            f.write(response.content)
        log("Successfully downloaded new yp.xml.")
        return response.content
    except Exception as e:
        log(f"Failed to fetch yp.xml: {e}", "ERROR")
        return None


def parse_yp_entry(entry):
    """Parse a single station entry from YP.xml"""
    return {
        "server_name": entry.findtext("server_name") or "",
        "server_type": entry.findtext("server_type") or "",
        "bitrate_kbps": entry.findtext("bitrate") or "",
        "sample_rate": entry.findtext("samplerate") or "",
        "channels": entry.findtext("channels") or "",
        "listen_url": entry.findtext("listen_url") or "",
        "current_song": entry.findtext("current_song") or "",
        "genre": entry.findtext("genre") or "",  # Keep raw genre
    }


def truncate_tables(cursor):
    """Clear existing data from tables"""
    log("Truncating existing tables...")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("TRUNCATE TABLE mounts")
    cursor.execute("TRUNCATE TABLE stations")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    log("Tables truncated successfully.")


def insert_station(cursor, station_data, tid):
    """Insert a single station into the database"""
    try:
        # Use the final URL if available (after redirects)
        listen_url = station_data.get('final_url', station_data['listen_url'])

        cursor.execute("""
                       INSERT INTO stations (server_name, server_type, bitrate_kbps, sample_rate, channels,
                                             listen_url, current_song, genre_primary, last_updated)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                       """, (
                           station_data['server_name'][:255],  # Ensure field length limits
                           station_data['server_type'][:50],
                           station_data['bitrate_kbps'][:10],
                           station_data['sample_rate'][:10],
                           station_data['channels'][:10],
                           listen_url[:500],
                           station_data['current_song'][:500],
                           station_data['genre'][:255]
                       ))
        station_id = cursor.lastrowid
        log(f"Inserted station ID {station_id}: {station_data['server_name']}", "DEBUG", tid)
        update_stats('inserted')
        return station_id
    except mysql.connector.IntegrityError as e:
        if 'Duplicate entry' in str(e):
            log(f"Duplicate station URL: {listen_url}", "DEBUG", tid)
            update_stats('duplicates')
        else:
            log(f"Database error inserting station: {e}", "ERROR", tid)
            update_stats('failed_insert')
        return None
    except Exception as e:
        log(f"Failed to insert station: {e}", "ERROR", tid)
        update_stats('failed_insert')
        return None


def process_station(entry):
    """Process a single station entry - main worker function"""
    global shutdown_event

    if shutdown_event:
        return None

    tid = get_transaction_id()
    station_data = parse_yp_entry(entry)

    log(f"Processing station: {station_data['server_name']} [Remaining: {stats['remaining']}]", "INFO", tid)

    # Basic validation - only check for URL
    url = station_data.get('listen_url', '').strip()

    if not url:
        log(f"Skipped station with no URL: '{station_data['server_name']}'", "DEBUG", tid)
        update_stats('skipped_no_url')
        update_stats('processed')
        return None

    # Test connection to station
    log(f"Testing connection to {url}", "DEBUG", tid)
    is_reachable, final_url = test_station_connection(url, tid)

    if is_reachable:
        log(f"Connection successful to {url}", "DEBUG", tid)
        update_stats('successful_connection')
        update_stats('valid_stations')
        update_stats('processed')
        station_data['final_url'] = final_url  # Store the final URL after redirects
        return station_data
    else:
        log(f"Connection failed to {url} - skipping station", "WARN", tid)
        update_stats('failed_connection')
        update_stats('processed')
        return None


def fetch_station_status(station_id, base_url, tid):
    """Fetch status-json.xsl for a station"""
    try:
        status_json_url = f"{base_url}/status-json.xsl"
        log(f"Fetching status for station ID {station_id} from {status_json_url}", "DEBUG", tid)
        resp = requests.get(status_json_url, timeout=CONNECTION_TIMEOUT, verify=False)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception as e:
        log(f"Failed to fetch status for station ID {station_id}: {e}", "DEBUG", tid)
        return None


def insert_mounts(cursor, station_id, status_json, tid):
    """Insert mount points for a station"""
    icestats = status_json.get("icestats", {})
    sources = icestats.get("source", [])

    if isinstance(sources, dict):
        sources = [sources]

    if not sources:
        return 0

    mount_count = 0
    for src in sources:
        try:
            mount_point = urlparse(src.get("listenurl", "/")).path
            listeners = src.get("listeners", 0)
            peak = src.get("listener_peak", 0)
            genre = src.get("genre", "")
            server_name = src.get("server_name", "")
            stream_description = src.get("server_description", "")

            cursor.execute("""
                           INSERT INTO mounts (station_id, mount_point, stream_name, stream_description,
                                               bitrate_kbps, listeners_current, listeners_peak, genre,
                                               stream_url, currently_playing, last_updated)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                           """, (
                               station_id, mount_point[:100], server_name[:255], stream_description[:500],
                               str(src.get("bitrate", ""))[:10], str(listeners), str(peak), genre[:255],
                               src.get("listenurl", "")[:500],
                               (src.get("title", "") or src.get("yp_currently_playing", ""))[:500]
                           ))
            mount_count += 1
            log(f"Inserted mount '{mount_point}' for station ID {station_id}", "DEBUG", tid)
        except Exception as e:
            log(f"Failed to insert mount for station ID {station_id}: {e}", "ERROR", tid)

    return mount_count


def print_progress():
    """Print progress statistics with countdown"""
    with stats_lock:
        elapsed = time.time() - stats['start_time']
        rate = stats['processed'] / elapsed if elapsed > 0 else 0
        remaining_time = stats['remaining'] / rate if rate > 0 else 0

        log(f"PROGRESS: {stats['processed']}/{stats['total']} ({stats['processed'] * 100 / stats['total']:.1f}%) | "
            f"Remaining: {stats['remaining']} | Valid: {stats['valid_stations']} | "
            f"Failed Connection: {stats['failed_connection']} | "
            f"Status Pages Found: {stats['has_status_page']} | "
            f"Rate: {rate:.1f}/sec | ETA: {remaining_time / 60:.1f} min", "INFO")


def log_summary():
    """Print final summary statistics"""
    with stats_lock:
        elapsed = time.time() - stats['start_time']

        log("=" * 60, "INFO")
        log("FINAL PROCESSING SUMMARY", "INFO")
        log("=" * 60, "INFO")
        log(f"Processing started: {datetime.fromtimestamp(stats['start_time']).strftime('%Y-%m-%d %H:%M:%S')}", "INFO")
        log(f"Processing ended: {datetime.fromtimestamp(stats['end_time']).strftime('%Y-%m-%d %H:%M:%S')}", "INFO")
        log(f"Total duration: {elapsed:.2f} seconds ({elapsed / 60:.1f} minutes)", "INFO")
        log("", "INFO")
        log("STATION PROCESSING:", "INFO")
        log(f"Total entries in YP.xml: {stats['total']}", "INFO")
        log(f"Total processed: {stats['processed']}", "INFO")
        log(f"Valid stations (reachable): {stats['valid_stations']}", "INFO")
        log("", "INFO")
        log("CONNECTION STATISTICS:", "INFO")
        log(f"Successful connections: {stats['successful_connection']}", "INFO")
        log(f"Failed connections: {stats['failed_connection']}", "INFO")
        log(f"SSL connections: {stats['ssl_connections']}", "INFO")
        log(f"Redirects followed: {stats['redirects_followed']}", "INFO")
        log(f"Stations with status pages: {stats['has_status_page']}", "INFO")
        log(f"Stations without status pages: {stats['no_status_page']}", "INFO")
        log("", "INFO")
        log("DATABASE OPERATIONS:", "INFO")
        log(f"Stations inserted: {stats['inserted']}", "INFO")
        log(f"Failed insertions: {stats['failed_insert']}", "INFO")
        log(f"Duplicate URLs skipped: {stats['duplicates']}", "INFO")
        log(f"Mount points inserted: {stats['mount_count']}", "INFO")
        log("", "INFO")
        log("PERFORMANCE:", "INFO")
        log(f"Average processing rate: {stats['processed'] / elapsed:.2f} stations/second", "INFO")
        log(f"Stations per minute: {stats['processed'] / elapsed * 60:.1f}", "INFO")
        log("=" * 60, "INFO")


def initial_sync(xml_data, fetch_station_statuses=False, max_workers=20):
    """Main synchronization function"""
    global transaction_counter

    log("Starting initial sync")
    stats['start_time'] = time.time()

    # Parse XML data
    root = ET.fromstring(xml_data)
    entries = root.findall("entry")
    stats['total'] = len(entries)
    stats['remaining'] = len(entries)
    transaction_counter = len(entries)

    log(f"Found {stats['total']} entries in YP.xml")

    # Connect to database
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        # Truncate existing tables
        truncate_tables(cursor)
        conn.commit()

        log("=" * 60, "INFO")
        log("STARTING PARALLEL PROCESSING", "INFO")
        log("=" * 60, "INFO")

        # Process stations in parallel
        valid_stations = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            log(f"Processing {len(entries)} stations with {max_workers} workers", "INFO")

            # Submit all entries for processing
            futures = {executor.submit(process_station, entry): entry for entry in entries}

            last_progress_time = time.time()
            progress_interval = 10  # Print progress every 10 seconds

            # Process completed futures
            for future in as_completed(futures):
                if shutdown_event:
                    log("Shutdown requested, cancelling remaining tasks", "WARN")
                    for pending_future in futures:
                        pending_future.cancel()
                    break

                try:
                    result = future.result()
                    if result:
                        # Valid station - insert into database
                        station_id = insert_station(cursor, result, None)
                        if station_id and fetch_station_statuses:
                            # Use the final URL for status fetching
                            final_url = result.get('final_url', result['listen_url'])
                            parsed_url = urlparse(final_url)
                            base_url = f"{parsed_url.scheme}://{parsed_url.hostname}"
                            if parsed_url.port:
                                base_url += f":{parsed_url.port}"

                            status_json = fetch_station_status(station_id, base_url, None)
                            if status_json:
                                mount_count = insert_mounts(cursor, station_id, status_json, None)
                                update_stats('mount_count', mount_count)

                    # Commit periodically
                    if stats['processed'] % 100 == 0:
                        conn.commit()

                    # Print progress periodically
                    current_time = time.time()
                    if current_time - last_progress_time >= progress_interval:
                        print_progress()
                        last_progress_time = current_time

                except Exception as e:
                    log(f"Error processing station: {str(e)}", "ERROR")
                    log(f"Traceback: {traceback.format_exc()}", "DEBUG")

        # Final commit
        conn.commit()

    except Exception as e:
        conn.rollback()
        log(f"Database error: {e}", "ERROR")
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        raise
    finally:
        cursor.close()
        conn.close()

    stats['end_time'] = time.time()

    # Print final summary
    log("", "INFO")
    log("All processing completed. Generating final report...", "INFO")
    log("", "INFO")
    log_summary()

    log("Initial sync completed successfully!", "INFO")
    return stats['valid_stations']


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Icecast YP.xml Crawler - Discovery Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --dry-run                # Test database connection
  %(prog)s --live-run               # Crawl stations without status fetching
  %(prog)s --live-run --fetch-status --threads 30  # Full crawl with 30 threads
        """
    )
    parser.add_argument("--dry-run", action="store_true", help="Test database connection and XML fetching")
    parser.add_argument("--live-run", action="store_true", help="Perform the initial sync")
    parser.add_argument("--fetch-status", action="store_true", help="Fetch status-json.xsl for each station")
    parser.add_argument("--threads", type=int, default=20, help="Number of worker threads (default: 20)")
    parser.add_argument("--version", action="version", version="%(prog)s 2.0.0")
    args = parser.parse_args()

    if not (args.dry_run or args.live_run):
        parser.print_help()
        sys.exit(1)

    if args.dry_run:
        log("Running dry-run test")
        db_ok = check_db_connection()
        xml_data = fetch_yp_xml()
        log(f"Database Connection: {'OK' if db_ok else 'FAIL'}")
        log(f"YP XML Fetch: {'OK' if xml_data else 'FAIL'}")

        if not db_ok or not xml_data:
            sys.exit(1)

    if args.live_run:
        xml_data = fetch_yp_xml(force_refresh=True)
        if xml_data:
            try:
                stations_count = initial_sync(
                    xml_data,
                    fetch_station_statuses=args.fetch_status,
                    max_workers=args.threads
                )
                log(f"Sync completed with {stations_count} valid stations")
                sys.exit(0)
            except Exception as e:
                log(f"Sync failed: {e}", "ERROR")
                log(f"Traceback: {traceback.format_exc()}", "ERROR")
                sys.exit(1)
        else:
            log("Failed to fetch YP.xml, aborting sync", "ERROR")
            sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Keyboard interrupt received", "WARN")
        sys.exit(130)
    except Exception as e:
        log(f"Fatal error: {str(e)}", "ERROR")
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        sys.exit(1)