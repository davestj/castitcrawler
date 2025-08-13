#!/usr/bin/env python3.11

import requests
import yaml
import mysql.connector
import socket
import time
import re
import subprocess
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import sys
from queue import Queue
import signal
import traceback

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load config
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

DB_CONFIG = config['database']

# Threading configuration
MAX_WORKERS = 20  # Adjust based on your system capacity
BATCH_SIZE = 100  # Number of stations to process per batch
REQUEST_TIMEOUT = 5  # HTTP request timeout
DB_POOL_SIZE = 10  # Database connection pool size

# Global variables for graceful shutdown and statistics
shutdown_event = False
stats_lock = Lock()
stats = {
    'total': 0,
    'processed': 0,
    'remaining': 0,
    'successful': 0,
    'failed': 0,
    'deleted': 0,
    'xsl_parsed': 0,
    'json_parsed': 0,
    'errors': 0,
    'mounts_added': 0,
    'mounts_updated': 0,
    'mounts_failed': 0,
    'good_stations': 0,
    'bad_stations': 0,
    'start_time': None,
    'end_time': None
}

# Transaction tracking
transaction_counter = 0
transaction_lock = Lock()


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


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_event
    log("Shutdown signal received. Finishing current tasks...", "WARN")
    shutdown_event = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_db_connection():
    """Get a database connection from the pool"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        log(f"Failed to create database connection: {e}", "ERROR")
        raise


def get_base_url(listen_url):
    """Extract base URL from listen URL"""
    try:
        parsed = urlparse(listen_url)
        if not parsed.scheme or not parsed.hostname:
            return None
        base = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            base += f":{parsed.port}"
        return base
    except Exception as e:
        log(f"Error parsing URL {listen_url}: {e}", "ERROR")
        return None


def check_status_json_url(proto, host, port, station_id, tid):
    """Check which status page is available for the station"""
    paths = ['/status-json.xsl', '/status2.xsl', '/7.xsl', '/status.xsl']
    log(f"Station ID {station_id}: Checking status pages for {proto}://{host}:{port}", "DEBUG", tid)

    for path in paths:
        if shutdown_event:
            return None, None

        url = f"{proto}://{host}:{port}{path}"
        try:
            start_time = time.time()
            response = requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
            elapsed = time.time() - start_time

            if response.status_code == 200:
                log(f"Station ID {station_id}: Found valid status page at {path} (200 OK, {elapsed:.2f}s)", "DEBUG",
                    tid)
                return path, url
            else:
                log(f"Station ID {station_id}: {path} returned {response.status_code} ({elapsed:.2f}s)", "DEBUG", tid)
        except requests.exceptions.Timeout:
            log(f"Station ID {station_id}: Timeout checking {path}", "DEBUG", tid)
        except Exception as e:
            log(f"Station ID {station_id}: Error checking {path}: {str(e)}", "DEBUG", tid)

    log(f"Station ID {station_id}: No valid status pages found at {proto}://{host}:{port}", "WARN", tid)
    return None, None


def parse_status_html(url, station_id, tid):
    """Parse HTML status page and update database"""
    log(f"Station ID {station_id}: Parsing HTML status page at {url}", "DEBUG", tid)

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        mounts = soup.find_all('div', class_='roundbox')

        log(f"Station ID {station_id}: Found {len(mounts)} mount(s) in HTML", "DEBUG", tid)

        mount_count = 0
        for m in mounts:
            if shutdown_event:
                return False

            mount_name = m.find('h3', class_='mount').text.strip().replace('Mount Point', '').strip()
            rows = m.find_all('tr')
            fields = {row.find_all('td')[0].text.strip(':'): row.find_all('td')[1].text.strip()
                      for row in rows if len(row.find_all('td')) == 2}

            log(f"Station ID {station_id}: Processing mount '{mount_name}'", "DEBUG", tid)

            # Update mounts table
            if insert_or_update_mount_xsl(station_id, mount_name, fields, tid):
                mount_count += 1

            # Update extended station fields
            update_station_extended(station_id, fields, mount_name, tid)

        log(f"Station ID {station_id}: Successfully parsed and processed {mount_count} mount(s)", "INFO", tid)
        return True

    except Exception as e:
        log(f"Station ID {station_id}: Failed to parse HTML: {str(e)}", "ERROR", tid)
        log(f"Station ID {station_id}: Traceback: {traceback.format_exc()}", "DEBUG", tid)
        return False


def insert_or_update_mount_xsl(station_id, mount_name, fields, tid):
    """Insert or update mount from XSL parsing"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Check if mount exists
        cursor.execute("SELECT id FROM mounts WHERE station_id = %s AND mount_point = %s",
                       (station_id, mount_name))
        existing = cursor.fetchone()

        if existing:
            # Update existing mount
            cursor.execute("""
                           UPDATE mounts
                           SET stream_name        = %s,
                               stream_description = %s,
                               content_type       = %s,
                               stream_started     = %s,
                               bitrate_kbps       = %s,
                               genre              = %s,
                               stream_url         = %s,
                               currently_playing  = %s,
                               last_updated       = NOW()
                           WHERE station_id = %s
                             AND mount_point = %s
                           """, (
                               fields.get('Stream Name', ''), fields.get('Stream Description', ''),
                               fields.get('Content Type', ''), fields.get('Stream started', None),
                               fields.get('Bitrate', ''), fields.get('Genre', ''),
                               fields.get('Stream URL', ''), fields.get('Currently playing', ''),
                               station_id, mount_name
                           ))
            log(f"Station ID {station_id}: Updated existing mount '{mount_name}'", "DEBUG", tid)
            with stats_lock:
                stats['mounts_updated'] += 1
        else:
            # Insert new mount
            cursor.execute("""
                           INSERT INTO mounts (station_id, mount_point, stream_name, stream_description,
                                               content_type, stream_started, bitrate_kbps, genre,
                                               stream_url, currently_playing, last_updated)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                           """, (
                               station_id, mount_name, fields.get('Stream Name', ''),
                               fields.get('Stream Description', ''), fields.get('Content Type', ''),
                               fields.get('Stream started', None), fields.get('Bitrate', ''),
                               fields.get('Genre', ''), fields.get('Stream URL', ''),
                               fields.get('Currently playing', '')
                           ))
            log(f"Station ID {station_id}: Inserted new mount '{mount_name}'", "DEBUG", tid)
            with stats_lock:
                stats['mounts_added'] += 1

        conn.commit()
        return True

    except mysql.connector.Error as e:
        log(f"Station ID {station_id}: Database error with mount '{mount_name}': {e}", "ERROR", tid)
        with stats_lock:
            stats['mounts_failed'] += 1
        return False
    finally:
        cursor.close()
        conn.close()


def update_station_extended(station_id, fields, mount_name, tid):
    """Update extended station fields"""
    log(f"Station ID {station_id}: Updating extended fields", "DEBUG", tid)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
                       UPDATE stations
                       SET stream_page_url      = %s,
                           mount_point          = %s,
                           stream_description   = %s,
                           content_type         = %s,
                           stream_started       = %s,
                           server_version       = %s,
                           server_build         = %s,
                           stream_type          = %s,
                           source_type          = %s,
                           stream_authenticator = %s
                       WHERE id = %s
                       """, (
                           fields.get('Stream URL', ''), mount_name,
                           fields.get('Stream Description', ''), fields.get('Content Type', ''),
                           fields.get('Stream started', None), '', '', '', 'audio', '',
                           station_id
                       ))
        conn.commit()
        log(f"Station ID {station_id}: Extended fields updated successfully", "DEBUG", tid)
    except mysql.connector.Error as e:
        log(f"Station ID {station_id}: Database error updating extended fields: {e}", "ERROR", tid)
    finally:
        cursor.close()
        conn.close()


def process_json_status(station_id, proto, host, port, path, tid):
    """Process JSON status page"""
    url_full = f"{proto}://{host}:{port}{path}"
    log(f"Station ID {station_id}: Processing JSON status from {url_full}", "INFO", tid)

    try:
        resp = requests.get(url_full, timeout=REQUEST_TIMEOUT, verify=False)
        data = resp.json()
        icestats = data.get('icestats', {})
        sources = icestats.get('source', [])

        if isinstance(sources, dict):
            sources = [sources]

        if not sources:
            log(f"Station ID {station_id}: No valid sources found in JSON response", "WARN", tid)
            return False

        log(f"Station ID {station_id}: Found {len(sources)} source(s) in JSON", "INFO", tid)

        # Process primary source
        primary = sources[0]
        audio_info = primary.get('audio_info', '')
        audio_dict = {}
        for pair in audio_info.split(';'):
            if '=' in pair:
                key, val = pair.split('=', 1)
                audio_dict[key.strip()] = val.strip()

        bitrate = str(primary.get('bitrate') or audio_dict.get('bitrate') or '')
        samplerate = str(primary.get('samplerate') or audio_dict.get('samplerate') or '')
        channels = str(primary.get('channels') or audio_dict.get('channels') or '')

        log(f"Station ID {station_id}: Primary source - Bitrate: {bitrate}, Sample Rate: {samplerate}, Channels: {channels}",
            "DEBUG", tid)

        # Update main station record
        update_main_station(station_id, primary, bitrate, samplerate, channels, tid)

        # Update extended fields
        update_extended_fields(station_id, primary, icestats, tid)

        # Update genres
        update_genres(station_id, primary, tid)

        # Process additional mounts
        if len(sources) > 1:
            log(f"Station ID {station_id}: Processing {len(sources) - 1} additional mount(s)", "INFO", tid)
            process_additional_mounts(station_id, sources[1:], tid)

        # Update listener statistics
        update_listener_stats(station_id, sources, tid)

        log(f"Station ID {station_id}: JSON processing completed successfully", "INFO", tid)
        return True

    except requests.exceptions.Timeout:
        log(f"Station ID {station_id}: Timeout fetching JSON status", "ERROR", tid)
        return False
    except json.JSONDecodeError as e:
        log(f"Station ID {station_id}: Invalid JSON response: {e}", "ERROR", tid)
        return False
    except Exception as e:
        log(f"Station ID {station_id}: Error processing JSON: {str(e)}", "ERROR", tid)
        log(f"Station ID {station_id}: Traceback: {traceback.format_exc()}", "DEBUG", tid)
        return False


def update_main_station(station_id, primary, bitrate, samplerate, channels, tid):
    """Update main station record"""
    log(f"Station ID {station_id}: Updating main station record", "DEBUG", tid)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        server_name = primary.get('server_name', '')
        server_type = primary.get('server_type', '')
        listen_url = primary.get('listenurl', '')
        current_song = primary.get('yp_currently_playing', '') or primary.get('title', '')
        genre = primary.get('genre', '')

        cursor.execute("""
                       UPDATE stations
                       SET server_name=%s,
                           server_type=%s,
                           bitrate_kbps=%s,
                           sample_rate=%s,
                           channels=%s,
                           listen_url=%s,
                           current_song=%s,
                           genre_primary=%s,
                           last_updated=NOW()
                       WHERE id = %s
                       """, (
                           server_name, server_type, bitrate, samplerate, channels,
                           listen_url, current_song, genre, station_id
                       ))
        conn.commit()

        log(f"Station ID {station_id}: Main record updated - Server: {server_name}, Type: {server_type}", "DEBUG", tid)

    except mysql.connector.Error as e:
        log(f"Station ID {station_id}: Database error updating main record: {e}", "ERROR", tid)
    finally:
        cursor.close()
        conn.close()


def update_extended_fields(station_id, primary, icestats, tid):
    """Update extended station fields"""
    log(f"Station ID {station_id}: Updating extended fields", "DEBUG", tid)

    iso_str = primary.get('stream_start_iso8601', '')
    try:
        dt_parsed = datetime.fromisoformat(iso_str.replace('Z', '+00:00')).astimezone().replace(tzinfo=None).strftime(
            "%Y-%m-%d %H:%M:%S")
    except Exception:
        dt_parsed = None

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        admin_email = icestats.get('admin', '')
        server_location = icestats.get('location', '')
        server_host = icestats.get('host', '')

        cursor.execute("""
                       UPDATE stations
                       SET stream_page_url      = %s,
                           mount_point          = %s,
                           stream_description   = %s,
                           content_type         = %s,
                           stream_started       = %s,
                           server_id            = %s,
                           server_build         = %s,
                           stream_type          = %s,
                           source_type          = %s,
                           stream_authenticator = %s,
                           admin_email          = %s,
                           server_location      = %s,
                           server_host          = %s
                       WHERE id = %s
                       """, (
                           primary.get('server_url', ''),
                           urlparse(primary.get('listenurl', '')).path or '',
                           primary.get('server_description', ''),
                           primary.get('server_type', ''), dt_parsed,
                           icestats.get('server_id', ''), str(icestats.get('build', '')),
                           primary.get('server_type', ''), 'audio',
                           primary.get('stream_authenticator', ''),
                           admin_email, server_location, server_host, station_id
                       ))
        conn.commit()

        log(f"Station ID {station_id}: Extended fields updated - Admin: {admin_email}, Location: {server_location}",
            "DEBUG", tid)

    except mysql.connector.Error as e:
        log(f"Station ID {station_id}: Database error updating extended fields: {e}", "ERROR", tid)
    finally:
        cursor.close()
        conn.close()


def update_genres(station_id, primary, tid):
    """Update genre fields"""
    log(f"Station ID {station_id}: Updating genre fields", "DEBUG", tid)

    genres_raw = (primary.get('genre') or '')
    parts = re.split(r'[\/;,\s]+', genres_raw)
    parts = [re.sub(r'[^\w\s&-]', '', p.strip()) for p in parts if p.strip()]

    genre_secondary = parts[1] if len(parts) > 1 else ''
    genre_tertiary = parts[2] if len(parts) > 2 else ''

    log(f"Station ID {station_id}: Genres - Primary: {parts[0] if parts else ''}, Secondary: {genre_secondary}, Tertiary: {genre_tertiary}",
        "DEBUG", tid)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
                       UPDATE stations
                       SET genre_secondary = %s,
                           genre_tertiary  = %s
                       WHERE id = %s
                       """, (genre_secondary, genre_tertiary, station_id))
        conn.commit()
    except mysql.connector.Error as e:
        log(f"Station ID {station_id}: Database error updating genres: {e}", "ERROR", tid)
    finally:
        cursor.close()
        conn.close()


def process_additional_mounts(station_id, mounts, tid):
    """Process additional mount points"""
    log(f"Station ID {station_id}: Processing {len(mounts)} additional mount(s)", "INFO", tid)

    conn = get_db_connection()
    cursor = conn.cursor()

    successful_mounts = 0
    try:
        for i, mount in enumerate(mounts):
            mount_point = urlparse(mount.get('listenurl', '/')).path
            audio_info = mount.get('audio_info', '')
            audio_dict = {}
            for pair in audio_info.split(';'):
                if '=' in pair:
                    key, val = pair.split('=', 1)
                    audio_dict[key.strip()] = val.strip()

            server_name = mount.get('server_name', '')
            listeners = mount.get('listeners', '')
            bitrate = mount.get('bitrate', '')

            log(f"Station ID {station_id}: Mount {i + 1}/{len(mounts)} - Path: {mount_point}, Listeners: {listeners}, Bitrate: {bitrate}",
                "DEBUG", tid)

            # Check if mount exists
            cursor.execute("SELECT id FROM mounts WHERE station_id = %s AND mount_point = %s",
                           (station_id, mount_point))
            existing = cursor.fetchone()

            if existing:
                # Update existing mount
                cursor.execute("""
                               UPDATE mounts
                               SET stream_name        = %s,
                                   stream_description = %s,
                                   bitrate_kbps       = %s,
                                   listeners_current  = %s,
                                   listeners_peak     = %s,
                                   genre              = %s,
                                   stream_url         = %s,
                                   currently_playing  = %s,
                                   audio_channels     = %s,
                                   audio_samplerate   = %s,
                                   audio_bitrate      = %s,
                                   last_updated       = NOW()
                               WHERE station_id = %s
                                 AND mount_point = %s
                               """, (
                                   server_name, mount.get('server_description', ''), str(bitrate),
                                   str(listeners), str(mount.get('listener_peak', '')),
                                   mount.get('genre', ''), mount.get('listenurl', ''),
                                   mount.get('yp_currently_playing', '') or mount.get('title', ''),
                                   audio_dict.get('channels', ''), audio_dict.get('samplerate', ''),
                                   audio_dict.get('bitrate', ''), station_id, mount_point
                               ))
                with stats_lock:
                    stats['mounts_updated'] += 1
                successful_mounts += 1
            else:
                # Insert new mount
                cursor.execute("""
                               INSERT INTO mounts (station_id, mount_point, stream_name, stream_description,
                                                   bitrate_kbps, listeners_current, listeners_peak, genre,
                                                   stream_url, currently_playing, audio_channels,
                                                   audio_samplerate, audio_bitrate, last_updated)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                               """, (
                                   station_id, mount_point, server_name,
                                   mount.get('server_description', ''), str(bitrate),
                                   str(listeners), str(mount.get('listener_peak', '')),
                                   mount.get('genre', ''), mount.get('listenurl', ''),
                                   mount.get('yp_currently_playing', '') or mount.get('title', ''),
                                   audio_dict.get('channels', ''), audio_dict.get('samplerate', ''),
                                   audio_dict.get('bitrate', '')
                               ))
                with stats_lock:
                    stats['mounts_added'] += 1
                successful_mounts += 1

        conn.commit()
        log(f"Station ID {station_id}: Successfully processed {successful_mounts}/{len(mounts)} mount(s)", "INFO", tid)

    except mysql.connector.Error as e:
        if 'Duplicate entry' in str(e):
            log(f"Station ID {station_id}: Mount already exists (race condition), skipping", "DEBUG", tid)
        else:
            log(f"Station ID {station_id}: Error processing mounts: {e}", "ERROR", tid)
        with stats_lock:
            stats['mounts_failed'] += (len(mounts) - successful_mounts)
    finally:
        cursor.close()
        conn.close()


def update_listener_stats(station_id, sources, tid):
    """Update listener statistics"""
    total_listeners = sum(int(src.get('listeners', 0) or 0) for src in sources)
    peak_listeners = max(int(src.get('listener_peak', 0) or 0) for src in sources)

    log(f"Station ID {station_id}: Updating listener stats - Current: {sources[0].get('listeners', '0')}, Peak: {sources[0].get('listener_peak', '0')}, Combined: {total_listeners}",
        "DEBUG", tid)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
                       UPDATE stations
                       SET listeners_current  = %s,
                           listeners_peak     = %s,
                           listeners_combined = %s
                       WHERE id = %s
                       """, (
                           str(sources[0].get('listeners', '0')),
                           str(sources[0].get('listener_peak', '0')),
                           str(total_listeners), station_id
                       ))
        conn.commit()
    except mysql.connector.Error as e:
        log(f"Station ID {station_id}: Database error updating listener stats: {e}", "ERROR", tid)
    finally:
        cursor.close()
        conn.close()


def delete_station(station_id, tid):
    """Delete a station and its mounts"""
    log(f"Station ID {station_id}: Deleting station and all associated mounts", "WARN", tid)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM mounts WHERE station_id = %s", (station_id,))
        mounts_deleted = cursor.rowcount
        cursor.execute("DELETE FROM stations WHERE id = %s", (station_id,))
        conn.commit()

        log(f"Station ID {station_id}: Successfully deleted station and {mounts_deleted} mount(s)", "INFO", tid)
        return True
    except mysql.connector.Error as e:
        log(f"Station ID {station_id}: Database error during deletion: {e}", "ERROR", tid)
        return False
    finally:
        cursor.close()
        conn.close()


def update_stats(status):
    """Update global statistics"""
    with stats_lock:
        stats['processed'] += 1
        stats['remaining'] -= 1

        if status == "DELETED":
            stats['deleted'] += 1
            stats['bad_stations'] += 1
        elif status in ["JSON_OK", "XSL_PARSED"]:
            stats['successful'] += 1
            stats['good_stations'] += 1
            if status == "JSON_OK":
                stats['json_parsed'] += 1
            else:
                stats['xsl_parsed'] += 1
        elif status in ["JSON_FAILED", "XSL_FAILED", "ERROR"]:
            stats['failed'] += 1
            stats['bad_stations'] += 1
            if status == "ERROR":
                stats['errors'] += 1


def print_progress():
    """Print progress statistics with countdown"""
    with stats_lock:
        elapsed = time.time() - stats['start_time']
        rate = stats['processed'] / elapsed if elapsed > 0 else 0
        remaining_time = stats['remaining'] / rate if rate > 0 else 0

        log(f"PROGRESS: {stats['processed']}/{stats['total']} ({stats['processed'] * 100 / stats['total']:.1f}%) | "
            f"Remaining: {stats['remaining']} | "
            f"Success: {stats['successful']} | Failed: {stats['failed']} | Deleted: {stats['deleted']} | "
            f"Rate: {rate:.1f}/sec | ETA: {remaining_time / 60:.1f} min", "INFO")


def process_station(station_data):
    """Process a single station - this is the worker function"""
    global shutdown_event

    if shutdown_event:
        return None

    station_id, listen_url = station_data
    tid = get_transaction_id()

    log(f"Station ID {station_id}: Starting processing [Remaining: {stats['remaining']}] URL: {listen_url}", "INFO",
        tid)

    try:
        parsed = urlparse(listen_url)
        proto = parsed.scheme or ''
        host = parsed.hostname or ''
        port = parsed.port or ('443' if proto == 'https' else '80')

        log(f"Station ID {station_id}: Parsed URL - Protocol: {proto}, Host: {host}, Port: {port}", "DEBUG", tid)

        path, full_url = check_status_json_url(proto, host, port, station_id, tid)

        if path:
            # Station is reachable
            log(f"Station ID {station_id}: Found valid status page at {path}", "INFO", tid)

            if path.endswith(('.xsl')) and not path.startswith('/status-json'):
                # Use HTML parser for XSL pages
                success = parse_status_html(full_url, station_id, tid)
                status = "XSL_PARSED" if success else "XSL_FAILED"
            else:
                # Use JSON parser
                success = process_json_status(station_id, proto, host, port, path, tid)
                status = "JSON_OK" if success else "JSON_FAILED"
        else:
            # Station is unreachable - delete it
            log(f"Station ID {station_id}: No valid status pages found, marking for deletion", "WARN", tid)
            delete_station(station_id, tid)
            status = "DELETED"

        log(f"Station ID {station_id}: Processing completed with status: {status} [Remaining: {stats['remaining'] - 1}]",
            "INFO", tid)
        update_stats(status)

        return (station_id, status, listen_url)

    except Exception as e:
        log(f"Station ID {station_id}: Fatal error during processing: {str(e)}", "ERROR", tid)
        log(f"Station ID {station_id}: Traceback: {traceback.format_exc()}", "DEBUG", tid)
        update_stats("ERROR")
        return (station_id, "ERROR", listen_url)


def log_summary():
    """Log final summary statistics"""
    with stats_lock:
        elapsed = time.time() - stats['start_time']

        log("=" * 60, "INFO")
        log("FINAL PROCESSING SUMMARY", "INFO")
        log("=" * 60, "INFO")
        log(f"Processing started: {datetime.fromtimestamp(stats['start_time']).strftime('%Y-%m-%d %H:%M:%S')}", "INFO")
        log(f"Processing ended: {datetime.fromtimestamp(stats['end_time']).strftime('%Y-%m-%d %H:%M:%S')}", "INFO")
        log(f"Total duration: {elapsed:.2f} seconds ({elapsed / 60:.1f} minutes)", "INFO")
        log(f"", "INFO")
        log(f"STATION STATISTICS:", "INFO")
        log(f"Total stations processed: {stats['processed']}/{stats['total']} ({stats['processed'] * 100 / stats['total']:.1f}%)",
            "INFO")
        log(f"Good stations: {stats['good_stations']} ({stats['good_stations'] * 100 / stats['processed']:.1f}%)",
            "INFO")
        log(f"Bad stations: {stats['bad_stations']} ({stats['bad_stations'] * 100 / stats['processed']:.1f}%)", "INFO")
        log(f"", "INFO")
        log(f"PROCESSING BREAKDOWN:", "INFO")
        log(f"Successful: {stats['successful']} ({stats['successful'] * 100 / stats['processed']:.1f}%)", "INFO")
        log(f"  - JSON parsed: {stats['json_parsed']}", "INFO")
        log(f"  - XSL parsed: {stats['xsl_parsed']}", "INFO")
        log(f"Failed: {stats['failed']} ({stats['failed'] * 100 / stats['processed']:.1f}%)", "INFO")
        log(f"Deleted: {stats['deleted']} ({stats['deleted'] * 100 / stats['processed']:.1f}%)", "INFO")
        log(f"Errors: {stats['errors']}", "INFO")
        log(f"", "INFO")
        log(f"MOUNT STATISTICS:", "INFO")
        log(f"Mounts added (new): {stats['mounts_added']}", "INFO")
        log(f"Mounts updated (existing): {stats['mounts_updated']}", "INFO")
        log(f"Mounts failed: {stats['mounts_failed']}", "INFO")
        log(f"Total mount operations: {stats['mounts_added'] + stats['mounts_updated']}", "INFO")
        log(f"", "INFO")
        log(f"PERFORMANCE:", "INFO")
        log(f"Average rate: {stats['processed'] / elapsed:.2f} stations/second", "INFO")
        log(f"Stations per minute: {stats['processed'] / elapsed * 60:.1f}", "INFO")
        log("=" * 60, "INFO")


def count_existing_mounts():
    """Count existing mounts in database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM mounts")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count


def fetch_station_urls():
    """Main function to fetch and process all station URLs"""
    global transaction_counter

    log("Starting Icecast station sanitizer", "INFO")
    log(f"Configuration: MAX_WORKERS={MAX_WORKERS}, REQUEST_TIMEOUT={REQUEST_TIMEOUT}s", "INFO")

    # Initialize statistics
    stats['start_time'] = time.time()

    # Count existing mounts before processing
    initial_mount_count = count_existing_mounts()
    log(f"Initial mount count in database: {initial_mount_count}", "INFO")

    # Get all stations from database
    log("Fetching station list from database", "INFO")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, listen_url FROM stations")
    stations = cursor.fetchall()
    cursor.close()
    conn.close()

    stats['total'] = len(stations)
    stats['remaining'] = len(stations)
    transaction_counter = len(stations)  # Initialize countdown counter

    log(f"Found {stats['total']} stations to process", "INFO")

    # Log first few stations as examples
    log("Sample stations to be processed:", "DEBUG")
    for i, (sid, url) in enumerate(stations[:5]):
        log(f"  Station {sid}: {url}", "DEBUG")
    if len(stations) > 5:
        log(f"  ... and {len(stations) - 5} more", "DEBUG")

    log("=" * 60, "INFO")
    log("STARTING PARALLEL PROCESSING", "INFO")
    log("=" * 60, "INFO")

    # Process stations in parallel using ThreadPoolExecutor
    completed_futures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks to the executor
        log(f"Submitting {len(stations)} tasks to thread pool with {MAX_WORKERS} workers", "INFO")
        futures = {executor.submit(process_station, station): station[0]
                   for station in stations}

        log(f"All tasks submitted, processing begins now", "INFO")

        last_progress_time = time.time()
        progress_interval = 30  # Print progress every 30 seconds

        # Process completed tasks as they finish
        for future in as_completed(futures):
            if shutdown_event:
                log("Shutdown requested, cancelling remaining tasks", "WARN")
                for pending_future in futures:
                    pending_future.cancel()
                break

            try:
                result = future.result()
                if result:
                    completed_futures.append(result)
                    station_id, status, url = result

                    # Print progress every N seconds
                    current_time = time.time()
                    if current_time - last_progress_time >= progress_interval:
                        print_progress()
                        last_progress_time = current_time

            except Exception as e:
                station_id = futures[future]
                log(f"Station ID {station_id}: Unhandled exception: {str(e)}", "ERROR")
                log(f"Station ID {station_id}: Traceback: {traceback.format_exc()}", "DEBUG")
                update_stats("ERROR")

    # Mark end time
    stats['end_time'] = time.time()

    # Count final mounts
    final_mount_count = count_existing_mounts()
    net_mount_change = final_mount_count - initial_mount_count

    log("", "INFO")
    log("All processing completed. Generating final report...", "INFO")
    log("", "INFO")

    # Print final summary
    log_summary()

    # Additional mount statistics
    log("", "INFO")
    log("MOUNT DATABASE CHANGES:", "INFO")
    log(f"Initial mount count: {initial_mount_count}", "INFO")
    log(f"Final mount count: {final_mount_count}", "INFO")
    log(f"Net change: {'+' if net_mount_change >= 0 else ''}{net_mount_change}", "INFO")
    log("", "INFO")

    # Exit message
    log("Icecast station sanitizer completed successfully", "INFO")
    log("Exiting...", "INFO")


if __name__ == "__main__":
    try:
        fetch_station_urls()
        log("Script execution completed", "INFO")
        sys.exit(0)
    except KeyboardInterrupt:
        log("Keyboard interrupt received", "WARN")
        sys.exit(130)
    except Exception as e:
        log(f"Fatal error: {str(e)}", "ERROR")
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        sys.exit(1)