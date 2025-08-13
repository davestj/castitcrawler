#!/usr/bin/env python3.11

import requests
import yaml
import mysql.connector
import socket
import time
import re
import subprocess
import json
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# Load config
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

DB_CONFIG = config['database']

# Helpers

def normalize_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.hostname}:{parsed.port if parsed.port else 80}"

def fetch_status_json(base_url):
    try:
        r = requests.get(f"{base_url}/status-json.xsl", timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        return None
    return None

def fetch_status_html(base_url):
    try:
        r = requests.get(f"{base_url}/status.xsl", timeout=10)
        if r.status_code == 200:
            return r.text
    except Exception:
        return None
    return None

def guess_encoding(content_type):
    if 'ogg' in content_type:
        return 'ogg'
    if 'aac' in content_type or 'aacp' in content_type:
        return 'aac'
    return 'mp3'

def check_non_english(text):
    if text is None:
        return False
    return not bool(re.match(r'^[\x00-\x7F]*$', text))

def check_generic_name(name):
    generic_terms = ["default", "stream", "radio", "audio", "live"]
    if not name:
        return True
    lower_name = name.lower()
    return any(term in lower_name for term in generic_terms)

def parse_audio_info(audio_info):
    info = {}
    if audio_info:
        parts = audio_info.split(';')
        for part in parts:
            k, v = part.split('=')
            info[k.strip()] = v.strip()
    return info

def parse_iso8601_to_datetime(date_str):
    try:
        return datetime.fromisoformat(date_str)
    except Exception:
        return None

def test_stream(listen_url):
    try:
        start = time.time()
        headers = {
            'Icy-MetaData': '1',
            'User-Agent': 'WinampMPEG/5.09'
        }
        r = requests.get(listen_url, headers=headers, stream=True, timeout=10)
        if r.status_code != 200:
            return None
        content_type = r.headers.get('Content-Type', '')
        encoding = guess_encoding(content_type)
        icy_name = r.headers.get('icy-name')
        icy_genre = r.headers.get('icy-genre')
        icy_url = r.headers.get('icy-url')
        icy_br = r.headers.get('icy-br')
        icy_description = r.headers.get('icy-description')
        playing = None

        # Stream read
        metaint = int(r.headers.get('icy-metaint', 0))
        bytes_read = 0
        buffer = b''
        for chunk in r.iter_content(chunk_size=512):
            buffer += chunk
            bytes_read += len(chunk)
            if metaint and bytes_read > metaint:
                metadata_length = buffer[metaint] * 16
                metadata = buffer[metaint+1:metaint+1+metadata_length]
                metadata_str = metadata.decode(errors='ignore')
                m = re.search(r"StreamTitle='(.*?)';", metadata_str)
                if m:
                    playing = m.group(1)
                break
            if time.time() - start > 15:
                break

        latency = int((time.time() - start) * 1000)
        return {
            'icy_name': icy_name,
            'icy_genre': icy_genre,
            'icy_url': icy_url,
            'icy_br': icy_br,
            'icy_description': icy_description,
            'currently_playing': playing,
            'content_type': content_type,
            'connection_latency_ms': latency,
        }
    except Exception as e:
        print(f"[!] Stream test error: {e}")
        return None

# Main Verification

def verify_all():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor_insert = conn.cursor()

    cursor.execute("SELECT * FROM stations")
    stations = cursor.fetchall()

    for station in stations:
        station_id = station['id']
        listen_url = station['listen_url']
        base_url = normalize_url(listen_url)

        print(f"[*] Verifying: {station['server_name']} at {listen_url}")

        json_data = fetch_status_json(base_url)
        html_data = None
        if not json_data:
            html_data = fetch_status_html(base_url)

        verification = {
            'station_id': station_id,
            'server_name': station['server_name'],
            'listen_url': listen_url,
            'mount_point': station.get('mount_point', '/'),
            'is_reachable': 1,
            'is_streaming': 1,
            'connection_latency_ms': None,
            'stream_bitrate': None,
            'content_type': None,
            'genre': None,
            'listeners_current': None,
            'listeners_peak': None,
            'stream_url': None,
            'currently_playing': None,
            'icy_name': None,
            'icy_genre': None,
            'icy_url': None,
            'icy_br': None,
            'icy_description': None,
            'icy_meta': None,
            'stream_started': None,
            'verified_at': datetime.utcnow(),
            'unusable_reason': None,
            'verification_duration_seconds': 15,
            'verification_rating': 'fair',
            'performance_rating': 50,
        }

        if json_data:
            source = json_data['icestats'].get('source')
            if isinstance(source, list):
                source = source[0]
            if source:
                verification.update({
                    'stream_bitrate': source.get('bitrate'),
                    'listeners_current': source.get('listeners'),
                    'listeners_peak': source.get('listener_peak'),
                    'content_type': source.get('server_type'),
                    'genre': source.get('genre'),
                    'stream_url': source.get('server_url'),
                    'currently_playing': source.get('title'),
                    'stream_started': parse_iso8601_to_datetime(source.get('stream_start_iso8601')),
                })

        stream_result = test_stream(listen_url)
        if stream_result:
            verification.update(stream_result)

        # Flagging
        if check_generic_name(verification.get('server_name')):
            verification['flag_generic'] = 1
        if check_non_english(verification.get('server_name')):
            verification['flag_non_english'] = 1

        # Insert or update verification
        fields = ','.join(verification.keys())
        placeholders = ','.join(['%s'] * len(verification))
        sql = f"INSERT INTO stream_verification ({fields}) VALUES ({placeholders})"
        cursor_insert.execute(sql, list(verification.values()))
        conn.commit()

        print(f"[✓] Verified: {station['server_name']} at {listen_url}")

    cursor.close()
    cursor_insert.close()
    conn.close()

if __name__ == "__main__":
    verify_all()
