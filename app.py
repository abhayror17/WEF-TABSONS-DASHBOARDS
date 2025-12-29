from typing import Any
from flask import Flask, render_template, request, send_from_directory, jsonify, url_for
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
from difflib import SequenceMatcher
import traceback
from waitress import serve
from cachetools import TTLCache
import threading
import time
import logging
from collections import OrderedDict
import sqlite3
import os
import schedule

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

# --- App Configuration ---
app = Flask(__name__)

# --- Configuration for QC Summary App ---
QC_CONFIG = {
    "BASE_URL": "http://10.18.81.23/tabsons",
    "USERNAME": "abhay.tabsons@gmail.com",
    "PASSWORD": "?&hM6kh5",
}
QC_CONFIG["LOGIN_PAGE_URL"] = f"{QC_CONFIG['BASE_URL']}/login"
QC_CONFIG["PROCESS_LOGIN_URL"] = f"{QC_CONFIG['BASE_URL']}/process-login"
QC_CONFIG["QC_NEWS_PAGE_URL"] = f"{QC_CONFIG['BASE_URL']}/qcnews"
QC_CONFIG["CHANNEL_DATA_API_URL"] = f"{QC_CONFIG['BASE_URL']}/qcnews/getdatagrid"
QC_CONFIG["STORY_DATA_API_URL"] = f"{QC_CONFIG['BASE_URL']}/qcnews/getstorydatagrid"

# --- Configuration for Logger Tagging App ---
LOGGER_CONFIG = {
    "API_URL": "http://10.18.80.14:2996/command/ExportEPGTabsons",
    "ALL_CHANNEL_IDS": [
        "1010013", "1010022", "1010485", "1010094", "1010170", "1010272", "1010268", "1010977", "1010702", "1015268",
        "1010178", "1010430", "1010261", "1010160", "1010119", "1010432", "1010071", "1010088", "1010768", "1010259",
        "1010034", "1010231", "1010523", "1010912", "1010318", "1010624", "1010443", "1010456", "1010280", "1010439",
        "1010337", "1010297", "1010345", "1010412", "1010305", "1015434", "1010709", "1010420", "1010131", "1010441",
        "1010304", "1010815", "1010509", "1010281", "1010196", "1010576", "1010676", "1015341", "1010665", "1010271",
        "1010298", "1010115", "1010707", "1015370", "1010020", "1015368", "1010129", "1010004", "1010302", "1010649",
        "1010125", "1015487", "1010021", "1010326", "1010442", "1010171", "1010468", "1010130", "1010654", "1010440",
        "1010341", "1010293", "1010137", "1010058", "1010504", "1010005", "1010169", "1010127", "1010482", "1010647",
        "1010041", "1010185", "1010797", "1010133", "1010484","1015471", "1015427", "1010123", "1010488", "1010902"
    ]
}

# --- Logger Dashboard Configuration ---
XEN_API_URL = "http://10.18.80.14:2996/command/ExportEPGTabsons"
EQ_API_URL = "http://10.18.50.26:5000/api/tabsons/getreport/{channel_id}/{date}"
MAX_EQ_CONCURRENT_REQUESTS = 2
EQ_REQUEST_DELAY_SECONDS = 0.25
cache = TTLCache(maxsize=128, ttl=900)

# --- CHANNEL DEFINITIONS ---
CLUSTERS = {
    "All National": ["1010013", "1010022", "1010485", "1010094", "1010170", "1010272", "1010268", "1010977", "1010702", "1015268", "1010178", "1010430", "1010261", "1010160", "1010119", "1010432", "1010071", "1010088", "1010768", "1010259", "1010269", "1010172", "1010276", "1010670", "1010458", "1011043"],
    "All South": ["1010034", "1015471","1010231", "1010523", "1010912", "1010318", "1010624", "1010443", "1010456", "1010280", "1010439", "1010337", "1010297", "1010345", "1010412", "1010305", "1015434", "1010709", "1010420", "1010131", "1010441", "1010304", "1010815", "1010509", "1010615"],
    "All East": ["1010281", "1010196", "1010576", "1010676", "1015341", "1010665", "1010271", "1010298", "1010115", "1010707", "1015370", "1010020", "1015368", "1010129", "1010004", "1010205"],
    "All West": ["1010302", "1010649", "1010125", "1015487", "1010021", "1010326", "1010442", "1010171", "1010468", "1010130", "1010654", "1010440", "1010341", "1010293", "1010067", "1010487"],
    "All Hindi Regional": ["1010137", "1010058", "1010504", "1010005", "1010169", "1010127", "1010482", "1010647", "1010041", "1010185", "1010797", "1010133", "1010484", "1015427", "1010123", "1010488", "1010902", "1010244", "1010173", "1010958", "1010332"],
}
EQ_CHANNELS = ["1010071", "1010160", "1010259","1015471", "1010272", "1010268", "1010170", "1010430", "1010485", "1010432", "1010280", "1010013", "1010649"]
all_channels_list = []
for cluster_channels in CLUSTERS.values(): all_channels_list.extend(cluster_channels)
all_channels_list.extend(EQ_CHANNELS)
CLUSTERS["All Channels"] = sorted(list(set(all_channels_list)))

# --- Dashboard Logic Constants ---
TAGGING_COMPLETION_THRESHOLD = "23:25:00"
QC_COMPLETION_THRESHOLD = "23:00:00"
FUZZY_MATCH_THRESHOLD = 0.8
DEFAULT_PULL_START_TIME = "06:00:00"
DEFAULT_PULL_END_TIME = "23:59:59"

# --- Channel Name Aliases ---
CHANNEL_NAME_ALIASES = {
    'news state bhjk': 'news state bihar jharkhand', 'zee up uk': 'zee uttar pradesh uttarakhand',
    'zee rajasthan': 'zee rajasthan news', 'news state mp cg': 'news state madhya pradesh-chhattisgarh',
    'news18 pnb hr': 'news18 punjab haryana', 'reporter': 'reporter tv new', 'portidin time': 'protidin time',
    'public tv': 'public tv new', 'tv9 telugu': 'tv9 telugu new', 'tv 5 news': 'tv 5 news new',
    'news18 kannada': 'news18 kannada new', 'dd news': 'dd news new', 'saam tv': 'saam tv new'
}

# --- Global Session for QC App ---
authenticated_session = requests.Session()

# --- SQLite Database Configuration ---
DB_PATH = "it_dashboard_cache.db"
CACHE_REFRESH_MINUTES = 20  # Auto-refresh cache every 20 minutes

def init_database():
    """Initialize the SQLite database for caching IT dashboard and logger dashboard data"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create table for logger data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logger_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            original_name TEXT NOT NULL,
            logger_end_time TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, normalized_name)
        )
    ''')
    
    # Create table for QC data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qc_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            last_qc_end_time TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, normalized_name)
        )
    ''')
    
    # Create table for processed dashboard data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dashboard_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            logger_end_time TEXT NOT NULL,
            qc_end_time TEXT NOT NULL,
            status_class TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, channel_name)
        )
    ''')
    
    # Create table for logger dashboard cluster data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logger_cluster_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            cluster_name TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            logger_type TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, cluster_name, channel_id, logger_type)
        )
    ''')
    
    # Create table for logger dashboard low duration channels
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logger_low_duration_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            cluster_name TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, cluster_name, channel_id)
        )
    ''')
    
    # Create table for logger dashboard cluster progress
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logger_cluster_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            cluster_name TEXT NOT NULL,
            total_channels INTEGER NOT NULL,
            qced_channels INTEGER NOT NULL,
            percentage REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, cluster_name)
        )
    ''')
    
    conn.commit()
    conn.close()
    logging.info("Database initialized successfully")

def get_db_connection():
    """Get a database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def is_data_cached(date):
    """Check if data is cached for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if we have dashboard data for this date
    cursor.execute("SELECT COUNT(*) as count FROM dashboard_data WHERE date = ?", (date,))
    count = cursor.fetchone()['count']
    
    conn.close()
    return count > 0

def is_cache_fresh(date):
    """Check if cached data is fresh (within CACHE_REFRESH_MINUTES)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get the creation time of the most recent record for this date
    cursor.execute("""
        SELECT created_at FROM dashboard_data
        WHERE date = ?
        ORDER BY created_at DESC
        LIMIT 1
    """, (date,))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        return False
    
    # Parse the timestamp and check if it's within the refresh window
    created_at = datetime.strptime(result['created_at'], '%Y-%m-%d %H:%M:%S')
    time_diff = datetime.now() - created_at
    
    return time_diff.total_seconds() < (CACHE_REFRESH_MINUTES * 60)

def should_refresh_cache(date):
    """Determine if cache should be refreshed for a given date"""
    if not is_data_cached(date):
        return False  # No data exists, need to fetch fresh
    
    return not is_cache_fresh(date)  # Data exists but is stale

def cache_logger_data(date, logger_data):
    """Cache logger data for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Clear existing data for this date
    cursor.execute("DELETE FROM logger_data WHERE date = ?", (date,))
    
    # Insert new data
    for norm_name, data in logger_data.items():
        cursor.execute('''
            INSERT INTO logger_data (date, normalized_name, original_name, logger_end_time)
            VALUES (?, ?, ?, ?)
        ''', (date, norm_name, data['original_name'], data['logger_end_time']))
    
    conn.commit()
    conn.close()
    logging.info(f"Cached logger data for {date}")

def cache_qc_data(date, qc_data):
    """Cache QC data for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Clear existing data for this date
    cursor.execute("DELETE FROM qc_data WHERE date = ?", (date,))
    
    # Insert new data
    for norm_name, data in qc_data.items():
        cursor.execute('''
            INSERT INTO qc_data (date, normalized_name, last_qc_end_time)
            VALUES (?, ?, ?)
        ''', (date, norm_name, data['last_qc_end_time']))
    
    conn.commit()
    conn.close()
    logging.info(f"Cached QC data for {date}")

def cache_dashboard_data(date, dashboard_data):
    """Cache processed dashboard data for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Clear existing data for this date
    cursor.execute("DELETE FROM dashboard_data WHERE date = ?", (date,))
    
    # Insert new data
    for item in dashboard_data:
        cursor.execute('''
            INSERT INTO dashboard_data (date, channel_name, logger_end_time, qc_end_time, status_class)
            VALUES (?, ?, ?, ?, ?)
        ''', (date, item['channel_name'], item['logger_end_time'], item['qc_end_time'], item['status_class']))
    
    conn.commit()
    conn.close()
    logging.info(f"Cached dashboard data for {date}")

def get_cached_dashboard_data(date):
    """Retrieve cached dashboard data for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT channel_name, logger_end_time, qc_end_time, status_class
        FROM dashboard_data
        WHERE date = ?
        ORDER BY channel_name
    ''', (date,))
    
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]

def get_cached_logger_data(date):
    """Retrieve cached logger data for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT normalized_name, original_name, logger_end_time
        FROM logger_data
        WHERE date = ?
    ''', (date,))
    
    rows = cursor.fetchall()
    conn.close()
    
    return {row['normalized_name']: {
        'original_name': row['original_name'],
        'logger_end_time': row['logger_end_time']
    } for row in rows}

def get_cached_qc_data(date):
    """Retrieve cached QC data for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT normalized_name, last_qc_end_time
        FROM qc_data
        WHERE date = ?
    ''', (date,))
    
    rows = cursor.fetchall()
    conn.close()
    
    return {row['normalized_name']: {
        'last_qc_end_time': row['last_qc_end_time']
    } for row in rows}

def cache_logger_cluster_data(date, cluster_name, cluster_data, low_durn_channels):
    """Cache logger dashboard cluster data for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Clear existing data for this date and cluster
    cursor.execute("DELETE FROM logger_cluster_data WHERE date = ? AND cluster_name = ?", (date, cluster_name))
    cursor.execute("DELETE FROM logger_low_duration_channels WHERE date = ? AND cluster_name = ?", (date, cluster_name))
    
    # Insert cluster data
    for channel_id, data in cluster_data.items():
        for log in data['logs']:
            cursor.execute('''
                INSERT INTO logger_cluster_data (date, cluster_name, channel_id, channel_name, logger_type, start_time, end_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (date, cluster_name, channel_id, data['name'], log['logger'], log['start'], log['end']))
    
    # Insert low duration channels
    for channel_id in low_durn_channels:
        cursor.execute('''
            INSERT INTO logger_low_duration_channels (date, cluster_name, channel_id)
            VALUES (?, ?, ?)
        ''', (date, cluster_name, channel_id))
    
    conn.commit()
    conn.close()
    logging.info(f"Cached logger cluster data for {cluster_name} on {date}")

def cache_logger_cluster_progress(date, cluster_progress):
    """Cache logger dashboard cluster progress for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Clear existing data for this date
    cursor.execute("DELETE FROM logger_cluster_progress WHERE date = ?", (date,))
    
    # Insert progress data
    for cluster_name, progress in cluster_progress.items():
        cursor.execute('''
            INSERT INTO logger_cluster_progress (date, cluster_name, total_channels, qced_channels, percentage)
            VALUES (?, ?, ?, ?, ?)
        ''', (date, cluster_name, progress['total'], progress['qced'], progress['percentage']))
    
    conn.commit()
    conn.close()
    logging.info(f"Cached logger cluster progress for {date}")

def get_cached_logger_cluster_data(date, cluster_name):
    """Retrieve cached logger dashboard cluster data for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get cluster data
    cursor.execute('''
        SELECT channel_id, channel_name, logger_type, start_time, end_time
        FROM logger_cluster_data
        WHERE date = ? AND cluster_name = ?
        ORDER BY channel_id, logger_type
    ''', (date, cluster_name))
    
    rows = cursor.fetchall()
    
    # Get low duration channels
    cursor.execute('''
        SELECT channel_id
        FROM logger_low_duration_channels
        WHERE date = ? AND cluster_name = ?
    ''', (date, cluster_name))
    
    low_durn_rows = cursor.fetchall()
    conn.close()
    
    # Reconstruct the data structure
    cluster_data = defaultdict(lambda: {"name": "", "logs": []})
    for row in rows:
        channel_id = row['channel_id']
        cluster_data[channel_id]['name'] = row['channel_name']
        cluster_data[channel_id]['logs'].append({
            "logger": row['logger_type'],
            "start": row['start_time'],
            "end": row['end_time']
        })
    
    low_durn_channels = {row['channel_id'] for row in low_durn_rows}
    
    return dict(cluster_data), low_durn_channels

def get_cached_logger_cluster_progress(date):
    """Retrieve cached logger dashboard cluster progress for a specific date"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT cluster_name, total_channels, qced_channels, percentage
        FROM logger_cluster_progress
        WHERE date = ?
    ''', (date,))
    
    rows = cursor.fetchall()
    conn.close()
    
    return {row['cluster_name']: {
        'total': row['total_channels'],
        'qced': row['qced_channels'],
        'percentage': row['percentage']
    } for row in rows}

def is_logger_data_cached(date, cluster_name):
    """Check if logger dashboard data is cached for a specific date and cluster"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT COUNT(*) as count FROM logger_cluster_data
        WHERE date = ? AND cluster_name = ?
    ''', (date, cluster_name))
    
    count = cursor.fetchone()['count']
    conn.close()
    
    return count > 0

def is_logger_cache_fresh(date, cluster_name):
    """Check if cached logger dashboard data is fresh (within CACHE_REFRESH_MINUTES)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT created_at FROM logger_cluster_data
        WHERE date = ? AND cluster_name = ?
        ORDER BY created_at DESC
        LIMIT 1
    ''', (date, cluster_name))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        return False
    
    created_at = datetime.strptime(result['created_at'], '%Y-%m-%d %H:%M:%S')
    time_diff = datetime.now() - created_at
    
    return time_diff.total_seconds() < (CACHE_REFRESH_MINUTES * 60)

def should_refresh_logger_cache(date, cluster_name):
    """Determine if logger dashboard cache should be refreshed for a given date and cluster"""
    if not is_logger_data_cached(date, cluster_name):
        return False  # No data exists, need to fetch fresh
    
    return not is_logger_cache_fresh(date, cluster_name)  # Data exists but is stale

def cleanup_old_data():
    """Clean up data older than the current week (Sunday to Sunday)"""
    today = datetime.now().date()
    current_weekday = today.weekday()  # Monday is 0, Sunday is 6
    
    # Calculate the most recent Sunday
    if current_weekday == 6:  # Today is Sunday
        last_sunday = today
    else:
        last_sunday = today - timedelta(days=current_weekday + 1)
    
    # Calculate the Sunday before that
    previous_sunday = last_sunday - timedelta(days=7)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Delete data older than the previous Sunday
    cursor.execute("DELETE FROM logger_data WHERE date < ?", (previous_sunday.isoformat(),))
    cursor.execute("DELETE FROM qc_data WHERE date < ?", (previous_sunday.isoformat(),))
    cursor.execute("DELETE FROM dashboard_data WHERE date < ?", (previous_sunday.isoformat(),))
    cursor.execute("DELETE FROM logger_cluster_data WHERE date < ?", (previous_sunday.isoformat(),))
    cursor.execute("DELETE FROM logger_low_duration_channels WHERE date < ?", (previous_sunday.isoformat(),))
    cursor.execute("DELETE FROM logger_cluster_progress WHERE date < ?", (previous_sunday.isoformat(),))
    
    deleted_rows = conn.total_changes
    conn.commit()
    conn.close()
    
    logging.info(f"Cleaned up {deleted_rows} old records from database")

def auto_refresh_cache():
    """Automatically refresh cache for today's date"""
    today = date.today().isoformat()
    
    logging.info(f"Auto-refreshing cache for {today}")
    
    try:
        # Refresh IT Dashboard data
        if is_data_cached(today) or should_refresh_cache(today):
            logging.info(f"Auto-refreshing IT dashboard data for {today}")
            
            # Fetch fresh data
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_logger_data = executor.submit(get_all_logger_data, today)
                future_qc_data = executor.submit(get_all_qc_data_with_times, today)
                logger_data = future_logger_data.result()
                qc_data, qc_error = future_qc_data.result()

            if qc_error:
                logging.error(f"IT Dashboard auto-refresh failed: {qc_error}")
            elif qc_data is None:
                logging.error("IT Dashboard auto-refresh failed: Failed to fetch QC data")
            else:
                # Cache the fresh data
                cache_logger_data(today, logger_data)
                cache_qc_data(today, qc_data)

                # Process and cache dashboard data
                matched_qc_data = {
                    logger_norm_name: qc_data[max(qc_data.keys(), key=lambda qc_name: similarity(logger_norm_name, qc_name))]
                    for logger_norm_name in logger_data
                    if max(qc_data.keys(), key=lambda qc_name: similarity(logger_norm_name, qc_name)) and similarity(logger_norm_name, max(qc_data.keys(), key=lambda qc_name: similarity(logger_norm_name, qc_name))) >= FUZZY_MATCH_THRESHOLD
                }
                
                dashboard_data = []
                for norm_name in sorted(logger_data.keys()):
                    log_info = logger_data[norm_name]
                    qc_info = matched_qc_data.get(norm_name)
                    
                    logger_time = log_info.get('logger_end_time')
                    if logger_time == "00:00:00":
                        logger_time = "23:59:59"
                    qc_time = qc_info.get('last_qc_end_time') if qc_info else None

                    if qc_time and qc_time.startswith("00:00"):
                        qc_time = "23:59:59"

                    is_tagging_complete = logger_time and logger_time >= TAGGING_COMPLETION_THRESHOLD
                    is_qc_done = qc_time and qc_time >= "23:30:00"

                    details = {
                        "channel_name": log_info['original_name'],
                        "logger_end_time": logger_time or "N/A",
                        "qc_end_time": qc_time or "Not in QC"
                    }

                    status_for_sort = ""
                    if is_qc_done:
                        status_for_sort = "QC DONE"
                        details["status_class"] = "status-completed"
                    elif is_tagging_complete and (qc_time and qc_time >= QC_COMPLETION_THRESHOLD):
                        status_for_sort = "QC DONE"
                        details["status_class"] = "status-completed"
                    elif not is_tagging_complete:
                        status_for_sort = "Tagging in Progress"
                        details["status_class"] = "status-progress"
                    else:
                        details["status_class"] = "status-eligible"
                        status_for_sort = "Eligible to Pull (Catch-up)" if qc_info and qc_time else "Eligible to Pull (Default)"
                    
                    details["status_for_sort"] = status_for_sort
                    dashboard_data.append(details)

                sort_order = {"Eligible to Pull (Default)": 0, "Eligible to Pull (Catch-up)": 1, "Tagging in Progress": 2, "QC DONE": 3}
                dashboard_data.sort(key=lambda item: sort_order.get(item['status_for_sort'], 99))
                
                # Remove the temporary sort key before caching
                for item in dashboard_data:
                    del item['status_for_sort']
                
                cache_dashboard_data(today, dashboard_data)
                logging.info(f"IT Dashboard auto-refresh completed: {len(dashboard_data)} records updated")
        
        # Refresh Logger Dashboard data
        auto_refresh_logger_data(today)
        
    except Exception as e:
        logging.error(f"Auto-refresh error: {str(e)}")
        import traceback
        traceback.print_exc()

def auto_refresh_logger_data(selected_date):
    """Automatically refresh logger dashboard data for all clusters"""
    logging.info(f"Auto-refreshing logger dashboard data for {selected_date}")
    
    clusters_to_process = [name for name in CLUSTERS.keys() if name != "All Channels"]
    cluster_progress = {}
    
    for cluster_name in clusters_to_process:
        try:
            # Check if we need to refresh this cluster
            if not is_logger_data_cached(selected_date, cluster_name) or should_refresh_logger_cache(selected_date, cluster_name):
                logging.info(f"Auto-refreshing cluster: {cluster_name}")
                
                cluster_data, low_durn_channels, error_message = get_combined_data_for_cluster(cluster_name, selected_date)
                
                if error_message:
                    logging.error(f"Auto-refresh failed for {cluster_name}: {error_message}")
                    continue
                
                # Cache the fresh data
                cache_logger_cluster_data(selected_date, cluster_name, cluster_data, low_durn_channels)
                
                # Calculate progress
                total_channels = len(cluster_data)
                if total_channels > 0:
                    qced_channels = total_channels - len(low_durn_channels)
                    percentage = (qced_channels / total_channels * 100)
                    cluster_progress[cluster_name] = {'total': total_channels, 'qced': qced_channels, 'percentage': round(percentage, 1)}
                else:
                    cluster_progress[cluster_name] = {'total': 0, 'qced': 0, 'percentage': 0}
                
                logging.info(f"Auto-refreshed {cluster_name}: {total_channels} total, {qced_channels} QCed ({percentage:.1f}%)")
            else:
                # Use existing cached data for progress calculation
                if is_logger_data_cached(selected_date, cluster_name):
                    cluster_data, low_durn_channels = get_cached_logger_cluster_data(selected_date, cluster_name)
                    total_channels = len(cluster_data)
                    if total_channels > 0:
                        qced_channels = total_channels - len(low_durn_channels)
                        percentage = (qced_channels / total_channels * 100)
                        cluster_progress[cluster_name] = {'total': total_channels, 'qced': qced_channels, 'percentage': round(percentage, 1)}
        
        except Exception as e:
            logging.error(f"Auto-refresh error for {cluster_name}: {str(e)}")
            continue
    
    # Also process "All Channels" separately
    try:
        if not is_logger_data_cached(selected_date, "All Channels") or should_refresh_logger_cache(selected_date, "All Channels"):
            logging.info("Auto-refreshing cluster: All Channels")
            cluster_data, low_durn_channels, error_message = get_combined_data_for_cluster("All Channels", selected_date)
            
            if not error_message:
                cache_logger_cluster_data(selected_date, "All Channels", cluster_data, low_durn_channels)
                total_channels = len(cluster_data)
                qced_channels = total_channels - len(low_durn_channels)
                percentage = (qced_channels / total_channels * 100) if total_channels > 0 else 0
                cluster_progress["All Channels"] = {'total': total_channels, 'qced': qced_channels, 'percentage': round(percentage, 1)}
                logging.info(f"Auto-refreshed All Channels: {total_channels} total, {qced_channels} QCed ({percentage:.1f}%)")
    except Exception as e:
        logging.error(f"Auto-refresh error for All Channels: {str(e)}")
    
    # Cache the progress data
    if cluster_progress:
        cache_logger_cluster_progress(selected_date, cluster_progress)
        logging.info(f"Auto-refreshed cluster progress for {len(cluster_progress)} clusters")

def schedule_cleanup():
    """Schedule the cleanup task to run every Tuesday at 2 AM"""
    schedule.every().tuesday.at("02:00").do(cleanup_old_data)
    logging.info("Scheduled database cleanup for every Tuesday at 2:00 AM")

def schedule_auto_refresh():
    """Schedule auto-refresh every 20 minutes"""
    schedule.every(CACHE_REFRESH_MINUTES).minutes.do(auto_refresh_cache)
    logging.info(f"Scheduled auto-refresh every {CACHE_REFRESH_MINUTES} minutes")

def run_scheduler():
    """Run the scheduler in a separate thread"""
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

# Start the scheduler thread
scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()

# =============================================================================
# SHARED UTILITY FUNCTIONS
# =============================================================================

def normalize_name(name):
    if not isinstance(name, str): return ""
    normalized = name.strip().lower()
    return CHANNEL_NAME_ALIASES.get(normalized, normalized)

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

# =============================================================================
# IT DASHBOARD FUNCTIONS
# =============================================================================

def fetch_logger_data_chunk(channel_ids, selected_date):
    headers = {"Content-Type": "application/json"}
    payload = {
        "StartDateUTC": f"{selected_date}T00:00:00", "EndDateUTC": f"{selected_date}T23:59:59",
        "SignalIds": [], "ChannelIds": channel_ids
    }
    try:
        response = requests.post(LOGGER_CONFIG["API_URL"], json=payload, headers=headers, timeout=120)
        return response.json() if response.ok else []
    except requests.RequestException: return []

def get_all_logger_data(selected_date):
    channel_ids = LOGGER_CONFIG["ALL_CHANNEL_IDS"]
    chunks = [channel_ids[i:i + 15] for i in range(0, len(channel_ids), 15)]
    all_clips = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(fetch_logger_data_chunk, chunks, [selected_date] * len(chunks))
        for result in results:
            if result: all_clips.extend(result)
    
    channel_data = defaultdict(lambda: {'original_name': '', 'latest_dt': datetime.min})
    for clip in all_clips:
        original_name = clip.get("channelname")
        normalized = normalize_name(original_name)
        if not normalized: continue
        try:
            clip_dt = datetime.strptime(f"{clip['ClipEndDate']} {clip['ClipEndTime']}", "%d-%m-%Y %H:%M:%S")
            if clip_dt > channel_data[normalized]['latest_dt']:
                channel_data[normalized]['latest_dt'] = clip_dt
                channel_data[normalized]['original_name'] = original_name
        except (ValueError, TypeError, KeyError): continue
    
    return {
        norm_name: {'original_name': data['original_name'], 'logger_end_time': data['latest_dt'].strftime("%H:%M:%S")}
        for norm_name, data in channel_data.items() if data['original_name']
    }

def get_fresh_csrf_and_session():
    try:
        test_response = authenticated_session.get(QC_CONFIG["QC_NEWS_PAGE_URL"], timeout=10, allow_redirects=True)
        if "/tabsons/login" in test_response.url:
            login_page = authenticated_session.get(QC_CONFIG["LOGIN_PAGE_URL"])
            soup = BeautifulSoup(login_page.text, 'html.parser')
            csrf = soup.find('input', {'name': '_csrf'})['value']
            payload = {'username': QC_CONFIG["USERNAME"], 'password': QC_CONFIG["PASSWORD"], '_csrf': csrf}
            authenticated_session.post(QC_CONFIG["PROCESS_LOGIN_URL"], data=payload, timeout=10)
        
        qc_page = authenticated_session.get(QC_CONFIG["QC_NEWS_PAGE_URL"])
        qc_page.raise_for_status()
        soup = BeautifulSoup(qc_page.text, 'html.parser')
        return soup.find('input', {'name': '_csrf'})['value']
    except (requests.RequestException, AttributeError, KeyError): return None

def get_last_qc_clip_time_for_channel(args):
    normalized_name, channel_info, selected_date, csrf_token = args
    date_for_api = datetime.strptime(selected_date, '%Y-%m-%d').strftime('%Y%m%d')
    payload = {
        'draw': '1', 'start': '0', 'length': '1', 'order[0][column]': '2', 'order[0][dir]': 'desc',
        'search_loggerid': channel_info['logger_id'], 'search_datenumber': date_for_api,
        'search_barcchannelcode': channel_info['barc_code'], 'qcedid': '-1', 'fieldname': 'StoryAndHeadlines'
    }
    headers = {'X-CSRF-TOKEN': csrf_token, 'X-Requested-With': 'XMLHttpRequest'}
    try:
        response = authenticated_session.post(QC_CONFIG["STORY_DATA_API_URL"], data=payload, headers=headers)
        story_data = json.loads(response.json().get('data', '[]'))
        return normalized_name, story_data[0].get('clipendtime') if story_data else None
    except (requests.RequestException, json.JSONDecodeError): return normalized_name, None

def get_all_qc_data_with_times(selected_date):
    csrf_token = get_fresh_csrf_and_session()
    if not csrf_token: return None, "Could not log in to the QC server."
    date_for_api = datetime.strptime(selected_date, '%Y-%m-%d').strftime('%d/%m/%Y')
    payload = {'search_currentdate': date_for_api}
    headers = {'X-CSRF-TOKEN': csrf_token, 'X-Requested-With': 'XMLHttpRequest'}
    try:
        response = authenticated_session.post(QC_CONFIG["CHANNEL_DATA_API_URL"], data=payload, headers=headers)
        qc_grid_data = response.json().get('qcGridData', [])
    except (requests.RequestException, json.JSONDecodeError) as e:
        return None, f"Error fetching QC channel list: {e}"
    
    channels_on_page = {
        normalize_name(row.get('channelname')): {'barc_code': row.get('barcchannelcode'), 'logger_id': row.get('loggerid')}
        for row in qc_grid_data if normalize_name(row.get('channelname'))
    }
    qc_data = {}
    tasks = [(name, info, selected_date, csrf_token) for name, info in channels_on_page.items()]
    with ThreadPoolExecutor(max_workers=10) as executor:
        for norm_name, last_clip_time in executor.map(get_last_qc_clip_time_for_channel, tasks):
            if norm_name: qc_data[norm_name] = {'last_qc_end_time': last_clip_time}
    return qc_data, None

# =============================================================================
# LOGGER DASHBOARD FUNCTIONS
# =============================================================================

def process_xen_data(json_data):
    """Processes Xen data, returning a dict keyed by UNIQUE channel ID."""
    channel_data = defaultdict(lambda: {'name': '', 'start': datetime.max, 'end': datetime.min})
    if not isinstance(json_data, list): return {}
    for clip in json_data:
        channel_id = clip.get("ChannelCode")
        if not channel_id: continue
        channel_data[channel_id]['name'] = clip.get("channelname", "Unknown")
        start_time_str, start_date_str = clip.get("ClipStartTime"), clip.get("ClipStartDate")
        end_time_str, end_date_str = clip.get("ClipEndTime"), clip.get("ClipEndDate")
        if not all([start_time_str, start_date_str, end_time_str, end_date_str]): continue
        try:
            start_dt = datetime.strptime(f"{start_date_str} {start_time_str}", "%d-%m-%Y %H:%M:%S")
            end_dt = datetime.strptime(f"{end_date_str} {end_time_str}", "%d-%m-%Y %H:%M:%S")
            if start_dt < channel_data[channel_id]['start']: channel_data[channel_id]['start'] = start_dt
            if end_dt > channel_data[channel_id]['end']: channel_data[channel_id]['end'] = end_dt
        except (ValueError, TypeError): continue
    
    processed = {}
    for cid, data in channel_data.items():
        start_str = data['start'].strftime("%H:%M:%S") if data['start'] != datetime.max else "N/A"
        end_str = data['end'].strftime("%H:%M:%S") if data['end'] != datetime.min else "N/A"
        if end_str == "00:00:00": end_str = "23:59:59"
        processed[cid] = {"name": data['name'], "start": start_str, "end": end_str}
    return processed

def fetch_xen_chunk(channel_ids, selected_date):
    payload = {"StartDateUTC": f"{selected_date}T00:00:00", "EndDateUTC": f"{selected_date}T23:59:59", "SignalIds": [], "ChannelIds": channel_ids}
    try:
        response = requests.post(XEN_API_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=120)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Xen API error: {e}")
        return []

def fetch_eq_channel(channel_id, selected_date):
    """Returns (channel_id, channel_name, result_dict)"""
    report_date = datetime.strptime(selected_date, "%Y-%m-%d").strftime("%d-%m-%Y")
    url = EQ_API_URL.format(channel_id=channel_id, date=report_date)
    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 500:
            logging.warning(f"EQ Server HTTP 500 for channel {channel_id}")
            return channel_id, None, {"error": "Server Failed (500)"}
        response.raise_for_status()
        data = response.json()
        all_clips = data.get("TabsonsReport")
        if not all_clips or not isinstance(all_clips, list): return channel_id, None, None
        channel_name = all_clips[0].get("channelname")
        filtered_clips = [c for c in all_clips if c.get("ProgramType") == "News" and c.get("ClipStartTime") and c.get("ClipEndTime")]
        if not filtered_clips: return channel_id, channel_name, None
        start_times = [datetime.strptime(c['ClipStartTime'], "%H:%M:%S") for c in filtered_clips]
        end_times = [datetime.strptime(c['ClipEndTime'], "%H:%M:%S") for c in filtered_clips]
        end_time_str = max(end_times).strftime("%H:%M:%S")
        if end_time_str == "00:00:00": end_time_str = "23:59:59"
        result = {"start": min(start_times).strftime("%H:%M:%S"), "end": end_time_str}
        return channel_id, channel_name, result
    except requests.exceptions.RequestException as e:
        logging.error(f"EQ Network error for {channel_id}: {e}")
        return channel_id, None, {"error": "Network Error"}
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"EQ Invalid data for {channel_id}: {e}")
        return channel_id, None, {"error": "Invalid Data"}

def get_combined_data_for_cluster(cluster_name, selected_date):
    cache_key = (cluster_name, selected_date)
    if cache_key in cache:
        logging.info(f"Cache hit for ({cluster_name}, {selected_date}).")
        return cache[cache_key]
    logging.info(f"Fetching new data for cluster: '{cluster_name}' on date: {selected_date}")
    channel_ids_in_cluster = CLUSTERS.get(cluster_name, [])
    if not channel_ids_in_cluster: return {}, {}, "Invalid cluster selected."
    
    # NEW STRUCTURE: Keyed by unique channel ID
    combined_data = defaultdict(lambda: {"name": "Unknown", "logs": []})
    eq_semaphore = threading.Semaphore(MAX_EQ_CONCURRENT_REQUESTS)

    def fetch_eq_channel_politely(cid, s_date):
        with eq_semaphore:
            time.sleep(EQ_REQUEST_DELAY_SECONDS)
            return fetch_eq_channel(cid, s_date)

    with ThreadPoolExecutor(max_workers=10) as executor:
        xen_chunks = [channel_ids_in_cluster[i:i + 10] for i in range(0, len(channel_ids_in_cluster), 10)]
        xen_futures = [executor.submit(fetch_xen_chunk, chunk, selected_date) for chunk in xen_chunks]
        eq_futures = {executor.submit(fetch_eq_channel_politely, cid, selected_date): cid for cid in EQ_CHANNELS if cid in channel_ids_in_cluster}
        
        xen_raw_data = []
        for future in xen_futures: xen_raw_data.extend(future.result())
        xen_processed_data = process_xen_data(xen_raw_data)
        for cid, data in xen_processed_data.items():
            combined_data[cid]['name'] = data['name']
            combined_data[cid]['logs'].append({"logger": "Xen", "start": data['start'], "end": data['end']})

        for future in eq_futures:
            cid, cname, result = future.result()
            if cname: combined_data[cid]['name'] = cname
            
            if result and "error" in result:
                combined_data[cid]['logs'].append({"logger": "EQ", "start": f"Error: {result['error']}", "end": "N/A"})
            elif result:
                combined_data[cid]['logs'].append({"logger": "EQ", **result})

    low_durn_channels = set() # Will store IDs
    for cid, data in combined_data.items():
        latest_time = "00:00:00"
        for log in data['logs']:
            end_time = log.get('end', '0')
            if end_time and "Error" not in end_time and end_time > latest_time: latest_time = end_time
        if latest_time < "23:00:00": low_durn_channels.add(cid)
    
    result_tuple = (dict(combined_data), low_durn_channels, None)
    cache[cache_key] = result_tuple
    return result_tuple

# =============================================================================
# QC DASHBOARD FUNCTIONS
# =============================================================================

def get_channel_data_with_progress(date_for_api):
    """Fetches channel data and calculates cluster progress."""
    csrf_token = get_fresh_csrf_and_session()
    if not csrf_token:
        return {'error': "Could not log in to the remote server."}, 500

    payload = {'search_currentdate': date_for_api}
    headers = {'X-CSRF-TOKEN': csrf_token, 'X-Requested-With': 'XMLHttpRequest'}

    try:
        response = authenticated_session.post(QC_CONFIG["CHANNEL_DATA_API_URL"], data=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        channel_data = data.get('qcGridData', [])
        
        # --- Start: Cluster Progress Calculation (FIXED LOGIC) ---
        unique_channels = {}
        for row in channel_data:
            barc_code = row.get('barcchannelcode')
            if not barc_code:
                continue

            if barc_code not in unique_channels:
                unique_channels[barc_code] = {
                    'clusterid': str(row.get('clusterid')),
                    'total_hours': 0,
                    'is_complete': False,
                    # Initialize pendingqc_count high. It should be updated from row data.
                    'pendingqc_count': 999999 
                }

            # 1. Accumulate Total Hours
            total_time_str = row.get('totltime')
            if total_time_str and isinstance(total_time_str, str):
                try:
                    time_parts = total_time_str.split(':')
                    hours = int(time_parts[0])
                    minutes = int(time_parts[1]) if len(time_parts) > 1 else 0
                    seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
                    logger_total_hours = hours + (minutes / 60.0) + (seconds / 3600.0)
                    unique_channels[barc_code]['total_hours'] += logger_total_hours
                except (ValueError, IndexError):
                    pass

            # 2. Update Pending QC Count: ONLY update if the row has a valid integer, 
            # as it's a channel-level metric that might be missing on some logger rows.
            # We assume a valid value in ANY row for the channel is the true value.
            pending_qc_val = row.get('pendqcrec')
            if pending_qc_val is not None:
                try:
                    pending_qc = int(pending_qc_val)
                    # Overwrite the default 999999 with the actual value from the row
                    unique_channels[barc_code]['pendingqc_count'] = pending_qc 
                except (TypeError, ValueError):
                    # Ignore if it's not a valid number, keeping the default/previous value
                    pass

        # 3. Determine Completion Status
        # A channel is complete if total_hours >= 17.0 AND pendingqc_count < 100
        for channel_info in unique_channels.values():
            is_high_duration = channel_info['total_hours'] >= 17.0
            is_low_pending_qc = channel_info.get('pendingqc_count', 999999) < 100

            if is_high_duration and is_low_pending_qc:
                channel_info['is_complete'] = True

        # 4. Calculate Cluster Progress
        cluster_progress = OrderedDict([
            ('1', {'name': 'All East', 'total': 15, 'completed': 0}),
            ('3', {'name': 'All National', 'total': 21, 'completed': 0}),
            ('5', {'name': 'All West', 'total': 14, 'completed': 0}),
            ('2', {'name': 'All Hindi Regional', 'total': 18, 'completed': 0}),
            ('4', {'name': 'All South', 'total': 24, 'completed': 0})
        ])

        for channel_info in unique_channels.values():
            cluster_id = channel_info['clusterid']
            if cluster_id in cluster_progress and channel_info['is_complete']:
                cluster_progress[cluster_id]['completed'] += 1
        
        cluster_names = {'5': 'West', '4': 'South', '3': 'National', '2': 'Hindi Reg', '1': 'East'}
        
        for row in channel_data:
            row['clustername'] = cluster_names.get(str(row.get('clusterid')), f"Cluster {row.get('clusterid')}")
            
            # Calculate individual logger time for sorting (for data table)
            total_time_str = row.get('totltime')
            row['total_hours_numeric'] = 0.0
            if total_time_str and isinstance(total_time_str, str):
                try:
                    time_parts = total_time_str.split(':')
                    hours = int(time_parts[0])
                    minutes = int(time_parts[1]) if len(time_parts) > 1 else 0
                    seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
                    row['total_hours_numeric'] = hours + (minutes / 60.0) + (seconds / 3600.0)
                except (ValueError, IndexError):
                    pass
            
            # Mark as low duration (INCOMPLETE)
            # is_low_duration = True if channel is INCOMPLETE
            barc_code = row.get('barcchannelcode')
            is_complete_status = unique_channels.get(barc_code, {}).get('is_complete', False)
            row['is_low_duration'] = not is_complete_status

        # Sort channels (Completed first, then Incomplete)
        channel_data.sort(key=lambda x: (
            not x.get('is_low_duration', False), # Sorts 'is_low_duration=False' (Completed) channels first
            int(x.get('clusterid', 0)),
            x.get('channelname', '').lower()
        ))

        # Calculate percentages for the final progress output
        final_cluster_progress = []
        for data in cluster_progress.values():
            if data['total'] > 0:
                data['percentage'] = round((data['completed'] / data['total']) * 100, 1)
            else:
                data['percentage'] = 0
            final_cluster_progress.append(data)
        # --- End: Cluster Progress Calculation (FIXED LOGIC) ---

        return {
            'channel_data': channel_data,
            'cluster_progress': final_cluster_progress
        }, 200

    except (requests.RequestException, json.JSONDecodeError) as e:
        error_message = f"Error fetching channel data: {e}"
        if 'response' in locals():
             error_message += f"<br>Response: {response.text}"
        return {'error': error_message}, 500

# =============================================================================
# STATIC FILE ROUTES
# =============================================================================

@app.route('/favicon.ico')
def favicon(): 
    return send_from_directory('static', 'favicon.svg', mimetype='image/svg+xml')

@app.route('/static/<path:filename>')
def static_files(filename): 
    return send_from_directory('static', filename)

# =============================================================================
# MAIN NAVIGATION ROUTE
# =============================================================================

@app.route('/')
def index():
    """Main dashboard navigation page"""
    return render_template('index.html')

# =============================================================================
# IT DASHBOARD ROUTES
# =============================================================================

@app.route("/it-dashboard", methods=["GET", "POST"])
def it_dashboard_page():
    selected_date = request.form.get("date", (date.today() - timedelta(days=1)).isoformat())
    return render_template("it_dashboard.html", selected_date=selected_date)

@app.route("/dashboard_data_api", methods=["POST"])
def dashboard_data_api():
    selected_date = request.form.get("selected_date")
    force_refresh = request.form.get("force_refresh", "false").lower() == "true"
    if not selected_date:
        return jsonify({"error": "Missing selected_date parameter."}), 400
        
    logging.info(f"IT Dashboard API request for {selected_date} (force_refresh: {force_refresh})")
    
    try:
        # Check if we should force refresh or use cached data
        if force_refresh or not is_data_cached(selected_date):
            if force_refresh:
                logging.info(f"Force refresh requested for IT dashboard on {selected_date}")
            else:
                logging.info(f"No cached data found for {selected_date}, fetching fresh data")
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_logger_data = executor.submit(get_all_logger_data, selected_date)
                future_qc_data = executor.submit(get_all_qc_data_with_times, selected_date)
                logger_data = future_logger_data.result()
                qc_data, qc_error = future_qc_data.result()

            if qc_error: return jsonify({"error": qc_error}), 500
            if qc_data is None: return jsonify({"error": "Failed to fetch QC data"}), 500

            # Cache the raw data
            cache_logger_data(selected_date, logger_data)
            cache_qc_data(selected_date, qc_data)

            matched_qc_data = {
                logger_norm_name: qc_data[max(qc_data.keys(), key=lambda qc_name: similarity(logger_norm_name, qc_name))]
                for logger_norm_name in logger_data
                if max(qc_data.keys(), key=lambda qc_name: similarity(logger_norm_name, qc_name)) and similarity(logger_norm_name, max(qc_data.keys(), key=lambda qc_name: similarity(logger_norm_name, qc_name))) >= FUZZY_MATCH_THRESHOLD
            }
            
            dashboard_data = []
            for norm_name in sorted(logger_data.keys()):
                log_info = logger_data[norm_name]
                qc_info = matched_qc_data.get(norm_name)
                
                logger_time = log_info.get('logger_end_time')
                if logger_time == "00:00:00":
                    logger_time = "23:59:59"
                qc_time = qc_info.get('last_qc_end_time') if qc_info else None

                if qc_time and qc_time.startswith("00:00"):
                    qc_time = "23:59:59"

                is_tagging_complete = logger_time and logger_time >= TAGGING_COMPLETION_THRESHOLD
                is_qc_done = qc_time and qc_time >= "23:30:00"

                details = {
                    "channel_name": log_info['original_name'],
                    "logger_end_time": logger_time or "N/A",
                    "qc_end_time": qc_time or "Not in QC"
                }

                status_for_sort = ""
                if is_qc_done:
                    status_for_sort = "QC DONE"
                    details["status_class"] = "status-completed"
                elif is_tagging_complete and (qc_time and qc_time >= QC_COMPLETION_THRESHOLD):
                    status_for_sort = "QC DONE"
                    details["status_class"] = "status-completed"
                elif not is_tagging_complete:
                    status_for_sort = "Tagging in Progress"
                    details["status_class"] = "status-progress"
                else:
                    details["status_class"] = "status-eligible"
                    status_for_sort = "Eligible to Pull (Catch-up)" if qc_info and qc_time else "Eligible to Pull (Default)"
                
                details["status_for_sort"] = status_for_sort
                dashboard_data.append(details)

            sort_order = {"Eligible to Pull (Default)": 0, "Eligible to Pull (Catch-up)": 1, "Tagging in Progress": 2, "QC DONE": 3}
            dashboard_data.sort(key=lambda item: sort_order.get(item['status_for_sort'], 99))
            
            # Remove the temporary sort key before sending the response
            for item in dashboard_data:
                del item['status_for_sort']
            
            # Cache the processed dashboard data
            cache_dashboard_data(selected_date, dashboard_data)
            
            return jsonify({"dashboard_data": dashboard_data})
        else:
            # Use cached data
            logging.info(f"Using cached data for {selected_date}")
            cached_data = get_cached_dashboard_data(selected_date)
            return jsonify({"dashboard_data": cached_data})
    
    except Exception as e:
        print(f"Unexpected error in dashboard API: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected server error occurred: {str(e)}"}), 500

# =============================================================================
# LOGGER DASHBOARD ROUTES
# =============================================================================

@app.route("/logger-dashboard")
def logger_dashboard():
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    return render_template("logger_dashboard.html", clusters=CLUSTERS.keys(), selected_date=yesterday, selected_cluster="All Channels")

@app.route("/api/dashboard_data")
def logger_dashboard_data_api():
    selected_date = request.args.get('date', (date.today() - timedelta(days=1)).isoformat())
    selected_cluster = request.args.get('cluster', 'All Channels')
    force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
    logging.info(f"API request for cluster: '{selected_cluster}' on date: {selected_date} (force_refresh: {force_refresh})")

    try:
        # Check if we should force refresh or use cached data
        if force_refresh or not is_logger_data_cached(selected_date, selected_cluster):
            if force_refresh:
                logging.info(f"Force refresh requested for {selected_cluster} on {selected_date}")
            else:
                logging.info(f"No cached data found for {selected_cluster} on {selected_date}, fetching fresh data")
            
            cluster_data, low_durn_channels, error_message = get_combined_data_for_cluster(selected_cluster, selected_date)
            
            # Cache the fresh data if no error occurred
            if not error_message:
                cache_logger_cluster_data(selected_date, selected_cluster, cluster_data, low_durn_channels)
        else:
            # Use cached data
            logging.info(f"Using cached logger data for {selected_cluster} on {selected_date}")
            cluster_data, low_durn_channels = get_cached_logger_cluster_data(selected_date, selected_cluster)
            error_message = None
        
        # NEW: Convert dict to a list of objects for the client
        channel_list = []
        for cid, data in cluster_data.items():
            channel_list.append({
                "id": cid,
                "name": data['name'],
                "logs": data['logs'],
            })

        # Check if we have any data
        if not channel_list:
            return jsonify({
                'channel_data': [],
                'low_durn_channels': [],
                'error_message': f"No data available for cluster '{selected_cluster}' on {selected_date}. The data source may be unavailable or the cluster may have no channels.",
                'total_channels': 0,
                'is_empty': True
            }), 404

        return jsonify({
            'channel_data': channel_list,
            'low_durn_channels': list(low_durn_channels), # Send list of IDs
            'error_message': error_message,
            'total_channels': len(channel_list)
        })
    
    except Exception as e:
        logging.error(f"Error in logger_dashboard_data_api: {str(e)}")
        return jsonify({
            'channel_data': [],
            'low_durn_channels': [],
            'error_message': f"Server error occurred while fetching data: {str(e)}",
            'total_channels': 0,
            'is_error': True
        }), 500

@app.route("/api/all_clusters_progress")
def all_clusters_progress_api():
    selected_date = request.args.get('date', (date.today() - timedelta(days=1)).isoformat())
    force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
    clusters_to_check = [name for name in CLUSTERS.keys() if name != "All Channels"]
    
    logging.info(f"Cluster progress API request for {selected_date} (force_refresh: {force_refresh})")
    
    try:
        # Check if we should force refresh or use cached data
        if force_refresh:
            logging.info(f"Force refresh requested for cluster progress on {selected_date}")
            cluster_progress = {}
            for cluster_name in clusters_to_check:
                cluster_data, low_durn_channels, error_message = get_combined_data_for_cluster(cluster_name, selected_date)
                
                # Only cache if no error occurred
                if not error_message:
                    cache_logger_cluster_data(selected_date, cluster_name, cluster_data, low_durn_channels)
                
                total_channels = len(cluster_data)
                if total_channels > 0:
                    qced_channels = total_channels - len(low_durn_channels)
                    percentage = (qced_channels / total_channels * 100)
                    cluster_progress[cluster_name] = {
                        'total': total_channels,
                        'qced': qced_channels,
                        'percentage': round(percentage, 1),
                        'error': error_message
                    }
                else:
                    cluster_progress[cluster_name] = {
                        'total': 0,
                        'qced': 0,
                        'percentage': 0,
                        'error': error_message or "No channels found"
                    }
            
            # Cache the progress data
            cache_logger_cluster_progress(selected_date, cluster_progress)
        else:
            # Check if we have cached progress data for all clusters
            all_clusters_cached = all(is_logger_data_cached(selected_date, cluster_name) for cluster_name in clusters_to_check)
            
            if all_clusters_cached:
                logging.info(f"Using cached cluster progress for {selected_date}")
                cluster_progress = get_cached_logger_cluster_progress(selected_date)
                
                # If we don't have complete progress data, fetch missing clusters
                if len(cluster_progress) < len(clusters_to_check):
                    logging.info(f"Some cluster progress missing, fetching missing data for {selected_date}")
                    for cluster_name in clusters_to_check:
                        if cluster_name not in cluster_progress:
                            if is_logger_data_cached(selected_date, cluster_name):
                                cluster_data, low_durn_channels = get_cached_logger_cluster_data(selected_date, cluster_name)
                                error_message = None
                            else:
                                cluster_data, low_durn_channels, error_message = get_combined_data_for_cluster(cluster_name, selected_date)
                                # Only cache if no error occurred
                                if not error_message:
                                    cache_logger_cluster_data(selected_date, cluster_name, cluster_data, low_durn_channels)
                            
                            total_channels = len(cluster_data)
                            if total_channels > 0:
                                qced_channels = total_channels - len(low_durn_channels)
                                percentage = (qced_channels / total_channels * 100)
                                cluster_progress[cluster_name] = {
                                    'total': total_channels,
                                    'qced': qced_channels,
                                    'percentage': round(percentage, 1),
                                    'error': error_message
                                }
                            else:
                                cluster_progress[cluster_name] = {
                                    'total': 0,
                                    'qced': 0,
                                    'percentage': 0,
                                    'error': error_message or "No channels found"
                                }
                    
                    # Cache the complete progress data
                    cache_logger_cluster_progress(selected_date, cluster_progress)
            else:
                # Only fetch fresh data for clusters that don't have cached data
                logging.info(f"Some clusters not cached, fetching missing data for {selected_date}")
                cluster_progress = {}
                for cluster_name in clusters_to_check:
                    if is_logger_data_cached(selected_date, cluster_name):
                        cluster_data, low_durn_channels = get_cached_logger_cluster_data(selected_date, cluster_name)
                        error_message = None
                    else:
                        cluster_data, low_durn_channels, error_message = get_combined_data_for_cluster(cluster_name, selected_date)
                        # Only cache if no error occurred
                        if not error_message:
                            cache_logger_cluster_data(selected_date, cluster_name, cluster_data, low_durn_channels)
                    
                    total_channels = len(cluster_data)
                    if total_channels > 0:
                        qced_channels = total_channels - len(low_durn_channels)
                        percentage = (qced_channels / total_channels * 100)
                        cluster_progress[cluster_name] = {
                            'total': total_channels,
                            'qced': qced_channels,
                            'percentage': round(percentage, 1),
                            'error': error_message
                        }
                    else:
                        cluster_progress[cluster_name] = {
                            'total': 0,
                            'qced': 0,
                            'percentage': 0,
                            'error': error_message or "No channels found"
                        }
                
                # Cache the progress data
                cache_logger_cluster_progress(selected_date, cluster_progress)
        
        # Check if we have any data
        if not cluster_progress:
            return jsonify({
                'error': f"No progress data available for any clusters on {selected_date}. The data sources may be unavailable.",
                'is_empty': True
            }), 404
        
        return jsonify(cluster_progress)
    
    except Exception as e:
        logging.error(f"Error in all_clusters_progress_api: {str(e)}")
        return jsonify({
            'error': f"Server error occurred while fetching cluster progress: {str(e)}",
            'is_error': True
        }), 500

# =============================================================================
# QC DASHBOARD ROUTES
# =============================================================================

@app.route("/qc-dashboard", methods=['GET', 'POST'])
def qc_dashboard():
    selected_date_str = None
    if request.method == 'POST':
        selected_date_str = request.form.get('selected_date')
    else: # GET request
        # Default to yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        selected_date_str = yesterday.strftime('%Y-%m-%d')

    # The main page now only renders the shell, not the data
    return render_template('qc_dashboard.html', selected_date=selected_date_str)

@app.route('/channel_data_api', methods=['POST'])
def qc_channel_data_api():
    selected_date_str = request.form.get('selected_date')
    if not selected_date_str:
        return jsonify({'error': 'Missing selected_date parameter.'}), 400

    try:
        date_obj = datetime.strptime(selected_date_str, '%Y-%m-%d')
        date_for_api = date_obj.strftime('%d/%m/%Y')
    except ValueError:
        return jsonify({'error': 'Invalid date format.'}), 400
    
    data, status_code = get_channel_data_with_progress(date_for_api)

    return jsonify(data), status_code

@app.route('/last_clip_times')
def qc_last_clip_times():
    """API endpoint to get the last clip start and end times for a channel."""
    selected_date_str = request.args.get('date')
    barc_code = request.args.get('barc_code')
    logger_id = request.args.get('logger_id')

    if not all([selected_date_str, barc_code, logger_id]):
        return jsonify({'error': 'Missing required parameters: date, barc_code, or logger_id'}), 400

    date_obj = datetime.strptime(selected_date_str, '%Y-%m-%d')
    date_for_api = date_obj.strftime('%Y%m%d')
    
    csrf_token = get_fresh_csrf_and_session()
    if not csrf_token:
        return jsonify({'error': 'Could not authenticate with the remote server'}), 500

    # Payload to get the last story by ordering by clip start time descending and fetching only 1
    story_payload = {
        'draw': '1', 'start': '0', 'length': '1', 'search[value]': '', 'search[regex]': 'false',
        'order[0][column]': '2', 'order[0][dir]': 'desc', # Order by clipstarttime descending
        'search_loggerid': logger_id, 'search_datenumber': date_for_api,
        'search_barcchannelcode': barc_code, 'qcedid': '-1', 'fieldname': 'StoryAndHeadlines'
    }
    headers = {'X-CSRF-TOKEN': csrf_token, 'X-Requested-With': 'XMLHttpRequest'}

    try:
        response = authenticated_session.post(QC_CONFIG["STORY_DATA_API_URL"], data=story_payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        story_data_raw = data.get('data', '[]')
        story_data = json.loads(story_data_raw) if isinstance(story_data_raw, str) else story_data_raw

        if story_data:
            last_story = story_data[0]
            return jsonify({
                'clipstarttime': last_story.get('clipstarttime'),
                'clipendtime': last_story.get('clipendtime')
            })
        else:
            return jsonify({'message': 'No clips found for this channel on the selected date.'})

    except (requests.RequestException, json.JSONDecodeError) as e:
        return jsonify({'error': f'An error occurred: {e}'}), 500

@app.route('/stories')
def qc_stories():
    selected_date_str = request.args.get('date')
    barc_code = request.args.get('barc_code')
    channel_name = request.args.get('channel_name', 'Unknown Channel')
    logger_id = request.args.get('logger_id')

    if not all([selected_date_str, barc_code, logger_id]):
        return "Missing date, BARC code, or Logger ID.", 400

    date_obj = datetime.strptime(selected_date_str, '%Y-%m-%d')
    date_for_api = date_obj.strftime('%Y%m%d')

    csrf_token = get_fresh_csrf_and_session()
    if not csrf_token:
        return "Could not log in to the remote server.", 500

    story_payload = {
        'draw': '1', 'start': '0', 'length': '500', 'search[value]': '', 'search[regex]': 'false',
        'order[0][column]': '2', 'order[0][dir]': 'asc',
        'search_loggerid': logger_id, 'search_datenumber': date_for_api,
        'search_barcchannelcode': barc_code, 'qcedid': '-1', 'fieldname': 'StoryAndHeadlines'
    }
    headers = {'X-CSRF-TOKEN': csrf_token, 'X-Requested-With': 'XMLHttpRequest'}

    try:
        response = authenticated_session.post(QC_CONFIG["STORY_DATA_API_URL"], data=story_payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        story_data_raw = data.get('data', [])
        story_data = []

        if isinstance(story_data_raw, str) and story_data_raw:
            try:
                story_data = json.loads(story_data_raw)
            except json.JSONDecodeError:
                print(f"Error decoding the nested JSON string for {channel_name}")
        elif isinstance(story_data_raw, list):
            story_data = story_data_raw

        return render_template('stories.html', story_data=story_data, date=date_obj.strftime('%B %d, %Y'), channel_name=channel_name)
    except (requests.RequestException, json.JSONDecodeError) as e:
        return f"Error fetching story data: {e}<br>Response: {response.text}", 500

if __name__ == "__main__":
    # Initialize the database
    init_database()
    
    # Schedule the cleanup task
    schedule_cleanup()
    
    # Schedule the auto-refresh task
    schedule_auto_refresh()
    
    logging.info("Starting Unified Dashboard Application on http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)