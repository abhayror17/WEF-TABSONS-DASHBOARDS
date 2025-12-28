from flask import Flask, render_template, request, jsonify
import requests
from collections import defaultdict
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor
from waitress import serve
from cachetools import TTLCache
import json
import threading
import time
import logging

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

app = Flask(__name__)

# --- CONFIGURATION ---
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
EQ_CHANNELS = ["1010071", "1010160", "1010259","1015471", "1010272", "1010268", "1010170""1010430", "1010485", "1010432", "1010280", "1010013", "1010649"]
all_channels_list = []
for cluster_channels in CLUSTERS.values(): all_channels_list.extend(cluster_channels)
all_channels_list.extend(EQ_CHANNELS)
CLUSTERS["All Channels"] = sorted(list(set(all_channels_list)))

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

@app.route("/")
def index():
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    return render_template("index.html", clusters=CLUSTERS.keys(), selected_date=yesterday, selected_cluster="All Channels")

@app.route("/api/dashboard_data")
def dashboard_data_api():
    selected_date = request.args.get('date', (date.today() - timedelta(days=1)).isoformat())
    selected_cluster = request.args.get('cluster', 'All Channels')
    logging.info(f"API request for cluster: '{selected_cluster}' on date: {selected_date}")

    cluster_data, low_durn_channels, error_message = get_combined_data_for_cluster(selected_cluster, selected_date)
    
    # NEW: Convert dict to a list of objects for the client
    channel_list = []
    for cid, data in cluster_data.items():
        channel_list.append({
            "id": cid,
            "name": data['name'],
            "logs": data['logs'],
        })

    return jsonify({
        'channel_data': channel_list,
        'low_durn_channels': list(low_durn_channels), # Send list of IDs
        'error_message': error_message,
        'total_channels': len(channel_list)
    })

@app.route("/api/all_clusters_progress")
def all_clusters_progress_api():
    selected_date = request.args.get('date', (date.today() - timedelta(days=1)).isoformat())
    cluster_progress = {}
    clusters_to_check = [name for name in CLUSTERS.keys() if name != "All Channels"]
    for cluster_name in clusters_to_check:
        cluster_data, low_durn_channels, _ = get_combined_data_for_cluster(cluster_name, selected_date)
        total_channels = len(cluster_data)
        if total_channels > 0:
            qced_channels = total_channels - len(low_durn_channels)
            percentage = (qced_channels / total_channels * 100)
            cluster_progress[cluster_name] = {'total': total_channels, 'qced': qced_channels, 'percentage': round(percentage, 1)}
        else:
            cluster_progress[cluster_name] = {'total': 0, 'qced': 0, 'percentage': 0}
    return jsonify(cluster_progress)

if __name__ == "__main__":
    logging.info("Starting Logger Dashboard on http://10.18.0.26:5003")
    serve(app, host='10.18.0.26', port=5003)