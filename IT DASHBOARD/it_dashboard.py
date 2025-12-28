from typing import Any
from flask import Flask, render_template, request, send_from_directory, jsonify
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
from difflib import SequenceMatcher
import traceback

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

def normalize_name(name):
    if not isinstance(name, str): return ""
    normalized = name.strip().lower()
    return CHANNEL_NAME_ALIASES.get(normalized, normalized)

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

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

# FAVICON AND STATIC FILE ROUTES
@app.route('/favicon.ico')
def favicon(): return send_from_directory('static', 'favicon.svg', mimetype='image/svg+xml')

@app.route('/static/<path:filename>')
def static_files(filename): return send_from_directory('static', filename)

# ==============================================================================
# IT DASHBOARD PAGE ROUTE
# ==============================================================================
@app.route("/", methods=["GET", "POST"])
def it_dashboard_page():
    selected_date = request.form.get("date", (date.today() - timedelta(days=1)).isoformat())
    return render_template("it_dashboard.html", selected_date=selected_date)

# ==============================================================================
# IT DASHBOARD API ROUTE FOR DATA
# ==============================================================================
@app.route("/dashboard_data_api", methods=["POST"])
def dashboard_data_api():
    selected_date = request.form.get("selected_date")
    if not selected_date:
        return jsonify({"error": "Missing selected_date parameter."}), 400
        
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_logger_data = executor.submit(get_all_logger_data, selected_date)
            future_qc_data = executor.submit(get_all_qc_data_with_times, selected_date)
            logger_data = future_logger_data.result()
            qc_data, qc_error = future_qc_data.result()

        if qc_error: return jsonify({"error": qc_error}), 500
        if qc_data is None: return jsonify({"error": "Failed to fetch QC data"}), 500

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
        
        return jsonify({"dashboard_data": dashboard_data})
    
    except Exception as e:
        print(f"Unexpected error in dashboard API: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected server error occurred: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5002, debug=True)