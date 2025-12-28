

from flask import Flask, render_template, request, url_for, jsonify
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
from collections import OrderedDict

# --- Configuration ---
CONFIG = {
    "BASE_URL": "http://10.18.81.23/tabsons",
    "USERNAME": "abhay.tabsons@gmail.com",
    "PASSWORD": "?&hM6kh5",
}
CONFIG["LOGIN_PAGE_URL"] = f"{CONFIG['BASE_URL']}/login"
CONFIG["PROCESS_LOGIN_URL"] = f"{CONFIG['BASE_URL']}/process-login"
CONFIG["QC_NEWS_PAGE_URL"] = f"{CONFIG['BASE_URL']}/qcnews"
CONFIG["CHANNEL_DATA_API_URL"] = f"{CONFIG['BASE_URL']}/qcnews/getdatagrid"
CONFIG["STORY_DATA_API_URL"] = f"{CONFIG['BASE_URL']}/qcnews/getstorydatagrid"

app = Flask(__name__)
authenticated_session = requests.Session()

def get_fresh_csrf_and_session():
    """
    Ensures the session is logged in and returns a fresh CSRF token.
    Returns: The CSRF token on success, None on failure.
    """
    try:
        # A quick check to see if we are still logged in by accessing a protected page
        test_response = authenticated_session.get(CONFIG["QC_NEWS_PAGE_URL"], timeout=10)
        # If the response URL is the login page, our session has expired
        if "/tabsons/login" in test_response.url:
            print("Session expired. Re-authenticating...")
            login_page_response = authenticated_session.get(CONFIG["LOGIN_PAGE_URL"])
            soup = BeautifulSoup(login_page_response.text, 'html.parser')
            login_csrf = soup.find('input', {'name': '_csrf'})['value']
            login_payload = {'username': CONFIG["USERNAME"], 'password': CONFIG["PASSWORD"], '_csrf': login_csrf}
            authenticated_session.post(CONFIG["PROCESS_LOGIN_URL"], data=login_payload, timeout=10)
            print("Re-authentication successful.")
        
        # Now get a fresh CSRF token from the page
        qc_page_response = authenticated_session.get(CONFIG["QC_NEWS_PAGE_URL"])
        qc_page_response.raise_for_status()
        qc_soup = BeautifulSoup(qc_page_response.text, 'html.parser')
        csrf_token = qc_soup.find('input', {'name': '_csrf'})['value']
        return csrf_token
    except requests.RequestException as e:
        print(f"An error occurred during session validation: {e}")
        return None

# --- NEW FUNCTION FOR DATA FETCHING AND PROCESSING ---
def get_channel_data_with_progress(date_for_api):
    """Fetches channel data and calculates cluster progress."""
    csrf_token = get_fresh_csrf_and_session()
    if not csrf_token:
        return {'error': "Could not log in to the remote server."}, 500

    payload = {'search_currentdate': date_for_api}
    headers = {'X-CSRF-TOKEN': csrf_token, 'X-Requested-With': 'XMLHttpRequest'}

    try:
        response = authenticated_session.post(CONFIG["CHANNEL_DATA_API_URL"], data=payload, headers=headers)
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
# --- END NEW FUNCTION ---

@app.route('/', methods=['GET', 'POST'])
def index():
    selected_date_str = None
    if request.method == 'POST':
        selected_date_str = request.form.get('selected_date')
    else: # GET request
        # Default to yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        selected_date_str = yesterday.strftime('%Y-%m-%d')

    # The main page now only renders the shell, not the data
    return render_template('index.html', selected_date=selected_date_str)


# --- NEW ASYNCHRONOUS API ROUTE ---
@app.route('/channel_data_api', methods=['POST'])
def channel_data_api():
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
# --- END NEW ASYNCHRONOUS API ROUTE ---


@app.route('/last_clip_times')
def last_clip_times():
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
        response = authenticated_session.post(CONFIG["STORY_DATA_API_URL"], data=story_payload, headers=headers)
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
def stories():
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
        response = authenticated_session.post(CONFIG["STORY_DATA_API_URL"], data=story_payload, headers=headers)
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)