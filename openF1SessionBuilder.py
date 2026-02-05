import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
import json

# --- Configuration ---
SESSION_KEY = 9523
DRIVER_LIMIT = 22
OUTPUT_DIR = f"race_data_{SESSION_KEY}"
TRACK_MAP_DOWNSAMPLE = 4

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "telemetry"), exist_ok=True)

print(f"Fetching data for Session {SESSION_KEY}")
print(f"Output directory: {OUTPUT_DIR}/")

# ==========================================
# 1. Fetch & Store Static Data
# ==========================================

# A. Session Info
session_url = "https://api.openf1.org/v1/sessions"
print("Fetching Session Info...")
session_data = requests.get(session_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/session_info.json", "w") as f:
    json.dump(session_data, f, indent=4)
session_info = session_data[0]
meeting_key = session_info.get('meeting_key')

# B. Drivers
driver_url = "https://api.openf1.org/v1/drivers"
print("Fetching Drivers...")
drivers_in_session = requests.get(driver_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/drivers.json", "w") as f:
    json.dump(drivers_in_session, f, indent=4)

# C. Session Results
session_result_url = "https://api.openf1.org/v1/session_result"
print("Fetching Session Results...")
session_result_info = requests.get(session_result_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/session_result.json", "w") as f:
    json.dump(session_result_info, f, indent=4)

# Force 'None' values to 999 so they sort to the bottom
sorted_results = sorted(session_result_info, key=lambda x: 999 if x.get('position') is None else x.get('position'))
race_winner = next((d['driver_number'] for d in sorted_results if d.get('position') == 1), None)
podium_drivers = [d['driver_number'] for d in sorted_results if d.get('position') in [1, 2, 3]]
valid_starting_grid_drivers = [d['driver_number'] for d in session_result_info if d['number_of_laps'] > 0 and d['dns'] is False]

# D. Laps & Fastest Lap Logic
laps_url = "https://api.openf1.org/v1/laps"
print("Fetching Laps...")
all_laps = requests.get(laps_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/laps.json", "w") as f:
    json.dump(all_laps, f, indent=4)

valid_laps = [l for l in all_laps if l.get('lap_duration') is not None]
fastest_lap_entry = min(valid_laps, key=lambda x: x['lap_duration']) if valid_laps else None

fastest_lap_info = {}
if fastest_lap_entry:
    fastest_lap_info = {
        "driver_number": fastest_lap_entry.get('driver_number'),
        "lap_number": fastest_lap_entry.get('lap_number'),
        "lap_time": fastest_lap_entry.get('lap_duration'),
        "sector_1": fastest_lap_entry.get('duration_sector_1'),
        "sector_2": fastest_lap_entry.get('duration_sector_2'),
        "sector_3": fastest_lap_entry.get('duration_sector_3')
    }

# E. Starting Grid
grid_url = "https://api.openf1.org/v1/starting_grid"
print("Fetching Starting Grid...")
full_starting_grid = requests.get(grid_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/starting_grid.json", "w") as f:
    json.dump(full_starting_grid, f, indent=4)

pole_entry = next((item for item in full_starting_grid if item.get('position') == 1), None)
pole_driver_num = pole_entry.get('driver_number') if pole_entry else drivers_in_session[0]['driver_number']

# F. Stints, Intervals, Positions
print("Fetching Stints, Intervals & Positions...")
stints_url = "https://api.openf1.org/v1/stints"
all_stints = requests.get(stints_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/stints.json", "w") as f:
    json.dump(all_stints, f, indent=4)
stints_df_raw = pd.DataFrame(all_stints)

intervals_url = "https://api.openf1.org/v1/intervals"
all_intervals = requests.get(intervals_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/intervals.json", "w") as f:
    json.dump(all_intervals, f, indent=4)
intervals_df_raw = pd.DataFrame(all_intervals)

pos_url = "https://api.openf1.org/v1/position"
all_positions = requests.get(pos_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/positions.json", "w") as f:
    json.dump(all_positions, f, indent=4)
positions_df_raw = pd.DataFrame(all_positions)


# ==========================================
# 2. Build Race Metadata
# ==========================================
first_lap_entry = next((item for item in all_laps if item["driver_number"] == pole_driver_num and item["lap_number"] == 1), None)
if first_lap_entry:
    start_dt_obj = datetime.fromisoformat(first_lap_entry['date_start']) - timedelta(seconds=20)
    start_dt_iso = start_dt_obj.isoformat()
else:
    start_dt_obj = datetime.now()
    start_dt_iso = start_dt_obj.isoformat()

race_metadata = {
    "session_key": SESSION_KEY,
    "reference_start_time": start_dt_iso,
    "race_winner": race_winner,
    "podium_drivers": podium_drivers,
    "fastest_lap": fastest_lap_info
}
with open(f"{OUTPUT_DIR}/race_metadata.json", "w") as f:
    json.dump(race_metadata, f, indent=4)


# ==========================================
# 2.5. Generate Track Layout + SECTORS
# ==========================================
print("Generating Track Layout & Sectors...")

track_layout = {
    "track_path": [],
    "pit_path": [],
    "sector_points": [], # Stores X,Y for start of S1, S2, S3
    "bounds": {"min_x": 0, "max_x": 0, "min_y": 0, "max_y": 0}
}

# Helper to find closest point by time
def find_closest_point(data, target_dt):
    # data must be sorted by 'date'
    closest = min(data, key=lambda x: abs((datetime.fromisoformat(x['date']) - target_dt).total_seconds()))
    return {"x": closest['x'], "y": closest['y']}

# 1. Main Path (Fastest Lap)
if fastest_lap_entry:
    fl_driver = fastest_lap_entry['driver_number']
    fl_start_str = fastest_lap_entry['date_start']
    fl_start_dt = datetime.fromisoformat(fl_start_str)
    
    # Calculate Sector Timestamps
    s1_dur = fastest_lap_entry.get('duration_sector_1')
    s2_dur = fastest_lap_entry.get('duration_sector_2')
    total_dur = fastest_lap_entry.get('lap_duration')
    
    fl_end_dt = fl_start_dt + timedelta(seconds=total_dur + 2) # Buffer
    fl_end_str = fl_end_dt.isoformat()
    
    print(f"   -> Fetching Racing Line (Driver {fl_driver})")
    
    loc_url = "https://api.openf1.org/v1/location"
    track_res = requests.get(loc_url, params={
        "session_key": SESSION_KEY, 
        "driver_number": fl_driver, 
        "date>": fl_start_str, "date<": fl_end_str
    })
    
    if track_res.status_code == 200:
        track_data = track_res.json()
        track_data.sort(key=lambda x: x['date'])
        
        # A. Fill Track Path
        for i, point in enumerate(track_data):
            if i % TRACK_MAP_DOWNSAMPLE == 0:
                track_layout["track_path"].append({"x": point['x'], "y": point['y']})
        
        # B. Identify Sector Gates (Start, End S1, End S2)
        if s1_dur and s2_dur:
            s1_end_dt = fl_start_dt + timedelta(seconds=s1_dur)
            s2_end_dt = s1_end_dt + timedelta(seconds=s2_dur)
            
            # Start Line (approximate start of lap)
            p_start = find_closest_point(track_data, fl_start_dt)
            # End of Sector 1
            p_s1 = find_closest_point(track_data, s1_end_dt)
            # End of Sector 2
            p_s2 = find_closest_point(track_data, s2_end_dt)
            
            track_layout["sector_points"] = [
                {"id": "Start/Finish", "x": p_start['x'], "y": p_start['y']},
                {"id": "Sector 1 End", "x": p_s1['x'], "y": p_s1['y']},
                {"id": "Sector 2 End", "x": p_s2['x'], "y": p_s2['y']}
            ]
            print(f"   -> Calculated Sector Gates.")

# 2. Pit Path
winner_stints = [s for s in all_stints if s['driver_number'] == race_winner]
pit_laps_found = False
if len(winner_stints) > 1:
    stint1 = winner_stints[0]
    in_lap_num = stint1['lap_end']
    out_lap_num = stint1['lap_end'] + 1
    
    in_lap_data = next((l for l in all_laps if l['driver_number'] == race_winner and l['lap_number'] == in_lap_num), None)
    out_lap_data = next((l for l in all_laps if l['driver_number'] == race_winner and l['lap_number'] == out_lap_num), None)
    
    if in_lap_data and out_lap_data:
        pit_start = in_lap_data['date_start']
        pit_end_dt = datetime.fromisoformat(out_lap_data['date_start']) + timedelta(seconds=out_lap_data['lap_duration'] + 5)
        pit_end = pit_end_dt.isoformat()
        
        print(f"   -> Fetching Pit Lane Geometry (Driver {race_winner})")
        pit_res = requests.get(loc_url, params={"session_key": SESSION_KEY, "driver_number": race_winner, "date>": pit_start, "date<": pit_end})
        
        if pit_res.status_code == 200:
            pit_data = pit_res.json()
            pit_data.sort(key=lambda x: x['date'])
            for i, point in enumerate(pit_data):
                if i % TRACK_MAP_DOWNSAMPLE == 0:
                    track_layout["pit_path"].append({"x": point['x'], "y": point['y']})
            pit_laps_found = True

# 3. Calculate Bounds
all_points = track_layout["track_path"] + track_layout["pit_path"]
if all_points:
    xs = [p['x'] for p in all_points]
    ys = [p['y'] for p in all_points]
    track_layout["bounds"] = {"min_x": min(xs), "max_x": max(xs), "min_y": min(ys), "max_y": max(ys)}

with open(f"{OUTPUT_DIR}/track_layout.json", "w") as f:
    json.dump(track_layout, f, indent=4)
print(f"   -> Track Layout Saved (Sectors Included).")


# ==========================================
# 3. Process Driver Telemetry
# ==========================================
print(f"Processing Drivers (Limit: {DRIVER_LIMIT})...")
count = 0
for driver in drivers_in_session:
    d_num = driver['driver_number']
    if d_num not in valid_starting_grid_drivers: continue
    if count >= DRIVER_LIMIT: break
    
    print(f"--- Processing Driver #{d_num} ---")
    
    # A. Fetch Telemetry
    loc_url = "https://api.openf1.org/v1/location"
    car_url = "https://api.openf1.org/v1/car_data"
    
    loc_res = requests.get(loc_url, params={"session_key": SESSION_KEY, "driver_number": d_num, "date>": start_dt_iso})
    car_res = requests.get(car_url, params={"session_key": SESSION_KEY, "driver_number": d_num, "date>": start_dt_iso})

    if loc_res.status_code != 200 or car_res.status_code != 200: continue
    loc_data, car_data = loc_res.json(), car_res.json()
    if not loc_data or not car_data: continue

    loc_df, car_df = pd.DataFrame(loc_data), pd.DataFrame(car_data)
    loc_df['date'] = pd.to_datetime(loc_df['date'], format='ISO8601')
    car_df['date'] = pd.to_datetime(car_df['date'], format='ISO8601')
    loc_df, car_df = loc_df.sort_values('date'), car_df.sort_values('date')

    merged_df = pd.merge_asof(loc_df, car_df, on='date', direction='nearest', suffixes=('', '_car'))

    # B. Intervals
    d_intervals = intervals_df_raw[intervals_df_raw['driver_number'] == d_num].copy()
    if not d_intervals.empty:
        d_intervals['date'] = pd.to_datetime(d_intervals['date'], format='ISO8601')
        d_intervals = d_intervals.sort_values('date')
        merged_df = pd.merge_asof(merged_df, d_intervals[['date', 'gap_to_leader', 'interval']], on='date', direction='backward')

    # C. Positions
    d_positions = positions_df_raw[positions_df_raw['driver_number'] == d_num].copy()
    if not d_positions.empty:
        d_positions['date'] = pd.to_datetime(d_positions['date'], format='ISO8601')
        d_positions = d_positions.sort_values('date')
        merged_df = pd.merge_asof(merged_df, d_positions[['date', 'position']], on='date', direction='backward')

    # D. Laps
    laps_df_raw = pd.DataFrame(all_laps)
    d_laps = laps_df_raw[laps_df_raw['driver_number'] == d_num].copy()
    lap_events = []
    for _, lap in d_laps.iterrows():
        try: t_start = pd.to_datetime(lap['date_start'], format='ISO8601')
        except: continue
        lap_events.append({'date': t_start, 'lap_number': lap['lap_number'], 'sector_1': np.nan, 'sector_2': np.nan, 'sector_3': np.nan, 'lap_time': np.nan})
        
        curr = t_start
        if pd.notna(lap.get('duration_sector_1')):
            curr += timedelta(seconds=lap['duration_sector_1'])
            lap_events.append({'date': curr, 'sector_1': lap['duration_sector_1']})
        if pd.notna(lap.get('duration_sector_2')):
            curr += timedelta(seconds=lap['duration_sector_2'])
            lap_events.append({'date': curr, 'sector_2': lap['duration_sector_2']})
        if pd.notna(lap.get('lap_duration')):
            end = t_start + timedelta(seconds=lap['lap_duration'])
            lap_events.append({'date': end, 'sector_3': lap['duration_sector_3'], 'lap_time': lap['lap_duration']})

    if lap_events:
        merged_df = pd.merge_asof(merged_df, pd.DataFrame(lap_events).sort_values('date'), on='date', direction='backward')

    # E. Stints
    d_stints = stints_df_raw[stints_df_raw['driver_number'] == d_num].copy()
    if not d_stints.empty and 'lap_number' in merged_df.columns:
        stint_map = []
        for _, s in d_stints.iterrows():
            for l in range(int(s['lap_start']), int(s['lap_end']) + 1):
                stint_map.append({'lap_number': l, 'compound': s['compound'], 'tyre_age': s['tyre_age_at_start'] + (l - s['lap_start'])})
        if stint_map:
            merged_df = pd.merge(merged_df, pd.DataFrame(stint_map).drop_duplicates('lap_number', keep='last'), on='lap_number', how='left')

    # F. Cleanup & Save
    cols = ['date', 'driver_number', 'x', 'y', 'speed', 'rpm', 'n_gear', 'throttle', 'brake', 'drs', 'gap_to_leader', 'interval', 'position', 'lap_number', 'sector_1', 'sector_2', 'sector_3', 'lap_time', 'compound', 'tyre_age']
    final = merged_df[[c for c in cols if c in merged_df.columns]].copy()
    
    final['time_offset'] = ((final['date'] - start_dt_obj).dt.total_seconds() * 1000).astype(int)
    final = final[['time_offset'] + [c for c in final.columns if c != 'time_offset']]
    final[final.select_dtypes(include=[np.number]).columns] = final.select_dtypes(include=[np.number]).fillna(0)
    final = final.fillna("")
    if 'date' in final.columns: final = final.drop(columns=['date'])
    
    out_path = f"{OUTPUT_DIR}/telemetry/driver_{d_num}.csv"
    final.to_csv(out_path, index=False)
    print(f"   -> Saved {len(final)} rows.")
    count += 1

print("\nProcessing Complete.")