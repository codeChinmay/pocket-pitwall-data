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

# Ensure output directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "telemetry"), exist_ok=True)

print(f"Fetching data for Session {SESSION_KEY}")
print(f"Output directory: {OUTPUT_DIR}/")

start_process_time = time.time()

# ==========================================
# 1. Fetch & Store Static Data "As Is"
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

# C. Session Results (Winner & Podium)
session_result_url = "https://api.openf1.org/v1/session_result"
print("Fetching Session Results...")
session_result_info = requests.get(session_result_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/session_result.json", "w") as f:
    json.dump(session_result_info, f, indent=4)

# Logic: Extract Winner and Podium
sorted_results = sorted(session_result_info, key=lambda x: x.get('position', 999))
race_winner = next((d['driver_number'] for d in sorted_results if d.get('position') == 1), None)
podium_drivers = [d['driver_number'] for d in sorted_results if d.get('position') in [1, 2, 3]]
valid_starting_grid_drivers = [d['driver_number'] for d in session_result_info if d['number_of_laps'] > 0 and d['dns'] is False]

# D. Laps (Fastest Lap Logic)
laps_url = "https://api.openf1.org/v1/laps"
print("Fetching Laps...")
all_laps = requests.get(laps_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/laps.json", "w") as f:
    json.dump(all_laps, f, indent=4)
laps_df_raw = pd.DataFrame(all_laps)

# Logic: Find Fastest Lap with Sectors
# Filter out laps with no duration
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

# E. Starting Grid (Full Fetch)
grid_url = "https://api.openf1.org/v1/starting_grid"
print("Fetching Starting Grid...")
# Fetching ALL grid positions for this session
full_starting_grid = requests.get(grid_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/starting_grid.json", "w") as f:
    json.dump(full_starting_grid, f, indent=4)

# Logic: Find Pole Position for Time Reference
pole_entry = next((item for item in full_starting_grid if item.get('position') == 1), None)
pole_driver_num = pole_entry.get('driver_number') if pole_entry else drivers_in_session[0]['driver_number']


# F. Intervals, Positions, Stints
print("Fetching Intervals, Positions, Stints...")
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

stints_url = "https://api.openf1.org/v1/stints"
all_stints = requests.get(stints_url, params={"session_key": SESSION_KEY}).json()
with open(f"{OUTPUT_DIR}/stints.json", "w") as f:
    json.dump(all_stints, f, indent=4)
stints_df_raw = pd.DataFrame(all_stints)


# ==========================================
# 2. Build Race Metadata & Reference Time
# ==========================================

# Determine Start Time (Zero Reference)
first_lap_entry = next((item for item in all_laps if item["driver_number"] == pole_driver_num and item["lap_number"] == 1), None)
if first_lap_entry:
    start_dt_obj = datetime.fromisoformat(first_lap_entry['date_start']) - timedelta(seconds=20)
    start_dt_iso = start_dt_obj.isoformat()
else:
    # Fallback if no lap data found for pole sitter
    start_dt_obj = datetime.now() 
    start_dt_iso = start_dt_obj.isoformat()
    print("WARNING: Could not determine start time from pole sitter.")

race_metadata = {
    "session_key": SESSION_KEY,
    "reference_start_time": start_dt_iso,
    "race_winner": race_winner,
    "podium_drivers": podium_drivers,
    "fastest_lap": fastest_lap_info
}

with open(f"{OUTPUT_DIR}/race_metadata.json", "w") as f:
    json.dump(race_metadata, f, indent=4)

print(f"--- Metadata Saved. Winner: {race_winner}, Fastest Lap: {fastest_lap_info.get('lap_time')}s ---")


# ==========================================
# 3. Process Driver Telemetry
# ==========================================
print(f"Processing Drivers (Limit: {DRIVER_LIMIT})...")

count = 0
for driver in drivers_in_session:
    d_num = driver['driver_number']

    if d_num not in valid_starting_grid_drivers:
        continue
    if count >= DRIVER_LIMIT:
        break
    
    print(f"--- Processing Driver #{d_num} ---")
    
    # --- A. Fetch Telemetry ---
    loc_url = "https://api.openf1.org/v1/location"
    car_url = "https://api.openf1.org/v1/car_data"
    
    # Fetch with buffer
    loc_res = requests.get(loc_url, params={"session_key": SESSION_KEY, "driver_number": d_num, "date>": start_dt_iso})
    car_res = requests.get(car_url, params={"session_key": SESSION_KEY, "driver_number": d_num, "date>": start_dt_iso})

    if loc_res.status_code != 200 or car_res.status_code != 200:
        continue

    loc_data = loc_res.json()
    car_data = car_res.json()
    
    if not loc_data or not car_data:
        continue

    loc_df = pd.DataFrame(loc_data)
    car_df = pd.DataFrame(car_data)

    loc_df['date'] = pd.to_datetime(loc_df['date'], format='ISO8601')
    car_df['date'] = pd.to_datetime(car_df['date'], format='ISO8601')
    
    loc_df = loc_df.sort_values('date')
    car_df = car_df.sort_values('date')

    # Merge Location & Car
    merged_df = pd.merge_asof(loc_df, car_df, on='date', direction='nearest', suffixes=('', '_car'))

    # --- B. Intervals ---
    d_intervals = intervals_df_raw[intervals_df_raw['driver_number'] == d_num].copy()
    if not d_intervals.empty:
        d_intervals['date'] = pd.to_datetime(d_intervals['date'], format='ISO8601')
        d_intervals = d_intervals.sort_values('date')
        d_intervals = d_intervals[['date', 'gap_to_leader', 'interval']]
        merged_df = pd.merge_asof(merged_df, d_intervals, on='date', direction='backward')

    # --- C. Positions ---
    d_positions = positions_df_raw[positions_df_raw['driver_number'] == d_num].copy()
    if not d_positions.empty:
        d_positions['date'] = pd.to_datetime(d_positions['date'], format='ISO8601')
        d_positions = d_positions.sort_values('date')
        d_positions = d_positions[['date', 'position']]
        merged_df = pd.merge_asof(merged_df, d_positions, on='date', direction='backward')

    # --- D. Laps & Sectors ---
    d_laps = laps_df_raw[laps_df_raw['driver_number'] == d_num].copy()
    lap_events = []
    
    for _, lap in d_laps.iterrows():
        try:
            t_start = pd.to_datetime(lap['date_start'], format='ISO8601')
        except:
            continue 
        
        s1 = lap.get('duration_sector_1')
        s2 = lap.get('duration_sector_2')
        s3 = lap.get('duration_sector_3')
        total = lap.get('lap_duration')
        
        # Start of Lap Event
        lap_events.append({
            'date': t_start,
            'lap_number': lap['lap_number'],
            'sector_1': np.nan, 'sector_2': np.nan, 'sector_3': np.nan, 'lap_time': np.nan
        })
        
        current_time = t_start
        
        # Sector 1 End Event
        if s1 and not np.isnan(s1):
            current_time += timedelta(seconds=s1)
            lap_events.append({'date': current_time, 'sector_1': s1})
        
        # Sector 2 End Event
        if s2 and not np.isnan(s2):
            current_time += timedelta(seconds=s2)
            lap_events.append({'date': current_time, 'sector_2': s2})

        # Sector 3 End Event (Lap Finish)
        if total and not np.isnan(total):
            finish_time = t_start + timedelta(seconds=total)
            lap_events.append({'date': finish_time, 'sector_3': s3, 'lap_time': total})

    if lap_events:
        lap_events_df = pd.DataFrame(lap_events)
        lap_events_df = lap_events_df.sort_values('date')
        merged_df = pd.merge_asof(merged_df, lap_events_df, on='date', direction='backward')

    # --- E. Stints ---
    d_stints = stints_df_raw[stints_df_raw['driver_number'] == d_num].copy()
    
    if not d_stints.empty and 'lap_number' in merged_df.columns:
        stint_map = []
        for _, stint in d_stints.iterrows():
            start_l = int(stint['lap_start'])
            end_l = int(stint['lap_end'])
            base_age = stint['tyre_age_at_start']
            compound = stint['compound']
            
            for l_num in range(start_l, end_l + 1):
                current_age = base_age + (l_num - start_l)
                stint_map.append({
                    'lap_number': l_num,
                    'compound': compound,
                    'tyre_age': current_age
                })

        if stint_map:
            stint_map_df = pd.DataFrame(stint_map)
            stint_map_df = stint_map_df.drop_duplicates(subset=['lap_number'], keep='last')
            merged_df = pd.merge(merged_df, stint_map_df, on='lap_number', how='left')

    # --- F. Final Cleanup & Serialization ---
    
    cols_to_keep = [
        'date', 'driver_number', 'x', 'y', 'speed', 'rpm', 'n_gear', 
        'throttle', 'brake', 'drs', 
        'gap_to_leader', 'interval', 'position',
        'lap_number', 'sector_1', 'sector_2', 'sector_3', 'lap_time',
        'compound', 'tyre_age'
    ]
    
    existing_cols = [c for c in cols_to_keep if c in merged_df.columns]
    final_driver_df = merged_df[existing_cols].copy()

    # Calculate Time Offset (Milliseconds)
    final_driver_df['time_offset'] = (final_driver_df['date'] - start_dt_obj).dt.total_seconds() * 1000
    final_driver_df['time_offset'] = final_driver_df['time_offset'].astype(int)

    # Reorder columns
    final_cols = ['time_offset'] + [c for c in final_driver_df.columns if c != 'time_offset']
    final_driver_df = final_driver_df[final_cols]

    # Fill NaNs
    numeric_cols = final_driver_df.select_dtypes(include=[np.number]).columns
    final_driver_df[numeric_cols] = final_driver_df[numeric_cols].fillna(0)
    final_driver_df = final_driver_df.fillna("")

    # Save to CSV (dropping 'date' to save space on M5Stack)
    output_path = f"{OUTPUT_DIR}/telemetry/driver_{d_num}.csv"
    if 'date' in final_driver_df.columns:
        final_driver_df = final_driver_df.drop(columns=['date'])

    final_driver_df.to_csv(output_path, index=False)
    
    count += 1
    print(f"   -> Saved {len(final_driver_df)} rows to {output_path}")

print("\nProcessing Complete.")