import requests
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

# --- Configuration ---
SESSION_KEY = 9523
DRIVER_LIMIT = 22
SPEED_FACTOR = 2 

print(f"Fetching data for Session {SESSION_KEY}")

start_time = time.time()
# 1. Setup & Basic Info
session_url = "https://api.openf1.org/v1/sessions"
session_info = requests.get(session_url, params={"session_key": SESSION_KEY}).json()[0]
meeting_key = session_info.get('meeting_key')

driver_url = f"https://api.openf1.org/v1/drivers"
drivers_in_session = requests.get(driver_url, params={"session_key": SESSION_KEY}).json()

session_result_url = f"https://api.openf1.org/v1/session_result"
session_result_info = requests.get(session_result_url, params={"session_key": SESSION_KEY}).json()
winner_driver = [d['driver_number'] for d in session_result_info if d['position'] == 1][0]
valid_starting_grid = [d['driver_number'] for d in session_result_info if d['number_of_laps'] > 0 and d['dns'] is False]

# 2. Fetch Static Data (Laps, Intervals, Positions, Stints)
print("Fetching Laps, Intervals, Positions, and Stints...")
laps_url = "https://api.openf1.org/v1/laps"
all_laps = requests.get(laps_url, params={"session_key": SESSION_KEY}).json()
laps_df_raw = pd.DataFrame(all_laps)

intervals_url = "https://api.openf1.org/v1/intervals"
all_intervals = requests.get(intervals_url, params={"session_key": SESSION_KEY}).json()
intervals_df_raw = pd.DataFrame(all_intervals)

# --- NEW: Fetch Positions ---
pos_url = "https://api.openf1.org/v1/position"
all_positions = requests.get(pos_url, params={"session_key": SESSION_KEY}).json()
positions_df_raw = pd.DataFrame(all_positions)

# --- NEW: Fetch Stints ---
stints_url = "https://api.openf1.org/v1/stints"
all_stints = requests.get(stints_url, params={"session_key": SESSION_KEY}).json()
stints_df_raw = pd.DataFrame(all_stints)

# 3. Determine Start Time
grid_pos_url = f"https://api.openf1.org/v1/starting_grid"
pole_info = requests.get(grid_pos_url, params={"meeting_key": meeting_key, "position": 1}).json()
pole_driver_num = pole_info[0].get('driver_number') if pole_info else drivers_in_session[0]['driver_number']

first_lap_entry = next((item for item in all_laps if item["driver_number"] == pole_driver_num and item["lap_number"] == 1), None)
start_dt_obj = datetime.fromisoformat(first_lap_entry['date_start']) - timedelta(seconds=20)
start_dt = start_dt_obj.isoformat()

print(f"--- Fetched Session Info : {time.time() - start_time} seconds ---")

all_driver_data = []

print(f"Processing drivers (Limit: {DRIVER_LIMIT})...")
count = 0
for driver in drivers_in_session:
    d_num = driver['driver_number']

    if d_num not in valid_starting_grid:
        continue
    if count >= DRIVER_LIMIT:
        break
    
    print(f"--- Fetching Driver #{d_num} data ---")
    start_time = time.time()

    log("Fetching Location & Telemetry..")
    # --- A. Fetch Telemetry ---
    loc_url = f"https://api.openf1.org/v1/location"
    car_url = f"https://api.openf1.org/v1/car_data"
    
    loc_res = requests.get(loc_url, params={"session_key": SESSION_KEY, "driver_number": d_num, "date>": start_dt})
    car_res = requests.get(car_url, params={"session_key": SESSION_KEY, "driver_number": d_num, "date>": start_dt})

    log("Fetched Location & Telemetry..")
    
    log("Processing Location & Telemetry..")
    if loc_res.status_code == 200 and car_res.status_code == 200:
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

        merged_df = pd.merge_asof(loc_df, car_df, on='date', direction='nearest', suffixes=('', '_car'))

        # --- B. Process Intervals & POSITIONS ---
        # 1. Intervals
        d_intervals = intervals_df_raw[intervals_df_raw['driver_number'] == d_num].copy()
        if not d_intervals.empty:
            d_intervals['date'] = pd.to_datetime(d_intervals['date'], format='ISO8601')
            d_intervals = d_intervals.sort_values('date')
            d_intervals = d_intervals[['date', 'gap_to_leader', 'interval']]
            merged_df = pd.merge_asof(merged_df, d_intervals, on='date', direction='backward')

        # 2. Positions (NEW)
        d_positions = positions_df_raw[positions_df_raw['driver_number'] == d_num].copy()
        if not d_positions.empty:
            d_positions['date'] = pd.to_datetime(d_positions['date'], format='ISO8601')
            d_positions = d_positions.sort_values('date')
            d_positions = d_positions[['date', 'position']]
            merged_df = pd.merge_asof(merged_df, d_positions, on='date', direction='backward')

        log("Processed Location, Telemetry, Intervals & Positions")

        log("Processing Laps and Sectors..")
        # --- C. Process Laps & Sectors ---
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
            
            lap_events.append({
                'date': t_start,
                'lap_number': lap['lap_number'],
                'sector_1': np.nan,
                'sector_2': np.nan,
                'sector_3': np.nan,
                'lap_time': np.nan
            })
            
            current_time = t_start
            
            if s1 and not np.isnan(s1):
                current_time += timedelta(seconds=s1)
                lap_events.append({'date': current_time, 'sector_1': s1})
            
            if s2 and not np.isnan(s2):
                current_time += timedelta(seconds=s2)
                lap_events.append({'date': current_time, 'sector_2': s2})

            if total and not np.isnan(total):
                finish_time = t_start + timedelta(seconds=total)
                lap_events.append({'date': finish_time, 'sector_3': s3, 'lap_time': total})

        if lap_events:
            lap_events_df = pd.DataFrame(lap_events)
            lap_events_df = lap_events_df.sort_values('date')
            merged_df = pd.merge_asof(merged_df, lap_events_df, on='date', direction='backward')

        log("Processing Stints..")
        # --- D. Process Stints ---
        # Stints are mapped to Lap Numbers, so we merge AFTER we have 'lap_number' in merged_df
        d_stints = stints_df_raw[stints_df_raw['driver_number'] == d_num].copy()
        
        if not d_stints.empty and 'lap_number' in merged_df.columns:
            # Create a lookup table for every lap in the stint
            stint_map = []
            for _, stint in d_stints.iterrows():
                start_l = int(stint['lap_start'])
                end_l = int(stint['lap_end'])
                base_age = stint['tyre_age_at_start']
                compound = stint['compound']
                
                # Expand range to cover every lap in this stint
                for l_num in range(start_l, end_l + 1):
                    current_age = base_age + (l_num - start_l)
                    stint_map.append({
                        'lap_number': l_num,
                        'compound': compound,
                        'tyre_age': current_age
                    })

            if stint_map:
                stint_map_df = pd.DataFrame(stint_map)
                # If Stint 1 ends on Lap 10 and Stint 2 starts on Lap 10, keep the latest one.
                stint_map_df = stint_map_df.drop_duplicates(subset=['lap_number'], keep='last')
                # Merge onto the main dataframe based on lap_number
                merged_df = pd.merge(merged_df, stint_map_df, on='lap_number', how='left')

        log("Processed Laps, Sectors & Stints")

        log("Cleaning up..")
        # --- E. Final Cleanup ---
        # Added: position, compound, tyre_age
        cols_to_keep = ['date', 'driver_number', 'x', 'y', 'speed', 'rpm', 'n_gear', 
                        'throttle', 'brake', 'drs', 
                        'gap_to_leader', 'interval', 'position',
                        'lap_number', 'sector_1', 'sector_2', 'sector_3', 'lap_time',
                        'compound', 'tyre_age']
        
        existing_cols = [c for c in cols_to_keep if c in merged_df.columns]
        final_driver_df = merged_df[existing_cols]

        # This prevents the "Index contains duplicate entries" error in pivot
        final_driver_df = final_driver_df.drop_duplicates(subset=['date'])
        
        all_driver_data.append(final_driver_df)
        count += 1
        print(f"--- Fetched Driver #{d_num} data : {time.time() - start_time} seconds ---")

# 5. Combine and Pivot
if not all_driver_data:
    print("No data found.")
    exit()

df_long = pd.concat(all_driver_data)

values_to_pivot = [c for c in df_long.columns if c not in ['date', 'driver_number']]
df_full = df_long.pivot(index='date', columns='driver_number', values=values_to_pivot)

df_full.columns = [f"{col[0]}_{col[1]}" for col in df_full.columns]
df_full.reset_index(inplace=True)

# --- FIX: Exclude 'compound' from numeric conversion because it contains strings (SOFT, MEDIUM) ---
numeric_cols = [c for c in df_full.columns if c != 'date' and not c.startswith('compound_')]

df_full[numeric_cols] = df_full[numeric_cols].apply(pd.to_numeric, errors='coerce')

# Now fill gaps
df_full = df_full.ffill().fillna(0)

print("Data processing complete. Preparing animation...")
print(df_full.columns[:10]) 

# --- Visualization ---

driver_lookup = {str(d['driver_number']): d for d in drivers_in_session}

class RaceCar:
    def __init__(self, driver_number, ax):
        self.number = str(driver_number)
        meta = driver_lookup.get(self.number, {})
        self.abbr = meta.get('name_acronym', f"#{self.number}")
        raw_color = meta.get('team_colour', '555555')
        self.color = f"#{raw_color}"
        
        self.x = 0
        self.y = 0
        self.gap = 0
        self.lap = 0
        self.pos = 0
        
        self.ax = ax
        self.dot, = ax.plot([], [], 'o', color=self.color, markersize=8, zorder=5)
        self.label = ax.text(0, 0, self.abbr, fontsize=8, color=self.color, zorder=6, fontweight='bold')
        self.info_text = ax.text(0, 0, '', fontsize=7, color=self.color, zorder=6)

    def update_telemetry(self, row):
        if f'x_{self.number}' in row:
            self.x = row[f'x_{self.number}']
            self.y = row[f'y_{self.number}']
            
            # Force convert gap and lap to float, defaulting to 0 if conversion fails
            try:
                raw_gap = row.get(f'gap_to_leader_{self.number}', 0)
                self.gap = float(raw_gap)
            except (ValueError, TypeError):
                self.gap = 0.0

            try:
                raw_lap = row.get(f'lap_number_{self.number}', 0)
                self.lap = float(raw_lap)
            except (ValueError, TypeError):
                self.lap = 0.0

            try:
                raw_pos = row.get(f'position_{self.number}', 0)
                self.pos = int(raw_pos)
            except (ValueError, TypeError):
                self.pos = 0
        else:
            self.x = np.nan
            self.y = np.nan

    def update_visuals(self):
        if np.isnan(self.x) or np.isnan(self.y):
            self.dot.set_data([], [])
            self.label.set_text('')
            self.info_text.set_text('')
            return

        self.dot.set_data([self.x], [self.y])
        self.label.set_position((self.x + 120, self.y + 120))
        
        info_str = f"L{int(self.lap)}"
        if self.gap > 0:
            info_str += f" +{self.gap:.1f}s"
        
        info_str += f"\nPos: {self.pos}"
        
        self.info_text.set_text(info_str)
        self.info_text.set_position((self.x + 120, self.y - 120))

# Detect Drivers
active_driver_numbers = [col.split('_')[1] for col in df_full.columns if col.startswith('x_')]
unique_drivers = list(set(active_driver_numbers))

# Setup Plot
fig, ax = plt.subplots(figsize=(12, 8))
ax.set_facecolor('#1a1a1a')

# Track Map
if unique_drivers:
    ref_driver = winner_driver
    ax.plot(df_full[f'x_{ref_driver}'], df_full[f'y_{ref_driver}'], 
            color='#333333', linewidth=8, label='Track', zorder=1)

ax.set_aspect('equal')
ax.axis('off')

cars = [RaceCar(d_num, ax) for d_num in unique_drivers]
time_text = ax.text(0.05, 0.95, '', transform=ax.transAxes, color='white', fontsize=14, fontfamily='monospace')

def init():
    if unique_drivers:
        ref_x = df_full[f'x_{unique_drivers[0]}']
        ref_y = df_full[f'y_{unique_drivers[0]}']
        ax.set_xlim(ref_x.min() - 1500, ref_x.max() + 1500)
        ax.set_ylim(ref_y.min() - 1500, ref_y.max() + 1500)
    return [c.dot for c in cars] + [c.label for c in cars] + [c.info_text for c in cars] + [time_text]

def update(frame_idx):
    row = df_full.iloc[frame_idx]
    artists = []
    
    for car in cars:
        car.update_telemetry(row)
        car.update_visuals()
        artists.extend([car.dot, car.label, car.info_text])
    
    current_time = row['date']
    time_text.set_text(f"{current_time.strftime('%H:%M:%S')}")
    artists.append(time_text)
    
    return artists

frames = range(0, len(df_full), SPEED_FACTOR)

ani = FuncAnimation(fig, update, frames=frames, init_func=init, blit=True, interval=50)

plt.tight_layout()
plt.show()