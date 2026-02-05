import pandas as pd
import glob
import os

# CONFIG
SESSION_KEY = 9523
DATA_DIR = f"race_data_{SESSION_KEY}/telemetry"

print(f"--- INSPECTING CSV DATA ({DATA_DIR}) ---")

csv_files = glob.glob(f"{DATA_DIR}/*.csv")
if not csv_files:
    print("No CSV files found!")
    exit()

for f in csv_files:
    d_id = os.path.basename(f)
    df = pd.read_csv(f)
    
    if 'time_offset' not in df.columns:
        print(f"{d_id}: ERROR - No 'time_offset' column")
        continue
        
    count = len(df)
    t_min = df['time_offset'].min()
    t_max = df['time_offset'].max()
    duration_sec = (t_max - t_min) / 1000
    duration_min = duration_sec / 60
    
    print(f"{d_id:<15} | Rows: {count:<6} | Max Offset: {t_max:<8} ms | Duration: {duration_min:.2f} min")

print("-" * 50)