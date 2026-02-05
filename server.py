from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import os
import json
import asyncio
import glob

print("--- F1 TELEMETRY SERVER v2.1 (FINAL) ---")

app = FastAPI()

# --- CONFIG ---
DATA_ROOT = "." 
FRAME_INTERVAL = 0.1 # 100ms (10 FPS)

class SessionManager:
    def __init__(self, session_key):
        self.session_key = session_key
        self.base_path = f"{DATA_ROOT}/race_data_{session_key}"
        self.drivers_data = {} 
        self.max_time = 0
        self.load_data()

    def load_data(self):
        print(f"Loading Session {self.session_key}...")
        telemetry_path = f"{self.base_path}/telemetry"
        
        csv_files = glob.glob(f"{telemetry_path}/*.csv")
        for f in csv_files:
            try:
                d_id = int(os.path.basename(f).split('_')[1].split('.')[0])
                df = pd.read_csv(f)
                
                if 'time_offset' not in df.columns: continue

                # 1. Determine local max time (Crucial: Use the VALUE, not the count)
                local_max = df['time_offset'].max()
                if local_max > self.max_time: self.max_time = local_max

                # 2. Reindex to 100ms grid (Forward Fill)
                # This ensures we have a row for every 0.1s tick
                df = df.set_index('time_offset')
                full_idx = range(0, int(local_max) + 1, int(FRAME_INTERVAL * 1000))
                df = df.reindex(full_idx, method='ffill')
                df = df.fillna(0) # Fill start gaps
                
                self.drivers_data[d_id] = df
                
            except Exception as e:
                print(f"Error loading {f}: {e}")
        
        # LOGGING: Verify this says ~104 minutes
        print(f"Loaded {len(self.drivers_data)} drivers. Max time: {self.max_time/1000/60:.2f} min")

active_sessions = {}

def get_session(session_key: str):
    if session_key not in active_sessions:
        if not os.path.exists(f"{DATA_ROOT}/race_data_{session_key}"): return None
        active_sessions[session_key] = SessionManager(session_key)
    return active_sessions[session_key]

@app.get("/session/{session_key}/{file_type}")
def get_static_data(session_key: str, file_type: str):
    path = f"{DATA_ROOT}/race_data_{session_key}/{file_type}.json"
    if not os.path.exists(path): raise HTTPException(status_code=404, detail="File not found")
    with open(path, "r") as f: return JSONResponse(content=json.load(f))

@app.websocket("/ws/{session_key}")
async def websocket_endpoint(websocket: WebSocket, session_key: str):
    await websocket.accept()
    session = get_session(session_key)
    if not session:
        await websocket.close(code=4004)
        return

    print(f"Client connected: {session_key} (Max Duration: {session.max_time} ms)")
    t = 0
    step = int(FRAME_INTERVAL * 1000)
    
    try:
        while True:
            # 1. Send Telemetry
            if t <= session.max_time:
                msg = [str(t)]
                for d_id, df in session.drivers_data.items():
                    if t in df.index:
                        row = df.loc[t]
                        if pd.notna(row['x']):
                            s = f"{d_id},{int(row['x'])},{int(row['y'])},{int(row['position'])}"
                            msg.append(s)
                
                if len(msg) > 1: await websocket.send_text("|".join(msg))
                t += step
            
            # 2. Race Over - HOLD CONNECTION (Do not disconnect)
            else:
                # print("Race finished. Holding...") 
                await websocket.send_text(f"{session.max_time}|FINISHED")
                await asyncio.sleep(1.0) # Slow heartbeat
                continue

            await asyncio.sleep(FRAME_INTERVAL)
            
    except Exception as e:
        print(f"Client disconnected: {e}")

if __name__ == "__main__":
    import uvicorn
    # Use 0.0.0.0 to allow external connections
    uvicorn.run(app, host="0.0.0.0", port=8000)