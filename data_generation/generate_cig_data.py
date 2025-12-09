import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_cig_datasets():
    np.random.seed(42)
    
    # --- 1. Server Logs (Telemetry) ---
    # Context: Simulating "Server Meshing" tests during IAE 2955 (Nov 2025)
    # High player counts (300-800 per shard) causing Interaction Delay
    start_date = datetime(2025, 11, 14, 12, 0, 0)
    timestamps = [start_date + timedelta(minutes=5*i) for i in range(2000)] # 1 week of 5-min logs
    
    server_data = []
    shards = ['shard_us_east_1', 'shard_eu_central_1', 'shard_asia_east_1']
    configs = ['Static_Mesh_V1', 'Dynamic_Mesh_V2_Test']

    for ts in timestamps:
        for shard in shards:
            # Simulate daily peak times (18:00 - 22:00)
            is_peak = 18 <= ts.hour <= 22
            base_load = 400 if is_peak else 150
            
            # Metric Generation
            player_count = int(np.random.normal(base_load, 50))
            player_count = max(0, player_count)
            
            # Server FPS drops as player count rises (Target is 30, critical is <5)
            server_fps = 30 - (player_count / 25) + np.random.normal(0, 2)
            server_fps = max(1, min(30, server_fps))
            
            # Interaction Delay (ms): Ideally <100ms. Meshing tests spike to 1000ms+
            delay_ms = (player_count * 1.5) + np.random.normal(0, 50)
            if server_fps < 5: 
                delay_ms += 2000 # Lag spike
            
            # Crash Event (30k error)
            is_crash = 1 if (server_fps < 4 and random.random() < 0.1) else 0
            
            server_data.append({
                'log_id': f"LOG_{random.randint(100000, 999999)}",
                'timestamp': ts,
                'shard_id': shard,
                'config_version': random.choice(configs),
                'player_count': player_count,
                'server_fps': round(server_fps, 2),
                'interaction_delay_ms': round(delay_ms, 2),
                'is_server_crash': is_crash
            })
            
    df_server = pd.DataFrame(server_data)

    # --- 2. Pledge Transactions (Sales) ---
    # Context: Sales of ships during IAE event.
    # If server crashes, sales might drop in the following hour.
    
    ships = [
        ('RSI Polaris', 750), ('Aegis Idris-P', 1500), ('Drake Cutter', 45),
        ('C1 Spirit', 125), ('Gatac Syulen', 80), ('Anvil Carrack', 600)
    ]
    
    pledge_data = []
    # Generate 5000 transactions
    for _ in range(5000):
        # Random time within the week
        tx_time = start_date + timedelta(seconds=random.randint(0, 7*24*3600))
        
        ship_choice = random.choices(ships, weights=[5, 2, 40, 25, 20, 8])[0]
        
        pledge_data.append({
            'transaction_id': f"TX_{random.randint(1000000,9999999)}",
            'account_id': random.randint(1000, 5000),
            'timestamp': tx_time,
            'sku_name': ship_choice[0],
            'pledge_amount_usd': ship_choice[1],
            'payment_status': 'COMPLETED'
        })
        
    df_pledges = pd.DataFrame(pledge_data)
    
    # Export for BigQuery upload
    df_server.to_csv('cig_server_logs.csv', index=False)
    df_pledges.to_csv('cig_pledge_sales.csv', index=False)
    print("Files generated: cig_server_logs.csv, cig_pledge_sales.csv")

if __name__ == "__main__":
    generate_cig_datasets()
