#INSTALL PACKAGES
!pip install pandas
----------------------------------------
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

            # --- UPDATED METRIC GENERATION ---
            # 1. Player Count: Increased variance to push servers to limits
            # Base load 400 (peak) or 150 (off-peak).
            player_count = int(np.random.normal(base_load, 75))
            player_count = max(0, player_count) # Ensure no negative players

            # 2. Server FPS: Made more sensitive to player count
            # Divisor lowered from 25 to 12. This ensures FPS drops faster as players increase.
            # Example: 400 players -> 30 - (400/12) = -3.3 -> Clamped to 1 FPS
            server_fps = 30 - (player_count / 12) + np.random.normal(0, 2)
            server_fps = max(1, min(30, server_fps))

            # 3. Interaction Delay: Prevent Negative Values
            # Added max(0, ...) to fix the "Negative Delay" bug, OR leave it
            # if you want to filter it out in SQL as part of the "Audit" demo.
            # We will generate HIGH delay for low FPS to simulate "Server Meshing" lag.
            base_delay = (player_count * 2)
            noise = np.random.normal(0, 50)

            # If FPS is critical (< 10), delay spikes exponentially
            if server_fps < 10:
                delay_ms = base_delay + 1500 + noise # Huge spike
            else:
                delay_ms = base_delay + noise

            # 4. Crash Event: Increased Probability
            # Threshold raised to < 8 FPS (was < 4) and probability increased to 15%
            is_crash = 1 if (server_fps < 8 and random.random() < 0.15) else 0

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
----------------------------------------
import pandas as pd
df_server = pd.read_csv('cig_server_logs.csv')
df_server['is_server_crash'].sum()
