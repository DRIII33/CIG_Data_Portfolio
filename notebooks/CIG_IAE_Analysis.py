# Title: CIG Data Analysis - Server Meshing vs Revenue
# Author: Daniel Rodriguez
# Description: Analysis of how Server Meshing stress tests impact IAE sales.

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load Data (Simulating loading the CSVs generated earlier)
df_server = pd.read_csv('cig_server_logs.csv')
df_pledges = pd.read_csv('cig_pledge_sales.csv')

# Convert timestamps
df_server['timestamp'] = pd.to_datetime(df_server['timestamp'])
df_pledges['timestamp'] = pd.to_datetime(df_pledges['timestamp'])

# --- ANALYSIS 1: Server Performance vs Player Count ---
plt.figure(figsize=(12, 6))
sns.scatterplot(data=df_server, x='player_count', y='interaction_delay_ms', hue='config_version', alpha=0.6)
plt.axhline(y=1000, color='r', linestyle='--', label='Unplayable Threshold (1000ms)')
plt.title('Server Meshing Stress Test: Player Count vs Interaction Delay')
plt.xlabel('Concurrent Players')
plt.ylabel('Interaction Delay (ms)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()

print("""
INSIGHT 1: The 'Dynamic_Mesh_V2_Test' config maintains lower delay until 
approx 600 players, after which delay spikes exponentially.
""")

# --- ANALYSIS 2: Revenue Impact of Crashes ---
# Resample both datasets to hourly
df_server.set_index('timestamp', inplace=True)
hourly_crashes = df_server.resample('H')['is_server_crash'].sum().reset_index()

df_pledges.set_index('timestamp', inplace=True)
hourly_revenue = df_pledges.resample('H')['pledge_amount_usd'].sum().reset_index()

# Merge
df_impact = pd.merge(hourly_revenue, hourly_crashes, on='timestamp')

# Calculate Correlation
corr = df_impact['pledge_amount_usd'].corr(df_impact['is_server_crash'])
print(f"Correlation between Server Crashes and Hourly Revenue: {corr:.2f}")

plt.figure(figsize=(14, 5))
sns.lineplot(data=df_impact, x='timestamp', y='pledge_amount_usd', label='Hourly Revenue ($)')
# Overlay crashes
ax2 = plt.twinx()
sns.barplot(data=df_impact, x='timestamp', y='is_server_crash', color='red', alpha=0.3, ax=ax2, label='Server Crashes')
plt.title(f'Hourly Revenue vs Server Crashes (Correlation: {corr:.2f})')
plt.show()
