/* Title: Server Health & Revenue Impact Analysis
  Author: Daniel Rodriguez
  Description: Audits server telemetry for crash rates and correlates 
               performance dips with revenue loss during IAE 2955.
*/

-- 1. DATA AUDIT: Identifying Malformed Logs
SELECT 
  shard_id,
  config_version,
  COUNT(*) AS malformed_entries,
  ROUND(MIN(interaction_delay_ms), 2) AS minimum_delay -- Confirms the negative value
FROM 
  `driiiportfolio.cig_data.server_logs`
WHERE 
  interaction_delay_ms < 0 -- Targeting the known anomaly
GROUP BY 
  1, 2
ORDER BY 
  malformed_entries DESC;
