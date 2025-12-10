-- 2. A/B TEST ANALYSIS: Comparing Server Mesh Performance Metrics
SELECT
  config_version,
  COUNT(log_id) AS total_logs,
  ROUND(AVG(player_count), 0) AS avg_player_count,
  ROUND(AVG(server_fps), 2) AS avg_server_fps,
  ROUND(AVG(interaction_delay_ms), 2) AS avg_interaction_delay_ms,
  SUM(is_server_crash) AS total_crashes
FROM
  `driiiportfolio.cig_data.server_logs`
GROUP BY
  config_version
ORDER BY
  total_crashes DESC;
