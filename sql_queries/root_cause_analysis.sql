-- 3. ROOT CAUSE ANALYSIS (RCA): Crash Metrics Summary
SELECT
  config_version,
  ROUND(AVG(player_count), 0) AS avg_players_at_crash,
  ROUND(AVG(server_fps), 2) AS avg_fps_at_crash,
  ROUND(AVG(interaction_delay_ms), 2) AS avg_delay_at_crash,
  COUNT(*) AS crash_count
FROM
  `driiiportfolio.cig_data.server_logs`
WHERE
  is_server_crash = 1 -- Focus only on the crash events
GROUP BY
  config_version
ORDER BY
  crash_count DESC;
