- 4. REVENUE IMPACT ANALYSIS: Calculating Sales Loss Post-Crash
SELECT
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', t1.crash_timestamp) AS crash_time_utc,
  t1.shard_id,
  SUM(t2.pledge_amount_usd) AS revenue_post_crash,
  COUNT(t2.transaction_id) AS sales_count
FROM
(
  -- Select all crash event timestamps and shard IDs
  SELECT 
    TIMESTAMP(timestamp) AS crash_timestamp,
    shard_id
  FROM 
    `driiiportfolio.cig_data.server_logs`
  WHERE 
    is_server_crash = 1
) AS t1
INNER JOIN 
  `driiiportfolio.cig_data.pledge_sales` AS t2 
ON
  t2.timestamp > t1.crash_timestamp -- Sales occurred after the crash
  AND t2.timestamp <= TIMESTAMP_ADD(t1.crash_timestamp, INTERVAL 60 MINUTE) -- Sales occurred within 60 minutes
GROUP BY
  1, 2
ORDER BY
  revenue_post_crash DESC;
