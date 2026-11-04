SET search_path TO payflow;

-- 1. Daily USD volume and settlement rate per corridor, with a 3-day rolling volume
WITH daily AS (
    SELECT created_date, corridor_key,
           COUNT(*)                                        AS payments,
           SUM(amount_usd)                                 AS volume_usd,
           AVG((status = 'SETTLED')::int)::numeric(5,4)    AS settle_rate
    FROM fact_payments
    GROUP BY created_date, corridor_key
)
SELECT *,
       SUM(volume_usd) OVER (PARTITION BY corridor_key ORDER BY created_date
                             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS volume_3d
FROM daily
ORDER BY volume_usd DESC
LIMIT 20;

-- 2. Settlement latency percentiles by rail
SELECT rail,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY settlement_seconds) / 60 AS p50_min,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY settlement_seconds) / 60 AS p95_min
FROM fact_payments
WHERE status = 'SETTLED'
GROUP BY rail
ORDER BY p95_min DESC;

-- 3. Open reconciliation breaks by type and day
SELECT recon_date, recon_status, COUNT(*) AS breaks, SUM(amount_diff) AS total_diff
FROM recon_breaks
WHERE recon_status NOT IN ('MATCHED')
GROUP BY recon_date, recon_status
ORDER BY recon_date, recon_status;

-- 4. Data-quality trend: failed checks per run
SELECT run_date, layer, COUNT(*) FILTER (WHERE NOT passed) AS failed_checks
FROM dq_results
GROUP BY run_date, layer
ORDER BY run_date, layer;
