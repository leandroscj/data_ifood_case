SELECT
    competencia,
    CAST(AVG(total_amount) as decimal(10,2)) AS media_total_amount
FROM spec.ny_taxi_consumers
WHERE vehicle = 'yellow'
GROUP BY competencia
ORDER BY competencia;