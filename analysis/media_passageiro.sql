SELECT
    HOUR(tpep_pickup_datetime)  AS hora_do_dia,
    AVG(passenger_count)        AS media_passageiros
FROM spec.ny_taxi_consumers
WHERE competencia = 202305
GROUP BY hora_do_dia
ORDER BY hora_do_dia;