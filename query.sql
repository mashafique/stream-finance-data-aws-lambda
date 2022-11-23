SELECT name as Company, max(High) AS Hourly_High, SUBSTRING(ts, 12, 2) AS Hour
FROM mohammad_project03
GROUP BY name, SUBSTRING(ts, 12, 2)
ORDER BY name