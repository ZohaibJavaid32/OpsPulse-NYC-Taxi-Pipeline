use NYCTaxiAnalytics;

SELECT COUNT(*) FROM NYCTaxiAnalytics.dbo.taxi_trips;

SELECT * FROM NYCTaxiAnalytics.dbo.taxi_zones;

TRUNCATE TABLE taxi_trips;
DROP TABLE taxi_trips;
SELECT TOP 10 * FROM taxi_trips;


SELECT pickup_day, 
	   SUM(total_amount) AS total_revenue
FROM taxi_trips
GROUP BY pickup_day
ORDER BY total_revenue DESC;

SELECT pickup_hour,
		SUM(total_amount) AS total_revenue
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY total_revenue DESC;


SELECT 
     pickup_day, 
	 pickup_hour,
	 COUNT(*) AS total_trips
FROM taxi_trips
GROUP BY pickup_day , pickup_hour
ORDER BY total_trips DESC;


SELECT 
     pickup_day, 
	 pickup_hour,
	 COUNT(*) AS total_trips
FROM taxi_trips
GROUP BY pickup_day , pickup_hour
ORDER BY total_trips ASC;



	
