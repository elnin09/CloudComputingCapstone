/*insert overwrite local directory '/queryoutput/output1_1.csv'*/


/*query 1.1*/
SELECT value, count(*) as popcount
FROM ( 
   SELECT Origin as value
   FROM airportdata

   UNION ALL

   SELECT dest as value
   FROM airportdata
) t
GROUP BY value
ORDER BY popcount DESC
LIMIT 10;


/*query 1.2*/
SELECT UniqueCarrier, avg(ArrDelay) as avgdelay from airportdata
GROUP BY UniqueCarrier
ORDER BY avgdelay ASC
LIMIT 10;


/*query 1.3*/
SELECT DayOfWeek, avg(ArrDelay) as avgdelay from airportdata
GROUP BY DayOfWeek
ORDER BY avgdelay ASC;



