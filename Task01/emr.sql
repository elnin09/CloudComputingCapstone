
create database airportdatabase;

use airportdatabase;

CREATE TABLE airportdata 
  ( 
    Year int, 
    Quarter int, 
    Month int, 
    DayofMonth int , 
    DayOfWeek   int, 
    FlightDate string,
    UniqueCarrier string,
    AirlineID int,
    FlightNum int,
    Origin string,
    Dest string, 
    DepTime int,
    DepDelay double, 
    ArrTime int, 
    ArrDelay double
  ) 
row format delimited fields terminated BY ',' lines terminated BY '\n' 
tblproperties("skip.header.line.count"="1"); 

load data  inpath '/tmp/refined_combined_data.csv.gz' into table airportdata; 


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


SELECT UniqueCarrier, avg(ArrDelay) as avgdelay from airportdata
GROUP BY UniqueCarrier
ORDER BY avgdelay ASC
LIMIT 10;


SELECT DayOfWeek, avg(ArrDelay) as avgdelay from airportdata
GROUP BY DayOfWeek
ORDER BY avgdelay ASC;


create table output2_1  as 
select origin, UniqueCarrier, DepartureDelay
    from 
    (
          select origin, UniqueCarrier, DepartureDelay,
        rank() over (partition by origin order by DepartureDelay asc) as RowNum
        FROM 
        (
          SELECT origin,UniqueCarrier,avg(DepDelay) as DepartureDelay
          FROM airportdata
          GROUP BY origin,UniqueCarrier 
        )a

    )t
    WHERE RowNum < 11
    order by origin asc;



/*
insert overwrite local directory '/queryoutput/output2_2.csv'*/
create table output2_2  as 
select origin, dest, DepartureDelay
    from 
    (
          select origin, dest, DepartureDelay,
        rank() over (partition by origin order by DepartureDelay asc) as RowNum
        FROM 
        (
          SELECT origin,dest,avg(DepDelay) as DepartureDelay
          FROM airportdata
          GROUP BY origin,dest 
        )a

    )t
    WHERE RowNum < 11
    order by origin asc;






create table output2_3  as 
select origin,dest,uniquecarrier,ArrivalDelay
    from 
    (
          select origin, dest, ArrivalDelay,uniquecarrier, 
        rank() over (partition by origin,dest order by ArrivalDelay asc) as RowNum
        FROM 
        (
          SELECT origin,dest,uniquecarrier, avg(arrDelay) as ArrivalDelay
          FROM airportdata
          GROUP BY origin,dest,uniquecarrier 
        )a

    )t
    WHERE RowNum < 11
    order by origin asc,dest asc;


create table output2_4  as 
 SELECT origin,dest,avg(arrDelay) as ArrivalDelay
     FROM airportdata
     GROUP BY origin,dest
     ORDER BY origin asc,dest asc;




create table FirstLeg  as 
select distinct origin,dest as destination,cast(flightdate as date) as flightdate,arrDelay as ArrivalDelay,UniqueCarrier,flightnum,deptime as departuretime from 
(
  Select origin,dest,flightdate,arrDelay,UniqueCarrier,flightnum,deptime,
  rank() over (partition by origin,dest,flightdate order by arrDelay asc) as rownumber
  from 
  (select * from airportdata 
  where deptime <1200 and airportdata.year = 2008
  )b

)t

WHERE rownumber = 1
;

create table SecondLeg  as 
select distinct origin,dest as destination,cast(flightdate as date) as flightdate,arrDelay as ArrivalDelay,UniqueCarrier,flightnum,deptime as departuretime from 
(
  Select origin,dest,flightdate,arrDelay,UniqueCarrier,flightnum,deptime,
  rank() over (partition by origin,dest,flightdate order by arrDelay asc) as rownumber
  from 
  (select * from airportdata 
  where deptime >1200 and airportdata.year = 2008 
  )b

)t

WHERE rownumber = 1
;


/*origin,destination,flightdate,ArrivalDelay,UniqueCarrier,flightnum,departuretime*/

create table output3_2 as
select T1.origin as origin1, T1.Destination as destination1, T2.origin as origin2, T2.destination as destination2,
 T1.flightdate as Flightdate1,T2.flightdate as flightdate2,
 T1.ArrivalDelay as ArrivalDelay1, T2.ArrivalDelay as ArrivalDelay2,
 T1.UniqueCarrier as UniqueCarrier1 , T1.flightnum as FlightNumner1,
 T2.UniqueCarrier as UniqueCarrier2 , T2.flightnum as FlightNumner2,
 T1.departuretime as DepartureTime1, T2.departuretime as DepartureTime2 
 from FirstLeg T1, Secondleg T2
 where T1.destination = T2.origin and datediff(T2.flightdate,T1.Flightdate) = 2;

  /*
 and 
 (
     (T1.origin = 'CMI' and T1.destination = 'ORD' and T2.destination = 'LAX')
     OR
     (T1.origin = 'JAX' and T1.destination = 'DFW' and T2.destination = 'CRP')
     OR
     (T1.origin = 'SLC' and T1.destination = 'BFL' and T2.destination = 'LAX')
     OR
     (T1.origin = 'LAX' and T1.destination = 'SFO' and T2.destination = 'PHX')
     OR
     (T1.origin = 'DFW' and T1.destination = 'ORD' and T2.destination = 'DFW')
     OR
     (T1.origin = 'LAX' and T1.destination = 'ORD' and T2.destination = 'JFK')

 ) 
 ;
 
/*CMI → ORD → LAX, 04/03/2008
JAX → DFW → CRP, 09/09/2008
SLC → BFL → LAX, 01/04/2008
LAX → SFO → PHX, 12/07/2008
DFW → ORD → DFW, 10/06/2008
LAX → ORD → JFK, 01/01/2008*/








