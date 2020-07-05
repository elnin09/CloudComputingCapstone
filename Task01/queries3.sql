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
 where T1.destination = T2.origin and datediff(T2.flightdate,T1.Flightdate) = 2
 /*and 
 (
     (T1.origin = 'CMI' and T1.destination = 'ORD' and T2.destination = 'LAX')
     OR
     (T1.origin = 'JAX' and T1.destination = 'DFW' and T2.destination = 'CRP')
 ) */
 ;
 
/*CMI → ORD → LAX, 04/03/2008
JAX → DFW → CRP, 09/09/2008
SLC → BFL → LAX, 01/04/2008
LAX → SFO → PHX, 12/07/2008
DFW → ORD → DFW, 10/06/2008
LAX → ORD → JFK, 01/01/2008*/

select * from output3_2
where 
(origin1 = 'CMI' and  destination1 = 'ORD' and destination2 = 'LAX' and Flightdate1 = '2008-03-04')
OR
(origin1 = 'JAX' and  destination1 = 'DFW' and destination2 = 'CRP' and Flightdate1 = '2008-09-09')
OR
(origin1 = 'SLC' and  destination1 = 'BFL' and destination2 = 'LAX' and Flightdate1 = '2008-04-01')
OR
(origin1 = 'LAX' and  destination1 = 'SFO' and destination2 = 'PHX' and Flightdate1 = '2008-07-12')
OR
(origin1 = 'DFW' and  destination1 = 'ORD' and destination2 = 'DFW' and Flightdate1 = '2008-06-10')
OR
(origin1 = 'LAX' and  destination1 = 'ORD' and destination2 = 'JFK' and Flightdate1 = '2008-01-01')
;







