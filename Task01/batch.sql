
SELECT * from output2_1
where 
origin = 'CMI' OR origin = 'BWI' OR origin = 'MIA' OR origin = 'LAX' OR origin = 'IAH' OR origin = 'SFO'
order by origin asc,departuredelay asc
;

SELECT * from output2_2
where 
origin = 'CMI' OR origin = 'BWI' OR origin = 'MIA' OR origin = 'LAX' OR origin = 'IAH' OR origin = 'SFO'
order by origin asc,departuredelay asc
;

select * from output2_3
where
(origin='CMI' and dest = 'ORD') OR
(origin='IND' and dest = 'CMH') OR
(origin='DFW' and dest = 'IAH') OR
(origin='LAX' and dest = 'SFO') OR
(origin='JFK' and dest = 'LAX') OR
(origin='ATL' and dest = 'PHX') 
order by origin asc,dest asc,ArrivalDelay;

select * from output2_4
where
(origin='CMI' and dest = 'ORD') OR
(origin='IND' and dest = 'CMH') OR
(origin='DFW' and dest = 'IAH') OR
(origin='LAX' and dest = 'SFO') OR
(origin='JFK' and dest = 'LAX') OR
(origin='ATL' and dest = 'PHX') ;


use airportdatabase;
select origin1,destination1,flightdate1,departuretime1,concat(uniquecarrier1,flightnumner1) as Flightdetails1,ArrivalDelay1,
origin2,destination2,flightdate2,departuretime2,concat(uniquecarrier2,flightnumner2) as Flightdetails2,arrivaldelay2
from output3_2
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



/*
CMIORDLAX2008-03-04
JAXDFWCRP2008-09-09
SLCBFLLAX2008-04-01
LAXSFOPHX2008-07-12
DFWORDDFW2008-06-10
LAXORDJFK2008-01-01
*/