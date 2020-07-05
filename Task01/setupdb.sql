create database airportdatabase;
use airportdata;
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

load data local inpath '/refined_combined_data.csv.gz' into table airportdata; 



