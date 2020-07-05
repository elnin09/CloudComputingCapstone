
/*insert overwrite local directory '/queryoutput/output2_1.csv'*/
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




CREATE EXTERNAL TABLE ddboutput2_1 
(origin string, destination string, departure_delay double)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' 
TBLPROPERTIES (
    "dynamodb.table.name" = "output2_1", 
    "dynamodb.column.mapping" = "origin:origin,destination:destination,departure_delay:departure_delay"
);

INSERT OVERWRITE TABLE ddboutput2_1
SELECT * from output2_1;

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


CREATE EXTERNAL TABLE ddboutput2_2 
(origin string, destination string, departure_delay double)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' 
TBLPROPERTIES (
    "dynamodb.table.name" = "output2_2", 
    "dynamodb.column.mapping" = "origin:origin,destination:destination,departure_delay:departure_delay"
);

INSERT OVERWRITE TABLE ddboutput2_2
SELECT * from output2_2;


/*insert overwrite local directory '/queryoutput/output2_3.csv'*/
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

CREATE EXTERNAL TABLE ddboutput2_3 
(origin string, destination string,carrier string,arrival_delay double)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' 
TBLPROPERTIES (
    "dynamodb.table.name" = "output2_3", 
    "dynamodb.column.mapping" = "origin:origin,destination:destination,carrier:carrier,arrival_delay:arrival_delay"
);

INSERT OVERWRITE TABLE ddboutput2_3
SELECT DISTINCT * from output2_3;


create table output2_4  as 
/*insert overwrite local directory '/queryoutput/output2_4.csv'*/
 SELECT origin,dest,avg(arrDelay) as ArrivalDelay
     FROM airportdata
     GROUP BY origin,dest
     ORDER BY origin asc,dest asc;