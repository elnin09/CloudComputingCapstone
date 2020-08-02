import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import re
import sys
from pyspark.sql import SQLContext
from pyspark import sql
from decimal import Decimal
from cassandra.cluster import Cluster

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /spark-2.1.1-bin-hadoop2.6/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'

def toCSVLine(data):
  return ','.join(str(d) for d in data)


def printresults(time,rdd): 
    #df = sc.createDataFrame(rdd).toDF("id", "vals")
    #df.show()
    #print((df.count(), len(df.columns)))
    #df.write.format("org.apache.spark.sql.cassandra").options(table="output2_1", keyspace="cloudcomputingcapstone").save()
    keys= ["LGA,BOS","BOS,LGA","OKC,DFW","MSP,ATL"]
    print("New streaming data")
    cluster = Cluster()
    session = cluster.connect()
    for record in rdd.collect():
        session.execute('use cloudcomputingcapstone')
        key = str(record[0])
        value = str(record[1]) 
        query = "insert into output2_4(key,value) values('"+key+"','"+value+"')"
        #print(query)
        session.execute(query)
        if record[0] in keys:
            print(','.join([record[0], str(record[1])]))
    cluster.shutdown()

"""
def savetocassandra(time,rdd):
    cluster=Cluster()
    session=cluster.connect()
    session.execute("use cloudcomputingcapstone")
    for record in rdd.collect():
        origin,carrier = re.split(",",record[0])
        query = "insert into output2_1(origin,carrier,delay) values("+"'"+origin+"'"+",'"+carrier+"'"+","+record[1]+")"
"""       



def mapperfunction(line):
    values = re.split(",",line);
    #print(values)
    #retval.append(values[6])
    #retval.append(values[14])
    try:
        return((values[9]+','+values[10],[float(values[14]),1]))
    except:
        return(("plaeholder",[float(99999),float(1)]))

def reducefunction(a,b):
    #print(a,b)
    retval =list()
    try:
        retval.append(float(a[0])+float(b[0]))
        retval.append(float(a[1])+float(b[1]))
    except:
        try:
            retval.append(float(a[0]))
            retval.append(float(a[1]))
        except:
            retval.append(float(b[0]))
            retval.append(float(b[1]))

    #print("please print")
    #print(retval)
    return retval

def updatefunction(a,b):
    #print(a,b)
    if b is None:
        return a
    retval = [[]]
    try:
        retval[0].append(float(a[0][0])+float(b[0][0]))
        retval[0].append(float(a[0][1])+float(b[0][1]))
    except:
        try:
            retval[0].append(float(a[0][0]))
            retval[0].append(float(a[0][1]))
        except:
            retval[0].append(float(b[0][0]))
            retval[0].append(float(b[0][1]))
    #print(retval)
    return retval

    

def finalmap(x):
    #print(x)
    return ( (x[0],(x[1][0][0])/(x[1][0][1]) ))

conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc,5)
sqlContext = sql.SQLContext(sc)
print('ssc =================== {} {}')
ssc.checkpoint("/tmp/streaming")
kstream = KafkaUtils.createDirectStream(ssc, topics = ['testing12345'], 
     kafkaParams = {"metadata.broker.list": 'localhost:9092'})

print('contexts =================== {} {}')
lines = kstream.map(lambda x: x[1])
datanew = lines.map(lambda x:mapperfunction(x))
#datanew.pprint()
wcreduce = datanew.reduceByKey(lambda a, b: reducefunction(a,b)).updateStateByKey(updatefunction)
#wcreduce.pprint()
wcreduce = wcreduce.map(lambda x:finalmap(x))

rdd = wcreduce.transform(lambda rdd: rdd.sortBy(lambda x: x[0], ascending=True)).foreachRDD(printresults)
if rdd is not None:
    rdd.saveAsTextFile('output.csv')

ssc.start()
ssc.awaitTermination()
ssc.stop(stopGraceFully = True)
sc.stop()
