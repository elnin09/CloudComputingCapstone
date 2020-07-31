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
import pyspark_cassandra

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /spark-2.1.1-bin-hadoop2.6/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'

def toCSVLine(data):
  return ','.join(str(d) for d in data)


def printresults(time,rdd): 
    #df = sc.createDataFrame(rdd).toDF("id", "vals")
    #df.show()
    #print((df.count(), len(df.columns)))
    #df.write.format("org.apache.spark.sql.cassandra").options(table="output2_1", keyspace="cloudcomputingcapstone").save()
    #keys= ["LGA,BOS","BOS,LGA","OKC,DFW","MSP,ATL"]
    rdd.collect().saveToCassandra("cloudcomputingcapstone","newtable")
    print("New streaming data")
    for record in rdd.collect(): 
        if True or record[0] in keys:
            print(','.join([record[0], str(record[1])]))

"""
def savetocassandra(time,rdd):
    cluster=Cluster()
    session=cluster.connect()
    session.execute("use cloudcomputingcapstone")
    for record in rdd.collect():
        origin,carrier = re.split(",",record[0])
        query = "insert into output2_1(origin,carrier,delay) values("+"'"+origin+"'"+",'"+carrier+"'"+","+record[1]+")"
"""       

def mapperfunction1(line):
    values = re.split(",",line);
    #print(values)
    #retval.append(values[6])
    #retval.append(values[14])
    #print("map phase start")
    try:
        if(float(values[11])<1200 and float(values[0]==2008)):
            print("check 1SS")
            return((values[9]+','+values[10]+','+values[5],[float(values[14]),values[6],values[8],values[11]]))
        else:
            return(("plae,ho,lder",[float(99999),"randokm","random","random"]))

    except:
        return(("plae,ho,lder",[float(99999),"randokm","random","random"]))

    


def mapperfunction(line):
    values = re.split(",",line);
    #print(values)
    #retval.append(values[6])
    #retval.append(values[14])
    #print("map phase start")
    try:
        if(float(values[11])>1200 and float(values[0])==2008):
            print("check 1SS")
            return((values[9]+','+values[10]+','+values[5],[float(values[14]),values[6],values[8],values[11]]))
        else:
            return(("plae,ho,lder",[float(99999),"randokm","random","random"]))

    except:
        return(("plae,ho,lder",[float(99999),"randokm","random","random"]))
    

def reducefunction(a,b):
    #print("reduce phase start")
    #print(a,b)
    try:
        retval =list()
        if(a[0]>b[0]):
            return b
        else:
            return a
    except:
        pass
    #print("please print")
    #print(retval)
    

def updatefunction(a,b):
    #print("update phase start")
    #print(a,b)
    if b is None:
        return a
    try:
        if(a[0][0]>b[0][0]):
            return b
        else:
            return a
    except:
        return b;

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
#lines = lines.flatMap(lambda x: flatmapfunction(x))
datanew = lines.map(lambda x:mapperfunction(x))
datanew1 = lines.map(lambda x:mapperfunction1(x))

#datanew.pprint()
wcreduce = datanew.reduceByKey(lambda a, b: reducefunction(a,b)).updateStateByKey(updatefunction)
wcreduce1 = datanew1.reduceByKey(lambda a, b: reducefunction(a,b)).updateStateByKey(updatefunction)

#wcreduce.pprint()
#wcreduce = wcreduce.map(lambda x:finalmap(x))

rdd = wcreduce.transform(lambda rdd: rdd.sortBy(lambda x: x[0], ascending=True)).foreachRDD(printresults)
rdd1 = wcreduce1.transform(lambda rdd: rdd.sortBy(lambda x: x[0], ascending=True)).foreachRDD(printresults)
#if rdd is not None:
    #rdd.saveAsTextFile('output.csv')

ssc.start()
ssc.awaitTermination()
ssc.stop(stopGraceFully = True)
sc.stop()

