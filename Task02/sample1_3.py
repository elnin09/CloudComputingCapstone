import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import re
import sys
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /spark-2.1.1-bin-hadoop2.6/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'

def toCSVLine(data):
  return ','.join(str(d) for d in data)


def printresults(time,rdd):
    print("New streaming data")
    for record in rdd.take(10):
        print(','.join([record[0], str(record[1])]))


def mapperfunction(line):
    values = re.split(",",line);
    #print(values)
    #retval.append(values[6])
    #retval.append(values[14])
    try:
        return((values[4],[float(values[14]),1]))
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
    return ((x[0],float(x[1][0][0]) / float(x[1][0][1])))

conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc,5)
print('ssc =================== {} {}')
ssc.checkpoint("/tmp/streaming")
kstream = KafkaUtils.createDirectStream(ssc, topics = ['testing12345'], 
     kafkaParams = {"metadata.broker.list": 'localhost:9092'})

print('contexts =================== {} {}')
lines = kstream.map(lambda x: x[1])
datanew = lines.map(lambda x:mapperfunction(x))
datanew.pprint()
wcreduce = datanew.reduceByKey(lambda a, b: reducefunction(a,b)).updateStateByKey(updatefunction)
wcreduce.pprint()
wcreduce = wcreduce.map(lambda x:finalmap(x))

rdd = wcreduce.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=True)).foreachRDD(printresults)

ssc.start()
ssc.awaitTermination()
ssc.stop(stopGraceFully = True)
sc.stop()
