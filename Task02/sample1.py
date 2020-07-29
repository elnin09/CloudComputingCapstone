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
     for record in rdd.take(10):
          print(','.join([record[0], str(record[1])]))


def mapperfunction(line):
    retval=list()
    values = re.split(",",line);
    #print(values)
    retval.append(values[9])
    retval.append(values[10])
    return retval

conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
sc = SparkContext(conf=conf)

ssc = StreamingContext(sc,5)
sc.setLogLevel("ERROR")
print('ssc =================== {} {}')
ssc.checkpoint("/tmp/streaming")
kstream = KafkaUtils.createDirectStream(ssc, topics = ['testing12345'], 
     kafkaParams = {"metadata.broker.list": 'localhost:9092'})

print('contexts =================== {} {}')
lines = kstream.map(lambda x: x[1])
datanew = lines.flatMap(lambda x:mapperfunction(x))
datanew.count()

popularity = datanew.map(lambda x: (x,1))
wcreduce = popularity.reduceByKey(lambda a, b: a + b).updateStateByKey(lambda n,o : sum(n)+(o or 0))

rdd = wcreduce.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False)).foreachRDD(printresults)

ssc.start()
ssc.awaitTermination()
ssc.stop(stopGraceFully = True)
sc.stop()
