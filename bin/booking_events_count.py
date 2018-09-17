from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def handler(message):
    records = message.collect()
    for record in records:
        #m1=str(record)
        dt=str(datetime.now().strftime("%Y-%m-%d,%H:%M"))
        m1,m2=str(record[0]),str(record[1])
        msg=dt+","+m1+","+m2
        producer.send('test2', msg)
        producer.flush()

def main():
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 60)

    kvs = KafkaUtils.createDirectStream(ssc, ['booking_events'], {"metadata.broker.list": 'localhost:9092', "auto.offset.reset": "largest"})

    tweets = kvs.map(lambda x: x[1])
    words = tweets.map(lambda line:line.split(',')[2].split('"')[0])
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)


    wordCounts.foreachRDD(handler)

    ssc.start()
    ssc.awaitTermination()
if __name__ == "__main__":

   main()

