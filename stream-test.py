import sys
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('StreamWordCount')
    sc = SparkContext(conf = conf)
    ssc = StreamingContext(sc, 5)
    
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    count = lines.flatMap(lambda lines: lines.split(' '))\
            .map(lambda word: (word, 1))\
            .reduceByKey(lambda x, y: x + y)

    count.pprint()

    ssc.start()
    ssc.awaitTermination()
              
    
