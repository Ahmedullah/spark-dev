from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('spark_test')
    sc = SparkContext(conf = conf)

    nums = sc.parallelize(range(1, 10), 4)

    sq_nums = nums.map(lambda x: x * x)
    print sq_nums.collect()

    sc.stop()
    
    


