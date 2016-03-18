import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}

/**
	Use the --master option in the spark-submit command
*/
object TestPartition {
	def main(args: Array[String]) {
		
		val cfg = new SparkConf().setAppName("TestPartitionerJob")
		val sc = new SparkContext(cfg)
		
		val data = for { x <- 1 to 4; y <- 1 to 3 } yield (x, y)
		println(">>>> Test data:")
		data.foreach(println)

		println(">>>> Creating paired RDD with 2 hash partitions.")
		val pairs = sc.parallelize(data)
						.partitionBy(new HashPartitioner(2))
						.cache

		println(">>>> Number of elements in each parition.")
		// Preserve the parition as nothing is changed in each partition
		pairs.mapPartitions(x => Iterator(x.length), true).collect.foreach(println)	

		println(">>>> foldByKey(0)(_ + _)")
		pairs.foldByKey(0)(_ + _).collect.foreach(println)

		println(">>>> reduceByKey(_ + _)")
		pairs.reduceByKey(_ + _).collect.foreach(println)
	}
}

