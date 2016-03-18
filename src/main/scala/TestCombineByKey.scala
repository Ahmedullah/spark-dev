
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Sample for using PairRDDFunctions.combineByKey(...)
 */

// Data store class for student and their subject score
case class ScoreDetail(studentName: String, subject: String, score: Float)

object TestCombineByKey {
	def main(args: Array[String]): Unit = {
		
		val scores = List(ScoreDetail("Theresa", "Math", 98), ScoreDetail("Wilma", "Math", 88), 
				ScoreDetail("Shamak", "Math", 75), ScoreDetail("Theresa", "English", 78), 
				ScoreDetail("Wilma", "English", 90), ScoreDetail("Shamak", "English", 80))
				
		// convert to (key, values) -> (Student Name: String, score: ScoreDetail)
		val scoresWithKey = for { i <- scores } yield (i.studentName, i)
		
		val sc = new SparkContext(new SparkConf().setAppName("TestCombineByKeyJob"))
		
		// If data set is reused then cache recommended...
		val scoresWithKeyRDD = sc.parallelize(scoresWithKey).cache
		
		// Combine the scores for each student
		val avgScoresRDD = scoresWithKeyRDD.combineByKey(
					(x: ScoreDetail) => (x.score, 1) /*createCombiner*/, 
					(acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1) /*mergeValue*/, 
					(acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) /*mergeCombiners*/
			// calculate the average
			).map( { case(key, value) => (key, value._1/value._2) })
		
		avgScoresRDD.collect.foreach(println)
		
	}
}