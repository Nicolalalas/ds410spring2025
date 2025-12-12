import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

object Q2 {
  def main(args: Array[String]) = {
    val sc = getSC()
    val myrdd = getRDD(sc)
    val counts = doCity(myrdd)
    saveIt(counts, "spadoopframes_q2")
  }

  def getSC(): SparkContext = {
    val conf = new SparkConf().setAppName("Q2")
    new SparkContext(conf)
  }

  def getRDD(sc: SparkContext): RDD[String] = {
    sc.textFile("/datasets/cities")
  }

  def doCity(input: RDD[String]): RDD[(String, (Int, Int, Int))] = {
    input
      .map(line => line.split("\t"))
      .filter(parts => parts.length >= 6)
      .map(parts => {
        try {
          val state = parts(1)
          val population = parts(3).toLong
          val zipStr = parts(4).trim
          
          // Count zip codes
          val numZips = if (zipStr == "") 0 else zipStr.split("\\s+").length
          
          // Skip cities with 0 zip codes
          if (numZips == 0) {
            null
          } else {
            val peoplePerZip = population.toFloat / numZips
            val isDense = if (population >= 500L * numZips) 1 else 0
            val isSuperDense = if (population >= 1000L * numZips) 1 else 0
            
            (state, (peoplePerZip, 1, isDense, isSuperDense))
          }
        } catch {
          case _: Exception => null
        }
      })
      .filter(_ != null)
      .reduceByKey((a, b) => (
        a._1 + b._1,  // sum of people per zip
        a._2 + b._2,  // count of cities
        a._3 + b._3,  // count of dense cities
        a._4 + b._4   // count of super dense cities
      ))
      .mapValues { case (sumPPZ, cityCount, denseCount, superDenseCount) =>
        val avgPPZ = math.round(sumPPZ / cityCount).toInt
        (avgPPZ, denseCount, superDenseCount)
      }
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    sc.parallelize(Seq(
      "City1\tCA\tCounty1\t10000\t90001 90002\tID1",
      "City2\tCA\tCounty2\t5000\t90003\tID2",
      "City3\tNY\tCounty3\t100000\t10001 10002 10003 10004\tID3",
      "City4\tNY\tCounty4\t50000\t10005\tID4"
    ))
  }

  def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int, Int))] = {
    sc.parallelize(Seq(
      ("CA", (6667, 0, 0)),
      ("NY", (40000, 1, 0))
    ))
  }

  def saveIt(myrdd: RDD[(String, (Int, Int, Int))], name: String) = {
    myrdd.saveAsTextFile(name)
  }
}
