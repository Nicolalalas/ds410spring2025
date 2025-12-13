import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q2 {  
    def main(args: Array[String]) = {
        val sc = getSC()
        val myrdd = getRDD(sc)
        val counts = doCity(myrdd)
        saveit(counts, "spadoopframes_q2")
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
            .map(_.split("\t"))
            .filter(_.length >= 6)
            .flatMap { parts =>
                try {
                    val state = parts(1)
                    val population = parts(3).toLong
                    val zipStr = parts(4).trim
                    
                    // Handle empty string case: "".split("\\s+") gives Array("") with length 1
                    // We want 0 zips for empty string
                    val numZips = if (zipStr.isEmpty) {
                        0
                    } else {
                        zipStr.split("\\s+").length
                    }
                    
                    // Skip cities with 0 zip codes
                    if (numZips == 0) {
                        None
                    } else {
                        val peoplePerZip = population.toDouble / numZips
                        val isDense = if (population >= 500L * numZips) 1 else 0
                        val isSuperDense = if (population >= 1000L * numZips) 1 else 0
                        
                        Some((state, (peoplePerZip, 1, isDense, isSuperDense)))
                    }
                } catch {
                    case _: Exception => None
                }
            }
            .reduceByKey { case ((sum1, cnt1, d1, sd1), (sum2, cnt2, d2, sd2)) =>
                (sum1 + sum2, cnt1 + cnt2, d1 + d2, sd1 + sd2)
            }
            .mapValues { case (sumPPZ, count, dense, superDense) =>
                val avgPPZ = math.round(sumPPZ / count).toInt
                (avgPPZ, dense, superDense)
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
    
    def saveit(myrdd: RDD[(String, (Int, Int, Int))], name: String) = {
        myrdd.saveAsTextFile(name)
    }
}
