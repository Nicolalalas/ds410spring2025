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
        sc.textFile("/datasets/cities/cities.csv")
    }
    
    def doCity(input: RDD[String]): RDD[(String, (Int, Int, Int))] = {
        input
            .mapPartitionsWithIndex { (idx, iter) =>
                // Skip header line (first line of first partition)
                if (idx == 0) iter.drop(1) else iter
            }
            .map(_.split("\t"))
            .filter(_.length >= 7)  // 7 fields now!
            .flatMap { parts =>
                try {
                    val state = parts(1)
                    val population = parts(4).toLong  // population is at index 4 now!
                    val zipStr = parts(5).trim  // zipcodes at index 5 now!
                    
                    // Count zip codes - handle empty/null properly
                    val numZips = if (zipStr == null || zipStr.trim.isEmpty) {
                        0
                    } else {
                        zipStr.split("\\s+").length
                    }
                    
                    val isDense = if (numZips > 0 && population >= 500L * numZips) 1 else 0
                    val isSuperDense = if (numZips > 0 && population >= 1000L * numZips) 1 else 0
                    
                    // Emit: (state, (population, num_zips, is_dense, is_super_dense))
                    Some((state, (population, numZips, isDense, isSuperDense)))
                } catch {
                    case _: Exception => None
                }
            }
            .reduceByKey { case ((pop1, zip1, d1, sd1), (pop2, zip2, d2, sd2)) =>
                (pop1 + pop2, zip1 + zip2, d1 + d2, sd1 + sd2)
            }
            .mapValues { case (totalPop, totalZips, dense, superDense) =>
                // CRITICAL: sum(population) / sum(num_zips), NOT avg(pop/zips per city)
                val avgPPZ = if (totalZips > 0) {
                    math.round(totalPop.toDouble / totalZips).toInt
                } else {
                    0
                }
                (avgPPZ, dense, superDense)
            }
    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = {
        sc.parallelize(Seq(
            "city\tstate\tstate_full\tcounty\tpopulation\tzipcodes\tid",  // header
            "City1\tCA\tCalifornia\tCounty1\t10000\t90001 90002\tID1",
            "City2\tCA\tCalifornia\tCounty2\t5000\t90003\tID2",
            "City3\tNY\tNew York\tCounty3\t100000\t10001 10002 10003 10004\tID3",
            "City4\tNY\tNew York\tCounty4\t50000\t10005\tID4"
        ))
    }
    
    def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int, Int))] = {
        sc.parallelize(Seq(
            ("CA", (5000, 0, 0)),
            ("NY", (30000, 1, 0))
        ))
    }
    
    def saveit(myrdd: RDD[(String, (Int, Int, Int))], name: String) = {
        myrdd.saveAsTextFile(name)
    }
}
