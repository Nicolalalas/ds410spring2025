//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object WordCount {  // all code must be inside an object or class
  def main(args: Array[String]) = {  // this is the entry point to our code
    val sc = getSC()  // one function to get the sc variable
    val myrdd = getRDD(sc) // on function to get the rdd
    val counts = doWordCount(myrdd) // additional functions to do the computation
    saveit("result", counts)  // save the rdd to HDFS
  }
  
  def getSC() = { // get the spark context variable
    val conf = new SparkConf().setAppName("wc")  //change the app name
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def getRDD(sc:SparkContext) = { // get the big data rdd
     sc.textFile("/datasets/wap")
  }

  def getTestRDD(sc: SparkContext) = { // create a small testing rdd
     val mylines = List(" a  bbb \t  bbb cc   ddd",
                        "cc bbb ddd",
                        "ddd")
     sc.parallelize(mylines, 3)

  }

  def doWordCount(input: RDD[String]) = { // the interesting stuff happens here
    // it is a separate function so we can test it out
    // the input is an rdd, so we can swap in the testing or big data rdd
    val words = input.flatMap(_.split("\\s+")) //split by whitespace
    val noblank = words.filter(x => x.size > 0)
    val kv = noblank.map(word => (word,1L))
    val counts = kv.reduceByKey((x,y) => x+y)
    counts
  }

  def saveit(name: String, counts: org.apache.spark.rdd.RDD[(String, Long)]) = { // change types here as appropriate
    counts.saveAsTextFile(name)
  }
}
