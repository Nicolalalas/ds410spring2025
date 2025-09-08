// don't forget imports

object Q2 {
  def main(args: Array[String]) = {  // this is the entry point to our code
    // don't change this function
    val sc = getSC()  // sc variable for your program, 
    val myrdd = getRDD(sc) //  get the rdd
    val counts = doWordCount(myrdd)  // main computation
    saveit("result", counts)  // save the rdd to HDFS, change the folder name
  }

  def getSC(): SparkContext = { // get the spark context variable
  }

  def getRDD(sc:SparkContext): RDD[String] = { 
  }


  def doWordCount(input: RDD[String]): RDD[((Char, Char), Int)] = {
       // this does the bulk of the work. 
  }
  def getTestRDD(sc: SparkContext): RDD[String] = {
  }
  def expectedOutput(sc: SparkContext): RDD[((Char, Char), Int)] = {
  }
  def saveit(name: String, counts: RDD[((Char, Char), Int)]) = { 
    // save the RDD, see wordcount example
  }


}
