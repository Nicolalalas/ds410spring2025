// don't forget imports

object Q4 {
  def main(args: Array[String]) = {  // this is the entry point to our code
    // don't change this function
    val sc = getSC()  // sc variable for your program, 
    val myrdd = getRDD(sc) //  get the rdd
    val result = doRetail(myrdd)  // main computation
    saveit("result", result)  // save the rdd to HDFS, change the folder name
  }

  def getSC(): SparkContext = { // get the spark context variable
  }

  def getRDD(sc:SparkContext): RDD[String] = { 
  }


  def doRetail(input: RDD[String]): RDD[(String, (Int, Int))] = {
       // this does the bulk of the work. 
  }
  def getTestRDD(sc: SparkContext): RDD[String] = {
  }
  def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int))] = {
  }
  def saveit(name: String, counts: RDD[(String, (Int, Int))]) = { 
    // save the RDD, see wordcount example
  }


}
