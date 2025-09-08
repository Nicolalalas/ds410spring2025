// don't forget imports

object Q1 {
  def main(args: Array[String]) = {  // this is the entry point to our code
    // don't change this function
    val sc = getSC()  // sc variable for your program, see wordcount example
    val myrdd = getRDD(sc) //  get the war and peace rdd
    val counts = doWordCount(myrdd)  // main computation
    saveit("result", counts)  // save the rdd to HDFS, change the folder name
  }

  def getSC(): SparkContext = { // get the spark context variable
      // see wordcount example
  }

  def getRDD(sc:SparkContext): RDD[String] = { // get war and peace as an RDD
     //every entry of the RDD should be a line of war and peace
  }


  def doWordCount(input: RDD[String]): RDD[((Char, Char), Int)] = {
       // this does the bulk of the work. The input is an rdd where
       // every entry is a line of text and the output is an rdd where
       // an entry could look something like: 
       //(a, !)    4
  }
  def getTestRDD(sc: SparkContext): RDD[String] = {
        // this returns a test rdd, like war and peace but smaller
        // use this function for developing and testing
        //  like in the spark guide assignment
  }
  def expectedOutput(sc: SparkContext): RDD[((Char, Char), Int)] = {
       // this returns the rdd that you would expect correct code
       // to produce it it was run on your test rdd
  }
  def saveit(name: String, counts: RDD[((Char, Char), Int)]) = { 
    // save the RDD, see wordcount example
  }


}
