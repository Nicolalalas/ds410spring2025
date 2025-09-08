
object RDDFinal {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, RDDs only
        val sc = getSC()  // one function to get the sc variable
        val myrdd = getRDD(sc) // on function to get the rdd
        val answer = doFinal(myrdd) // additional functions to do the computation
        saveit(answer, "rdd_final")  // save the rdd to your home directory in HDFS
    }

    def getSC(): SparkContext = {

    }

    def getRDD(sc:SparkContext): RDD[String] = { // get the big data rdd
         
    }

    def doFinal(input: RDD[String]): RDD[((String, String), Double)] = {
        //make sure to use split("\t", -1) so that it behaves like the python function split("\t")
        // in scala, dividing two ints gives you an int, so convert one to a float right before division
    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = {

    }

    def expectedOutput(sc: SparkContext): RDD[((String, String), Double)] = {

    }

    def saveit(myrdd: RDD[((String, String), Double)], name: String) = {
        myrdd.saveAsTextFile(name)
    }

}

