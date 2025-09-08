//add the imports

object Q2 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val (c, o, i) = getDF(spark)
        val counts = doOrders(c,o,i)
        saveit(counts, "dataframes_q2")  // save the rdd to your home directory in HDFS
    }


    def doOrders(customers: DataFrame, orders: DataFrame, items: DataFrame): DataFrame = {

    }

    def getDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getTestDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        // don't forget the spark.implicits
        import spark.implicits._
        // return 3 dfs (customers, orders, items)
    }
    def expectedOutput(spark: SparkSession) = {
        import spark.implicits._

    }

    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}

