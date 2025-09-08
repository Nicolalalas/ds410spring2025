object DFFinal {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, DataFrames only
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark)
        val answer = doFinal(mydf)
        saveit(answer, "df_final")

    }

    def doFinal(input: DataFrame): DataFrame = {

    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getDF(spark: SparkSession): DataFrame = {

    }
    
    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._

    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._

    }
 
    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}


