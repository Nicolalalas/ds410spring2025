import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q3 {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, DataFrames only
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark)
        val answer = doCity(mydf)
        saveit(answer, "spadoopframes_q3")

    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split("\\s+").size; case None => 0}})
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }

    def doCity(input: DataFrame): DataFrame = {
    }

    def getSparkSession(): SparkSession = {
        // always use this to get the spark variable, even when using the shell
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

    def getDF(spark: SparkSession): DataFrame = {
        //no schema, no points
    }
    
    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._ // do not delete this
    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        //return expected output of your test DF
        import spark.implicits._ // do not delete this
    }
 
    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}


