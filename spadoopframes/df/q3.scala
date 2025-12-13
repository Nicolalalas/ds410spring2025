import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q3 {
  def main(args: Array[String]) = {
    val spark = getSparkSession()
    registerZipCounter(spark)
    val mydf = getDF(spark)
    println("DEBUG COUNT = " + mydf.count())
    val answer = doCity(mydf)
    saveIt(answer, "spadoopframes_q3")
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Q3").getOrCreate()
  }

  def registerZipCounter(spark: SparkSession) = {
    val zipCounter = udf((x: String) => Option(x) match {
      case Some(y) => y.split("\\s+").size
      case None => 0
    })
    spark.udf.register("zipCounter", zipCounter)
  }

  def getDF(spark: SparkSession): DataFrame = {
    val schema = StructType(Array(
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("county", StringType, true),
      StructField("population", LongType, true),
      StructField("zipcodes", StringType, true),
      StructField("id", StringType, true)
    ))
    
    spark.read
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(schema)
      .csv("/datasets/cities")
  }

  def doCity(mydf: DataFrame): DataFrame = {
    val spark = mydf.sparkSession
    registerZipCounter(spark)
    
    mydf
      .filter(col("state").isNotNull && col("population").isNotNull && col("zipcodes").isNotNull)
      .withColumn("num_zips", expr("zipCounter(zipcodes)"))
      .filter(col("num_zips") > 0)
      .withColumn("people_per_zip", col("population") / col("num_zips"))
      .withColumn("is_dense", expr("int(population >= 500 * num_zips)"))
      .withColumn("is_super_dense", expr("int(population >= 1000 * num_zips)"))
      .groupBy("state")
      .agg(
        round(avg("people_per_zip")).cast("int").as("avg_people_per_zip"),
        sum(col("is_dense")).cast("int").as("dense_cities"),
        sum(col("is_super_dense")).cast("int").as("super_dense_cities")
      )
      .select("state", "avg_people_per_zip", "dense_cities", "super_dense_cities")
  }

  def getTestDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    
    Seq(
      ("City1", "CA", "County1", 10000L, "90001 90002", "ID1"),
      ("City2", "CA", "County2", 5000L, "90003", "ID2"),
      ("City3", "NY", "County3", 100000L, "10001 10002 10003 10004", "ID3"),
      ("City4", "NY", "County4", 50000L, "10005", "ID4")
    ).toDF("city", "state", "county", "population", "zipcodes", "id")
  }

  def expectedOutput(spark: SparkSession): DataFrame = {
    import spark.implicits._
    
    Seq(
      ("CA", 6667, 0, 0),
      ("NY", 40000, 1, 0)
    ).toDF("state", "avg_people_per_zip", "dense_cities", "super_dense_cities")
  }

  def saveIt(mydf: DataFrame, name: String) = {
    mydf.write.mode("overwrite").option("header", "false").csv(name)
  }
}
