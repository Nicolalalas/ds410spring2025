import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object Q3 {
  def main(args: Array[String]) = {
    val spark = getSparkSession()
    registerZipCounter(spark)
    val mydf = getDF(spark)
    val answer = doCity(mydf)
    saveit(answer, "spadoopframes_q3")
  }
  
  def getSparkSession(): SparkSession = {
    SparkSession.builder().appName("Q3").getOrCreate()
  }

  def registerZipCounter(spark: SparkSession) = {
    val zipCounter = udf((x: String) =>
      Option(x).map(_.trim).filter(_.nonEmpty) match {
        case Some(y) => y.split("\\s+").length
        case None => 0
      }
    )
    spark.udf.register("zipCounter", zipCounter)
  }

  def getDF(spark: SparkSession): DataFrame = {
    val schema = StructType(Array(
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("state_full", StringType, true),
      StructField("county", StringType, true),
      StructField("population", LongType, true),
      StructField("zipcodes", StringType, true),
      StructField("id", StringType, true)
    ))
    
    spark.read
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema)
      .csv("/datasets/cities/cities.csv")
  }
  
  def doCity(mydf: DataFrame): DataFrame = {
    val df = mydf
      .filter(col("state").isNotNull && col("population").isNotNull)
      .withColumn("num_zips", expr("zipCounter(zipcodes)"))
      .withColumn("is_dense", expr("int(population >= 500 * num_zips)"))
      .withColumn("is_super_dense", expr("int(population >= 1000 * num_zips)"))
    
    df.groupBy(col("state"))
      .agg(
        round(
          sum(col("population")).cast("double") /
          sum(col("num_zips")).cast("double")
        ).cast("int").as("avg_people_per_zip"),
        sum(col("is_dense")).cast("int").as("dense_cities"),
        sum(col("is_super_dense")).cast("int").as("super_dense_cities")
      )
      .select(col("state"), col("avg_people_per_zip"), col("dense_cities"), col("super_dense_cities"))
  }
  
  def getTestDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      ("City1", "CA", "California", "County1", 10000L, "90001 90002", "ID1"),
      ("City2", "CA", "California", "County2", 5000L, "90003", "ID2"),
      ("City3", "NY", "New York", "County3", 100000L, "10001 10002 10003 10004", "ID3"),
      ("City4", "NY", "New York", "County4", 50000L, "10005", "ID4")
    ).toDF("city", "state", "state_full", "county", "population", "zipcodes", "id")
  }
  
  def expectedOutput(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      ("CA", 5000, 0, 0),
      ("NY", 30000, 1, 0)
    ).toDF("state", "avg_people_per_zip", "dense_cities", "super_dense_cities")
  }
 
  def saveit(counts: DataFrame, name: String) = {
    counts.write.format("csv").mode("overwrite").option("header", "false").save(name)
  }
}
