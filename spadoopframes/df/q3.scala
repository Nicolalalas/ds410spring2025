import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q3_alt {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Q3").getOrCreate()

    val df = loadCities(spark)
    val out = computeByState(df)

    writeOut(out, "spadoopframes_q3")
  }

  def loadCities(spark: SparkSession): DataFrame = {
    val schema = StructType(Seq(
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("state_full", StringType, true),
      StructField("county", StringType, true),
      StructField("population", LongType, true),
      StructField("zipcodes", StringType, true),
      StructField("id", StringType, true)
    ))

    spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .schema(schema)
      .load("/datasets/cities/cities.csv")
  }

  def computeByState(df0: DataFrame): DataFrame = {
    val cleaned = df0
      .filter(col("state").isNotNull && col("population").isNotNull)
      .withColumn(
        "num_zips",
        when(col("zipcodes").isNull || length(trim(col("zipcodes"))) === 0, lit(0))
          .otherwise(size(split(trim(col("zipcodes")), "\\s+")))
      )
      .filter(col("num_zips") > 0)
      .withColumn(
        "dense_flag",
        when(col("population") >= lit(500) * col("num_zips"), lit(1)).otherwise(lit(0))
      )
      .withColumn(
        "super_dense_flag",
        when(col("population") >= lit(1000) * col("num_zips"), lit(1)).otherwise(lit(0))
      )

    cleaned
      .groupBy(col("state"))
      .agg(
        bround(
          sum(col("population")).cast("double") /
          sum(col("num_zips")).cast("double"),
          0
        ).cast("int").as("avg_people_per_zip"),
        sum(col("dense_flag")).cast("int").as("dense_cities"),
        sum(col("super_dense_flag")).cast("int").as("super_dense_cities")
      )
      .select("state", "avg_people_per_zip", "dense_cities", "super_dense_cities")
  }

  def writeOut(df: DataFrame, dir: String): Unit = {
    df.write.mode("overwrite").option("header", "false").csv(dir)
  }
}

