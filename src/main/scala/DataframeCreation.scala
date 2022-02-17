import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col


case class cityAvgTemp(city: String, month: Int, day: Int, year: Int, avgtemperature: Float)

object DataframeCreation {

  def createDataframe(spark: SparkSession): Unit = {

    val encoder = product[cityAvgTemp]

    val df1 = spark.read.format("csv").option("header", "true").load("input/anch_data.csv")
    df1.show(5)
    println(df1.count())
    println(df1.describe())
    val a = df1.withColumn("City",col("City"))
      .withColumn("Month",col("Month").cast(IntegerType))
      .withColumn("Day",col("Day").cast(IntegerType))
      .withColumn("Year",col("Year").cast(IntegerType))
      .withColumn("AvgTemperature",col("AvgTemperature").cast(FloatType))

    val df2 = spark.read.format("csv").option("header", "true").load("input/neth_data.csv")
    df2.show(5)
    println(df2.count())
    println(df2.describe())
    val b = df2.withColumn("City",col("City"))
      .withColumn("Month",col("Month").cast(IntegerType))
      .withColumn("Day",col("Day").cast(IntegerType))
      .withColumn("Year",col("Year").cast(IntegerType))
      .withColumn("AvgTemperature",col("AvgTemperature").cast(FloatType))

    val df3 = spark.read.format("csv").option("header", "true").load("input/shang_data.csv")
    df3.show(5)
    println(df3.count())
    println(df3.describe("AvgTemperature"))
    val c = df3.withColumn("City",col("City"))
      .withColumn("Month",col("Month").cast(IntegerType))
      .withColumn("Day",col("Day").cast(IntegerType))
      .withColumn("Year",col("Year").cast(IntegerType))
      .withColumn("AvgTemperature",col("AvgTemperature").cast(FloatType))

    //val ds1 = spark.read.format("csv").option("header", "true").schema(encoder).load("input/shang_data.csv")

    val ds1: Dataset[cityAvgTemp] = a.select("City","Month", "Day", "Year", "AvgTemperature").as(encoder)
    val ds2: Dataset[cityAvgTemp] = b.select("City","Month", "Day", "Year", "AvgTemperature").as(encoder)
    val ds3: Dataset[cityAvgTemp] = c.select("City","Month", "Day", "Year", "AvgTemperature").as(encoder)

    ds1.show()
    ds2.show()
    ds3.show()

  }

}
