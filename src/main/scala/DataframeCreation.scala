import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression


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

    val indexer = new StringIndexer().setInputCol("City").setOutputCol("cityCode")

    val indexed_ds1 = indexer.fit(ds1).transform(ds1)
    indexed_ds1.show()

    val dropped_ds1 = indexed_ds1.drop("City")
    dropped_ds1.show()

    val required_Features = new VectorAssembler().setInputCols(Array("Month", "Day", "Year", "AvgTemperature", "cityCode")).setOutputCol("features")
    val output = required_Features.transform(dropped_ds1)
    output.show()
    output.printSchema()
    val split = output.randomSplit(Array(0.7,0.3))
    val train = split(0)
    val test = split(1)
    val trainRows = train.count()
    val testRows = test.count()
    println("Training Row: " + trainRows + " Testing Rows: " + testRows)
    val trainML = train.select("features", "AvgTemperature")
    trainML.show(false)
    val lr = new LinearRegression().setLabelCol("AvgTemperature").setFeaturesCol("features").setMaxIter(10).setRegParam(0.3)
    val testModel = lr.fit(trainML)
    println ("Model trained!")
    val testML = test.select("features", "AvgTemperature")
    testML.show(false)
    val prediction = testModel.transform(testML)
    val predicted = prediction.select("features", "prediction", "AvgTemperature")
    predicted.show(500)
  }
}