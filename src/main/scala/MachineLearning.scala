import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.storage.StorageLevel

case class cityAll(city: String, month: Int, day: Int, year: Int, avgtemperature: Float)

object MachineLearning {

  def learnMachines(spark: SparkSession, year: Int, month: Int, day: Int, city: Int): Unit = {

    val t1 = System.nanoTime

    //create encoders for case classes
    val encoder1 = product[cityAll]

    //create data frame for holding csv data
    val df1 = spark.read.format("csv").option("header", "true").load("input/cities.csv")
    //df1.show(5)
    //println(df1.count())
    //println(df1.describe())
    val a = df1.withColumn("City", col("City"))
      .withColumn("Month", col("Month").cast(IntegerType))
      .withColumn("Day", col("Day").cast(IntegerType))
      .withColumn("Year", col("Year").cast(IntegerType))
      .withColumn("AvgTemperature", col("AvgTemperature").cast(FloatType))

    //create data set for any changes that need to be made to the data
    val ds1: Dataset[cityAll] = a.select("City", "Month", "Day", "Year", "AvgTemperature").as(encoder1)

    //create indexer to convert strings to int
    val indexer = new StringIndexer().setInputCol("City").setOutputCol("cityCode")

    //fit the dataset to the indexer
    val indexed_ds1 = indexer.fit(ds1).transform(ds1)

    //drop column that is no longer used
    val dropped_ds1 = indexed_ds1.drop("City")

    //create a vector assembler to create a vector based on given dataset
    //val required_Features = new VectorAssembler().setInputCols(Array("Month", "Day", "Year", "AvgTemperature", "cityCode")).setOutputCol("features")
    val required_Features = new VectorAssembler().setInputCols(Array("Month", "Day", "Year", "cityCode")).setOutputCol("features")

    //fit the data set to the vector assembler and get new output
    val output = required_Features.transform(dropped_ds1)

    output.persist(StorageLevel.MEMORY_ONLY)

    // output.select("features").show()

    //check schema to make sure it is correct
    //output.printSchema()

    //create a split array for train vs train data
    val split = output.randomSplit(Array(0.8, 0.2))

    //hold the train split value data
    val train = split(0)

    //hold the test split value data
    val test = split(1)

    //check the count to make sure the data split correctly
    //val trainRows = train.count()
    //val testRows = test.count()
    //println("Training Row: " + trainRows + " Testing Rows: " + testRows)

    //select the feature and label
    val trainML = train.select("features", "AvgTemperature")

    //check non truncated values
    //trainML.show(false)

    //create a new linear regression model based on the trained data selected
    //val lr = new LinearRegression().setLabelCol("AvgTemperature").setFeaturesCol("features").setMaxIter(10).setRegParam(0.3)
    val lr = new LinearRegression().setLabelCol("AvgTemperature").setFeaturesCol("features").setMaxIter(100).setRegParam(.02)

    //create the test model based on the train model
    val testModel = lr.fit(trainML)

    println("Model trained!")

    //select the feature and label
    val testML = test.select("features", "AvgTemperature")

    //month, day, year, city code
    //0 = Shanghai
    //1 = Amsterdam
    //2 = Anchorage
    val userInput = Vectors.dense(month, day, year, city)

    //check non truncated values
    //testML.show(false)

    //get prediction based on linear regression
    //val prediction = testModel.transform(trainML)

    //val prediction =  testModel.transform(testML)

    val prediction = testModel.transform(testML)
    val inputPrediction = testModel.predict(userInput)

    //println(prediction)

    //show predicted data
    //val predicted = prediction.select("features", "AvgTemperature")

    //prediction.show(false)

    println(s"The prediction based on the user input is $inputPrediction degrees Fahrenheit.")

    //print the coefficients and intercept for linear regression
    //println(s"Coefficients: ${testModel.coefficients} Intercept: ${testModel.intercept}")

    //val trainingSummary = testModel.summary
    //println(s"numIterations: ${trainingSummary.totalIterations}")
    //println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    //trainingSummary.residuals.show()
    //println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //println(s"r2: ${trainingSummary.r2}")

    val duration = (System.nanoTime - t1) / 1e9d

    println()
    println("The execution time of the function is: " + duration + " seconds.")
  }
}