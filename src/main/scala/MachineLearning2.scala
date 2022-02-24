import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.LinearRegression

case class cityFull(city: String, month: Int, day: Int, year: Int, avgtemperature: Float, dew: Float, humidity: Float, precip: Float, windgust: Float, windspeed: Float, pressure: Float, cloudcover: Float)

object MachineLearning2 {

  //Using more data and more cities, the model becomes very accurate. Unfortunately, there is prediction info a user could not supply 80 years into the future,
  //so machine learning 1 is best used for future predictions.

  def learnMachines2(spark: SparkSession): Unit = {

    //create encoders for case classes
    val encoder = product[cityFull]

    val zero: Float = 0

    //create data frame for holding csv data
    val df1 = spark.read.format("csv").option("header", "true").load("input/Cities_Full.csv")
    //below is for testing
    //df1.show(5)
    //println(df1.count())
    //println(df1.describe("Windgust"))

    //cast values to dataframe
    val a = df1.withColumn("City",col("City"))
      .withColumn("Month",col("Month").cast(IntegerType))
      .withColumn("Day",col("Day").cast(IntegerType))
      .withColumn("Year",col("Year").cast(IntegerType))
      .withColumn("AvgTemperature",when(col("temp").isNull, zero).otherwise(col("temp").cast(FloatType)))
      .withColumn("Dew",when(col("Dew").isNull, zero).otherwise(col("Dew").cast(FloatType)))
      .withColumn("Humidity",when(col("Humidity").isNull, zero).otherwise(col("Humidity").cast(FloatType)))
      .withColumn("Precip",when(col("Precip").isNull, zero).otherwise(col("Precip").cast(FloatType)))
      .withColumn("Windgust",when(col("Windgust").isNull, zero).otherwise(col("Windgust").cast(FloatType)))
      .withColumn("Windspeed",when(col("Windspeed").isNull, zero).otherwise(col("Windgust").cast(FloatType)))
      .withColumn("Pressure",when(col("sealevelpressure").isNull, zero).otherwise(col("Windgust").cast(FloatType)))
      .withColumn("Cloudcover",when(col("Cloudcover").isNull, zero).otherwise(col("Windgust").cast(FloatType)))

    //below for testing
    //a.show(5)

    //create data set for any changes that need to be made to the data
    val ds1: Dataset[cityFull] = a.select("City","Month", "Day", "Year", "AvgTemperature","Dew","Humidity","Precip","Windgust","Windspeed","Pressure","Cloudcover").as(encoder)


    //create indexer to convert strings to int
    val indexer = new StringIndexer().setInputCol("City").setOutputCol("cityCode")

    //fit the dataset to the indexer
    val indexed_ds1 = indexer.fit(ds1).transform(ds1)

    //below used to get cityCodes
    indexed_ds1.dropDuplicates("City").show(false)

    //drop column that is no longer used
    val dropped_ds1 = indexed_ds1.drop("City")

    //create a vector assembler to create a vector based on given dataset
    val required_Features = new VectorAssembler().setInputCols(Array("Month", "Day", "Year", "cityCode","Dew","Humidity","Precip","Windgust","Windspeed","Pressure","Cloudcover")).setOutputCol("features")

    //fit the data set to the vector assembler and get new output
    val output = required_Features.transform(dropped_ds1)

    //below is for testing
    //output.select("features").show(false)

    //check schema to make sure it is correct
    //output.printSchema()

    //create a split array for train vs train data
    val split = output.randomSplit(Array(0.8,0.2))

    //hold the train split value data
    val train = split(0)

    //hold the test split value data
    val test = split(1)

    //check the count to make sure the data split correctly for testing
    //val trainRows = train.count()
    //val testRows = test.count()
    //println("Training Row: " + trainRows + " Testing Rows: " + testRows)

    //select the feature and label
    val trainML = train.select("features", "AvgTemperature")

    //check non truncated values for testing
    //trainML.show(false)

    //create a new linear regression model based on the trained data selected
    val lr = new LinearRegression().setLabelCol("AvgTemperature").setFeaturesCol("features").setMaxIter(100).setRegParam(.02)

    //create the test model based on the train model
    val testModel = lr.fit(trainML)

    println ("Model trained!")

    //select the feature and label
    val testML = test.select("features", "AvgTemperature")

    //"Month", "Day", "Year", "cityCode","Dew","Humidity","Precip","Windgust","Windspeed","Pressure","Cloudcover"
    //city codes: Amsterdam = 0, Cape Town = 1, Sao Paulo = 2, Anchorage = 3, Sydney = 4, Shanghai = 5
    //val userDataSet = Vectors.dense(6.0,1.0,2030.0,3.0,80,0,0,0,0,0)
    //val output = required_Features.transform(dropped_ds1)


    //val userInput = userInputDF.select("features").collect()(1).toString()
    //println(s"UI = $userInputDF")
    //val UI = userInput.toVector

    //check non truncated values for testing
    //testML.show(false)

    //get prediction based on linear regression
    val prediction =  testModel.transform(testML)
    //val inputPrediction = testModel.predict(userDataSet)

    //println(prediction)

    //show predicted data
    prediction.show(false)

    //println(s"The prediction based on the user input is $inputPrediction degrees Fahrenheit.")

    //print the coefficients and intercept for linear regression
    println(s"Coefficients: ${testModel.coefficients} Intercept: ${testModel.intercept}")

    val trainingSummary = testModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    //trainingSummary.residuals.show()
    //standard deviation of prediction errors
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //variation in the dependent variable that is predictable from the independent variable
    println(s"r2: ${trainingSummary.r2}")
  }
}
