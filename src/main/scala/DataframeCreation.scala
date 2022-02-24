import scala.io.StdIn._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import MachineLearning._
import java.util.InputMismatchException
import java.util.Scanner

import java.util.{InputMismatchException, Scanner}

case class cityAvgTemp(city: String, month: Int, day: Int, year: Int, avgtemperature: Float)

object DataframeCreation {

  def createDataframe(spark: SparkSession, caseInt: Int): Unit = {

    val scanner = new Scanner(System.in)

    val df1 = spark.read.format("csv").option("header", "true").load("input/anchor_data.csv")
    //df1.show(5)

    val df2 = spark.read.format("csv").option("header", "true").load("input/shang_data.csv")
    //df2.show(5)

    val df3 = spark.read.format("csv").option("header", "true").load("input/nether_data.csv")
    //df3.show(5)

    try{
      val i = caseInt
      i match {
        case 1 => // ========================================= Query 1 =================================================

          println("Enter starting date in the format of YYYY-MM-DD")
          val startDate = readLine()
          println("Enter ending date in the format of YYYY-MM-DD")
          val endDate = readLine()
          println("Which City would you like to see?\n1. Shanghai\n2. Amsterdam\n3. Anchorage")
          val city = readLine()
          if (city == "a") {
            val df1_avg = df1.select("*").where(df1("Date").between(startDate,endDate)).where("AvgTemperature != -99.0")
            println("AvgTemperature:" + df1_avg.select(avg("AvgTemperature")).show())
          }
          else if (city == "b") {
            val df2_avg = df2.select("*").where(df2("Date").between(startDate,endDate)).where("AvgTemperature != -99.0")
            println("AvgTemperature:" + df2_avg.select(avg("AvgTemperature")).show())
          }
          else if (city == "c") {
            val df3_avg = df3.select("*").where(df3("Date").between(startDate,endDate)).where("AvgTemperature != -99.0")
            println("AvgTemperature:" + df3_avg.select(avg("AvgTemperature")).show())
          }
          else {
            println("That is not a valid input.")
          }

        case 2 => // =========================================== Query 2 ===============================================

          println("Pick a year between 1995 and 2020.")
          val year = readLine()
          if(year > "2020" || year < "1995") {
            println("The chosen year is not available.")
          } else {
            val df1_range = df1.select("*").where(df1("Date").between(s"$year-01-01", s"$year-12-31")).where("AvgTemperature != -99.0")
            val df2_range = df2.select("*").where(df2("Date").between(s"$year-01-01", s"$year-12-31")).where("AvgTemperature != -99.0")
            val df3_range = df3.select("*").where(df3("Date").between(s"$year-01-01", s"$year-12-31")).where("AvgTemperature != -99.0")
            val df1_fix = df1_range.withColumn("AvgTemperature",col("AvgTemperature").cast(FloatType))
            val df2_fix = df2_range.withColumn("AvgTemperature",col("AvgTemperature").cast(FloatType))
            val df3_fix = df3_range.withColumn("AvgTemperature",col("AvgTemperature").cast(FloatType))
            println("Anchorage:")
            df1_fix.select(max("AvgTemperature").alias("Hottest"), min("AvgTemperature").alias("Coldest")).show()
            println("Amsterdam:")
            df2_fix.select(max("AvgTemperature").alias("Hottest"), min("AvgTemperature").alias("Coldest")).show()
            println("Shanghai:")
            df3_fix.select(max("AvgTemperature").alias("Hottest"), min("AvgTemperature").alias("Coldest")).show()
          }
        case 3 =>  // =========================================== Query 3 ==============================================

          println("Which City would you like to see?\n1. Shanghai\n2. Amsterdam\n3. Anchorage")
          val city = scanner.nextInt() - 1
          println("Please insert a future date you would like to predict.\nYear:")
          val year = scanner.nextInt()
          println("Month:")
          val month = scanner.nextInt()
          println("Day:")
          val day = scanner.nextInt()
          learnMachines(spark, year, month, day, city)

        case 4 =>  // =========================================== Query 4 ==============================================

          println("Anchorage Average Temperature and US CO2 Emissions per year:")
          val df1_avg = df1.select("AvgTemperature","Year", "co2").where("AvgTemperature != -99.0")
          df1_avg.groupBy("Year","co2").agg(avg("AvgTemperature")).orderBy("Year").show(26)
          println("Amsterdam Average Temperature and Netherlands CO2 Emissions per year:")
          val df2_avg = df2.select("AvgTemperature","Year", "co2").where("AvgTemperature != -99.0")
          df2_avg.groupBy("Year","co2").agg(avg("AvgTemperature")).orderBy("Year").show(26)
          println("Shanghai Average Temperature and China CO2 Emissions per year:")
          val df3_avg = df3.select("AvgTemperature","Year", "co2").where("AvgTemperature != -99.0")
          df3_avg.groupBy("Year","co2").agg(avg("AvgTemperature")).orderBy("Year").show(26)

        case 5 =>  // =========================================== Query 5 ==============================================

          println("Anchorage Average Temperature and US Greenhouse Gas Emissions per year:")
          val df1_avg1 = df1.select("AvgTemperature", "Year", "city", "ghg").where("AvgTemperature != -99.0")
          df1_avg1.groupBy("Year", "city", "ghg").agg(avg("AvgTemperature")).orderBy("Year").show(26)
          println("Amsterdam Average Temperature and US Greenhouse Gas Emissions per year:")
          val df2_avg1 = df2.select("AvgTemperature", "Year", "city", "ghg").where("AvgTemperature != -99.0")
          df2_avg1.groupBy("Year", "city", "ghg").agg(avg("AvgTemperature")).orderBy("Year").show(26)
          println("Shanghai Average Temperature and US Greenhouse Gas Emissions per year:")
          val df3_avg1 = df3.select("AvgTemperature", "Year", "city", "ghg").where("AvgTemperature != -99.0")
          df3_avg1.groupBy("Year", "city", "ghg").agg(avg("AvgTemperature")).orderBy("Year").show(26)

        case 6 =>  // =========================================== Query 6 ==============================================

          println("Pick a month of the year (e.g. January = 01)")
          val month = readLine()
          println("Pick a City to observe.\n1. Anchorage\n2. Amsterdam\n3. Shanghai")
          val city = readLine()
          if (city == "1") {
            val df1_MM = df1.select("*").where(s"month = $month").where("AvgTemperature != -99.0")
            val df1_fix = df1_MM.withColumn("AvgTemperature",col("AvgTemperature").cast(FloatType))
            df1_fix.groupBy("year").agg(max("AvgTemperature")).orderBy("year").show(26)
            df1_fix.groupBy("year").agg(min("AvgTemperature")).orderBy("year").show(26)
          } else if (city == "2") {
            val df2_MM = df2.select("*").where(s"month = $month").where("AvgTemperature != -99.0")
            val df2_fix = df2_MM.withColumn("AvgTemperature",col("AvgTemperature").cast(FloatType))
            df2_fix.groupBy("year").agg(max("AvgTemperature")).orderBy("year").show(26)
            df2_fix.groupBy("year").agg(min("AvgTemperature")).orderBy("year").show(26)
          } else if (city == "3") {
            val df3_MM = df3.select("*").where(s"month = $month").where("AvgTemperature != -99.0")
            val df3_fix = df3_MM.withColumn("AvgTemperature",col("AvgTemperature").cast(FloatType))
            df3_fix.groupBy("year").agg(max("AvgTemperature")).orderBy("year").show(26)
            df3_fix.groupBy("year").agg(min("AvgTemperature")).orderBy("year").show(26)
          } else {
            println("That is not a valid input.")
          }
      }
    } catch {
      case e: InputMismatchException => println("Non-Integer entered. Exiting menu")
      case e: Exception => e.printStackTrace
    }
  }
}
