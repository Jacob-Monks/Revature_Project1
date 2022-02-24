import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.Scanner
import scala.io.StdIn._
import java.util.InputMismatchException
import DataframeCreation._
import login._

import scala.annotation.tailrec
import scala.sys.exit

object Project1_app {

  def main(args: Array[String]): Unit = {

    println("Hello")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val scanner = new Scanner(System.in)
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ClimateChange")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    println("created spark session")

    var caseInt = 0
    val condition = 1
    val statusSet = UserLogin(spark)
    while(condition == 1) {
      println("Please select an option.\n")
      if (statusSet == "admin")
      {
        println("1) Average between years of temperature.\n2) Highest and lowest temperature of any given year." +
          "\n3) Pick a date in the future for average temperature guess.\n4) Average temp per year for each country compared to CO2 emissions." +
          "\n5) Average temp per year for each country compared to GHG emissions.\n6) Highest and lowest temperature comparisons by month." +
          "\n7) Exit Application.")
        caseInt = getNumberInput
        if (caseInt == 1 || caseInt == 2 || caseInt == 3 || caseInt == 4 || caseInt == 5 || caseInt == 6) {
          createDataframe(spark, caseInt)
        } else if (caseInt == 7) {
          exit
        } else {
          println("Invalid input.")
        }
      }
      else if (statusSet == "user"){
        println("1) Average between years of temperature.\n2) Highest and lowest temperature of any given year." +
          "\n3) Pick a date in the future for average temperature guess.\n4) Exit Application.")
        caseInt = getNumberInput
        if (caseInt == 1 || caseInt == 2 || caseInt == 3) {
          createDataframe(spark, caseInt)
        } else if (caseInt == 4) {
          exit
        } else {
          println("Invalid input.")
        } //end if
      } //end if
    } //end while
  } //end main

  @tailrec
  def getNumberInput: Int = {
    try {
      var inputInt = readInt()
      while(inputInt < 1 || inputInt > 7) {
        inputInt = readInt()
      }
      inputInt
    }catch {
      case _: NumberFormatException =>
        println("Please Enter a valid number:")
        getNumberInput
    }
  }
} //end Project1_app