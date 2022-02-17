import org.apache.spark.sql.SparkSession
import DataframeCreation._
import login._
import scala.io.StdIn._
import org.apache.spark.SparkContext
object Project1_app {

  def main(args: Array[String]): Unit = {

    println("Hello")

    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    //create spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ClimateChange")
      //.enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("created spark session")
    UserLogin(spark)
    createDataframe(spark)

  }
}
