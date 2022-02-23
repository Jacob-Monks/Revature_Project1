import scala.io.StdIn.{readLine, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object login {
  def UserLogin(spark: SparkSession): String = {
    print("Please enter your credentials:\nUsername:\t")
    val user = readLine()
    print("Password:\t")
    val pass = readLine()
    val df4 = spark.read.format("csv").option("header", "true").load("input/user_data.csv")
    val checkUser = df4.filter(col("Username").contains(user)).toDF
    if(!checkUser.take(1).isEmpty) {
      val dfuser = df4.select("*").where(df4("Username")===user)
      val status = dfuser.first.getString(2)
      if (user == dfuser.first.getString(0) & pass == dfuser.first.getString(1)) {
        println("\nWelcome to the Climate Analysis Tool")
        return status
      } else {
        println("Username/Password is incorrect.")
        UserLogin(spark)
      }
    } else {
      println("Username does not exist")
      UserLogin(spark)
    }
  }
}