import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Project1_app {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("created spark session")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")

    //when creating a new table, change the name of the table otherwise an error will appear that table already exists.

    //table of Bev_Branch will be called Branch

    //spark.sql("create table Branch(Beverage String, Branch String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE Branch")

    //table of Bev_ConsCount will be called Cons

    //spark.sql("create table Cons(Beverage String, Count Int) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Conscount.txt' INTO TABLE Cons")

    //==============Scenario 1======================
    println("Total Consumers in Branch1")
    spark.sql("SELECT SUM(Count) FROM Branch JOIN Cons ON Branch.Beverage = Cons.Beverage WHERE Branch = 'Branch1'").show()
    println("Total Consumers in Branch2")
    spark.sql("SELECT SUM(Count) FROM Branch JOIN Cons ON Branch.Beverage = Cons.Beverage WHERE Branch = 'Branch2'").show()
    //spark.sql("SELECT * FROM Cons").show(/*number of rows*/)
  }
}
