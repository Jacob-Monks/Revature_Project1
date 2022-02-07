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

    //tables for each Bev_Branches file
    //spark.sql("CREATE TABLE BranchA (Beverage String, Branch String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE BranchA")
    //spark.sql("SELECT * FROM BranchA").show()
    //spark.sql("CREATE TABLE BranchB (Beverage String, Branch String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE BranchB")
    //spark.sql("SELECT * FROM BranchB").show()
    //spark.sql("CREATE TABLE BranchC (Beverage String, Branch String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE BranchC")
    //spark.sql("SELECT * FROM BranchC").show()

    //tables for each Bev_Conscount file
    //spark.sql("CREATE TABLE ConsA (Beverage String, Count Int) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE ConsA")
    //spark.sql("SELECT * FROM ConsA").show()
    //spark.sql("CREATE TABLE ConsB (Beverage String, Count Int) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE ConsB")
    //spark.sql("SELECT * FROM ConsB").show()
    //spark.sql("CREATE TABLE ConsC (Beverage String, Count Int) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE ConsC")
    //spark.sql("SELECT * FROM ConsC").show()

    //table of Bev_Branch will be called Branches
    //spark.sql("create table Branches(Beverage String, Branch String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' INTO TABLE Branches")

    //table of Bev_ConsCount will be called Cons
    //spark.sql("create table Cons(Beverage String, Count Int) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Conscount.txt' INTO TABLE Cons")

    //============== Scenario 1 ======================
    /*
    println("Total Consumers in Branch1")
    spark.sql("SELECT SUM(Count) FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage WHERE Branch = 'Branch1'").show(/*number of rows*/)
    println("Total Consumers in Branch2")
    spark.sql("SELECT SUM(Count) FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage WHERE Branch = 'Branch2'").show(/*number of rows*/)
    */
    //============== Scenario 2 ======================
    /*
    println("Most consumed beverage in Branch1")
    spark.sql("SELECT Branches.Beverage, SUM(Count) FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage WHERE Branch = 'Branch1' GROUP BY Branches.Beverage ORDER BY SUM(Count) DESC").show(1)
    println("Least consumed beverage in Branch2")
    spark.sql("SELECT Branches.Beverage, SUM(Count) FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage WHERE Branch = 'Branch2' GROUP BY Branches.Beverage ORDER BY SUM(Count) ASC").show(1)
    println("Average consumed beverage in Branch2")
    spark.sql("SELECT AVG(Consumed) AS Average FROM (SELECT Branches.Beverage, SUM(Count) AS Consumed FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage WHERE Branch = 'Branch2' GROUP BY Branches.Beverage)").show()
    */
    //============== Scenario 3 ======================
    /*
    println("Beverages available in Branch 1")
    spark.sql("SELECT Beverage FROM Branches WHERE Branch = 'Branch1' GROUP BY Beverage").show()
    println("Beverages available in Branch 8")
    spark.sql("SELECT Beverage FROM Branches WHERE Branch = 'Branch8' GROUP BY Beverage").show(50)
    println("Beverages available in Branch 10")
    spark.sql("SELECT Beverage FROM Branches WHERE Branch = 'Branch10' GROUP BY Beverage").show()
    println("Common Beverages in Branch 4 and Branch 7")
    spark.sql("(SELECT Beverage FROM Branches WHERE Branch = 'Branch4') INTERSECT (SELECT Beverage FROM Branches WHERE Branch = 'Branch7')").show(100)
    */
    //============== Scenario 4 ======================
    /*
    spark.sql("CREATE TABLE IF NOT EXISTS Branch_Part(Beverage String) PARTITIONED BY (Branch String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch1')")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch2')")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch3')")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch4')")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch5')")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch6')")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch7')")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch8')")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt' OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch9')")
    spark.sql("SELECT * FROM Branch_Part").show(100)
    */
    //============== Scenario 5 ======================
    spark.sql("ALTER TABLE branches Set TBLPROPERTIES('note' = 'comment')")
    spark.sql("SELECT * FROM branches")
    spark.sql("DESCRIBE FORMATTED branches").show()
    //============== Scenario 6 - Future Query ======================
  }
}
