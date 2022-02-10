import org.apache.spark.sql.SparkSession
import scala.io.StdIn._
import org.apache.spark.SparkContext
object Project1_app {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val check: Int = 1
    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("created spark session\n\nWelcome to the Beverage Market Analytics Service!\n\n")

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

    while(check==1) {
      println("Please select an option.")
      println("1. See number of consumers")
      println("2. See most/least popular beverages")
      println("3. See available beverages")
      println("4. Partition the data tables")
      println("5. Add a Comment and delete a row")
      println("6. Make market prediction")
      val sel = readLine()
      if (sel == "1") {
        //=========================== Scenario 1 ======================
        println("Total Consumers in Branch1")
        println("Please wait a moment...")
        spark.sql(
          """
            |SELECT SUM(Count)
            |FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage
            |WHERE Branch = 'Branch1'
            |""".stripMargin).show(/*number of rows*/)
        println("Total Consumers in Branch2")
        spark.sql(
          """
            |SELECT SUM(Count)
            |FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage
            |WHERE Branch = 'Branch2'
            |""".stripMargin).show(/*number of rows*/)
      }
      else if (sel == "2") {
        //============================ Scenario 2 ======================
        println("Most consumed beverage in Branch1")
        println("Please wait a moment...")
        spark.sql(
          """
            |SELECT Branches.Beverage, SUM(Count) AS Total_Sold
            |FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage
            |WHERE Branch = 'Branch1' GROUP BY Branches.Beverage ORDER BY SUM(Count) DESC
            |""".stripMargin).show(1)
        println("Least consumed beverage in Branch2")
        spark.sql(
          """
            |SELECT Branches.Beverage, SUM(Count) AS Total_Sold
            |FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage
            |WHERE Branch = 'Branch2' GROUP BY Branches.Beverage ORDER BY SUM(Count) ASC
            |""".stripMargin).show(1)
        println("Average consumed beverage in Branch2")
        spark.sql(
          """
            |SELECT ROUND(AVG(Consumed)) AS Average
            |FROM
            |(SELECT Branches.Beverage, SUM(Count) AS Consumed
            |FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage
            |WHERE Branch = 'Branch2' GROUP BY Branches.Beverage)
            |""".stripMargin).show()
      }
      else if (sel == "3") {
        //============================= Scenario 3 ======================
        println("Beverages available in Branch 1")
        println("Please wait a moment...")
        spark.sql(
          """
            |SELECT Beverage FROM Branches
            |WHERE Branch = 'Branch1' GROUP BY Beverage
            |""".stripMargin).show()
        println("Beverages available in Branch 8")
        spark.sql(
          """
            |SELECT Beverage FROM Branches
            |WHERE Branch = 'Branch8' GROUP BY Beverage
            |""".stripMargin).show(50)
        println("Beverages available in Branch 10")
        spark.sql(
          """
            |SELECT Beverage FROM Branches
            |WHERE Branch = 'Branch10' GROUP BY Beverage
            |""".stripMargin).show()
        println("Common Beverages in Branch 4 and Branch 7")
        spark.sql(
          """
            |(SELECT Beverage FROM Branches
            |WHERE Branch = 'Branch4')
            |INTERSECT
            |(SELECT Beverage FROM Branches
            |WHERE Branch = 'Branch7')
            |""".stripMargin).show(100)
      }
      else if (sel == "4") {
        //============================== Scenario 4 ======================
        spark.sql(
          """
            |CREATE TABLE IF NOT EXISTS Branch_Part(Beverage String)
            |PARTITIONED BY (Branch String) row format delimited fields terminated by ','
            |""".stripMargin)
        spark.sql(
          """
            |LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt'
            |OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch1')
            |""".stripMargin)
        spark.sql(
          """
            |LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt'
            |OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch2')
            |""".stripMargin)
        spark.sql(
          """
            |LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt'
            |OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch3')
            |""".stripMargin)
        spark.sql(
          """
            |LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt'
            |OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch4')
            |""".stripMargin)
        spark.sql(
          """
            |LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt'
            |OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch5')
            |""".stripMargin)
        spark.sql(
          """
            |LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt'
            |OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch6')
            |""".stripMargin)
        spark.sql(
          """
            |LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt'
            |OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch7')
            |""".stripMargin)
        spark.sql(
          """
            |LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt'
            |OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch8')
            |""".stripMargin)
        spark.sql(
          """
            |LOAD DATA LOCAL INPATH 'input/Bev_Branch.txt'
            |OVERWRITE INTO TABLE Branch_Part PARTITION(Branch = 'Branch9')
            |""".stripMargin)
        spark.sql("DESCRIBE FORMATTED branch_part").show()
      }
      else if (sel == "5") {
        //================================= Scenario 5 ======================
        //adding a note

        spark.sql("ALTER TABLE branches Set TBLPROPERTIES('My Notes' = 'Example Comment')")
        spark.sql("SHOW TBLPROPERTIES branches").show()
        spark.sql("DESCRIBE FORMATTED branches").show()

        //deleting a row

        spark.sql(
          """
            |CREATE TABLE IF NOT EXISTS staging_table(Beverage String, Branch String)
            |row format delimited fields terminated by ','
            |""".stripMargin)
        //spark.sql("INSERT INTO TABLE staging_table VALUES ('Cold_LATTE', 'Branch1')")
        spark.sql("SELECT * FROM branches ORDER BY branch, beverage").show()
        println("Record(s) to be deleted:")
        spark.sql("SELECT * FROM staging_table").show()
        spark.sql(
          """
            |(SELECT * FROM branches)
            |EXCEPT
            |(SELECT * FROM staging_table) ORDER BY branch, beverage
            |""".stripMargin).show()
      }
      else if (sel == "6") {
        //=========================== Scenario 6 - Future Query ======================
        println("Which branch would you like to see?")
        println("1.\t2.\t3.\t4.\t5.\t6.\t7.\t8.\t9.")
        val branch: String = readLine()
        if (branch == "1"|| branch == "2"|| branch == "3"|| branch == "4"|| branch == "5"|| branch == "6"|| branch == "7"|| branch == "8"|| branch == "9") {
          println(s"\nThese beverages are the most likely to continue succeeding in Branch $branch:")
          println("Please wait a moment...")
          spark.sql(
            s"""
               |SELECT Branches.Beverage, LAST(Count) AS Latest_Sales
               |FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage
               |WHERE Branch = 'Branch$branch' GROUP BY Branches.Beverage ORDER BY LAST(Count) DESC
               |""".stripMargin).show(3)
          println(s"\nThese beverages are the most likely to fail in Branch $branch:")
          spark.sql(
            s"""
              |SELECT Branches.Beverage, LAST(Count) AS Latest_Sales
              |FROM Branches JOIN Cons ON Branches.Beverage = Cons.Beverage
              |WHERE Branch = 'Branch$branch' GROUP BY Branches.Beverage ORDER BY LAST(Count) ASC
              |""".stripMargin).show(3)
        }
        else {
          println("Invalid Selection, returning to options.")
        }
      }
      else {
        println("Please make a valid selection.")
      }
    }
  }
}
