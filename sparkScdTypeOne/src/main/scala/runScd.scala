import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions._

object runScd {

  val URL = "jdbc:mysql://localhost:3306/employees"
  val DRIVER = "com.mysql.cj.jdbc.Driver"
  val USER = "root"
  val PASS = "Neoman#02"
  val FORMAT = "jdbc"

  val logger = Logger("Root")

  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("ScdTypeOne")
      .master("local[*]")
      .getOrCreate()
    spark
  }

  def readFromDbEmployee(tablename: String): DataFrame = {
    val dfcreate = createSparkSession()
      .read.format(FORMAT)
      .option("url", URL)
      .option("driver", DRIVER)
      .option("dbtable", tablename)
      .option("user", USER)
      .option("password", PASS)
      .load()
    dfcreate
  }

  def writtoDbEmployeeTgt(tablenameTarget: String, writeDataFrame: DataFrame): Unit = {
    writeDataFrame
      .write.format(FORMAT)
      .mode("append")
      .option("url", URL)
      .option("driver", DRIVER)
      .option("dbtable", tablenameTarget)
      .option("user", USER)
      .option("password", PASS)
      .save()
  }

  def renameColumnsInDf(renameDataFame: DataFrame,appendStr:String): DataFrame = {
    val renamed = renameDataFame
      .withColumnRenamed("emp_no", "emp_no_"+appendStr)
      .withColumnRenamed("birth_date", "birth_date_"+appendStr)
      .withColumnRenamed("first_name", "first_name_"+appendStr)
      .withColumnRenamed("last_name", "last_name_"+appendStr)
      .withColumnRenamed("gender", "gender_"+appendStr)
      .withColumnRenamed("hire_date", "hire_date_"+appendStr)
    renamed
  }

  def lookupOnTgt(sourceDf: DataFrame, targetDf: DataFrame): DataFrame = {
    val joinedDf = sourceDf.join(targetDf, col("emp_no_tgt") === col("emp_no_src"), "left")
    joinedDf
  }

  def main(args: Array[String]): Unit = {

    //Reading employees data from mysql database
    val emp_src = readFromDbEmployee("employees")
    //emp_src.show()

    //Reading data from employee target table
    val emp_tgt = readFromDbEmployee("employee_tgt")
    // emp_tgt.show()

    //Renaming employee source and target columns
    val emp_src_renamed = renameColumnsInDf(emp_src,"src")
    val emp_tgt_renamed = renameColumnsInDf(emp_tgt,"tgt")

    //Creating lookup on the target table.
    val lookupedData = lookupOnTgt(emp_src_renamed,emp_tgt_renamed)
    //lookupedData.show()

    //Flagging the records for insert or update
    val firstInsert = lookupedData
      .withColumn("ins_flg",
        when(
          (col("emp_no_src")<=>col("emp_no_tgt")) || col("emp_no_tgt").isNull,"Y"
        ).otherwise("Na")
      )

    //firstInsert.show()
    //Renaming the datafrom before writing to databse

    val empInsert = firstInsert
      .filter(col("ins_flg")==="Y")
      .select(
        col("emp_no_src").alias("emp_no"),
        col("birth_date_src").alias("birth_date"),
        col("first_name_src").alias("first_name"),
        col("last_name_src").alias("last_name"),
        col("gender_src").alias("gender"),
        col("hire_date_src").alias("hire_date")
      )
    //Writing the records to database
    writtoDbEmployeeTgt("employee_tgt",empInsert)

  }
}