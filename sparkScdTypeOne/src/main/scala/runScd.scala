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

  def renameColumnsInDf(renameDataFame: DataFrame, appendStr: String): DataFrame = {
    val renamed = renameDataFame
      .withColumnRenamed("emp_no", "emp_no_" + appendStr)
      .withColumnRenamed("birth_date", "birth_date_" + appendStr)
      .withColumnRenamed("first_name", "first_name_" + appendStr)
      .withColumnRenamed("last_name", "last_name_" + appendStr)
      .withColumnRenamed("gender", "gender_" + appendStr)
      .withColumnRenamed("hire_date", "hire_date_" + appendStr)
    renamed
  }

  def lookupOnTgt(sourceDf: DataFrame, targetDf: DataFrame, srcCol: String, tgtCol: String): DataFrame = {
    val joinedDf = sourceDf.join(targetDf, col(tgtCol) === col(srcCol), "left")
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
    val emp_src_renamed = renameColumnsInDf(emp_src, "src")
    val emp_tgt_renamed = renameColumnsInDf(emp_tgt, "tgt")

    //Creating lookup on the target table.
    val lookupedData = lookupOnTgt(emp_src_renamed, emp_tgt_renamed, "emp_no_src", "emp_no_tgt")
    //lookupedData.show()

    //Flagging the records for insert or update
    val firstInsert = lookupedData
      .withColumn("ins_flg",
        when(
          (col("emp_no_src") !== col("emp_no_tgt")) || col("emp_no_tgt").isNull, "Y"
        ).otherwise("Na")
      )

    //firstInsert.show()
    //Renaming the datafrom before writing to databse

    val empInsert = firstInsert
      .filter(col("ins_flg") === "Y")
      .select(
        col("emp_no_src").alias("emp_no"),
        col("birth_date_src").alias("birth_date"),
        col("first_name_src").alias("first_name"),
        col("last_name_src").alias("last_name"),
        col("gender_src").alias("gender"),
        col("hire_date_src").alias("hire_date")
      )
    //Writing the records to database
    writtoDbEmployeeTgt("employee_tgt", empInsert)

    //update employees.employees set first_name="Ashish" where emp_no=10081
    //Importing changed source and target for lookup
    val emp_src_chg = readFromDbEmployee("employees")
    val emp_tgt_chg = readFromDbEmployee("employee_tgt")

    //renaming the tgt columns
    val emp_tgt_rm = renameColumnsInDf(emp_tgt_chg, "tgt")

    val lookupedDataOnNewTgt = lookupOnTgt(emp_src_chg, emp_tgt_rm, "emp_no", "emp_no_tgt")

    //lookupedDataOnNewTgt.show()
    // lookupedDataOnNewTgt.filter(col("emp_no")===10081).show()
    def insertFlg(insertDataframe: DataFrame): DataFrame = {
      val returnInsertdf = insertDataframe
        .withColumn("ins",
          when((col("emp_no") !== col("emp_no_tgt")) || col("emp_no_tgt").isNull, "insert")
            .otherwise("NA")
        )
      returnInsertdf
    }

    def updateFlg(insertDataframe: DataFrame): DataFrame = {
      val returnUpdatedf = insertFlg(insertDataframe)
        .withColumn("upd",
          when(
            (col("emp_no") === col("emp_no_tgt")) &&
              (col("first_name") !== col("first_name_tgt")), "update"
          )
            .otherwise("NA")
        )
      returnUpdatedf
    }

    val insupdDf = insertFlg(lookupedDataOnNewTgt)
    //insupdDf.printSchema()

    val updateDf = updateFlg(insupdDf)

    //updateDf.filter(col("emp_no") === 10081).show()

    //Inserted dataframe
    val scd_ins = updateDf
      .filter(col("ins") === "insert")
      .select(
        col("emp_no"),
        col("birth_date"),
        col("first_name"),
        col("last_name"),
        col("gender"),
        col("hire_date")
      )

    //Updated datafram
    val scd_upd = updateDf
      .filter(col("upd") === "update")
      .select(
        col("emp_no"),
        col("birth_date"),
        col("first_name"),
        col("last_name"),
        col("gender"),
        col("hire_date")
      )
    //Overriden data frame
    val scd_overriden = updateDf
      .filter(
        (col("upd") === "update") &&
          (col("ins") === "insert")
      ).select(
      col("emp_no"),
      col("birth_date"),
      col("first_name"),
      col("last_name"),
      col("gender"),
      col("hire_date")
    )
    //Merging all the df as DML is not allowed in spark
    val finalDf = scd_ins.union(scd_upd).union(scd_overriden)
   // finalDf.show()
    writtoDbEmployeeTgt("employee_tgt", finalDf)
  }
}