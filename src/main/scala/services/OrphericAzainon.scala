package services
import org.apache.spark.sql.{DataFrame, SparkSession}

object OrphericAzainon {

  def readFile(spark: SparkSession, format : String, header: String, delim: String, path: String) = {
    spark
      .read
      .format(format)
      .option("header", header)
      .option("delimiter", delim)
      .load(path)
  }

  def writeDataframeinHive(df: DataFrame): Unit = {
    df.write.format("parquet").mode("overwrite")
      .option("path", "/user/ism_student4/m2_bigdata_2022/SparkFinalProject/orphericAzainon").saveAsTable("ism_m2_2022_examspark.orpheric_azainon")
  }
}
