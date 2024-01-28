package homework6

import org.apache.spark.sql.SparkSession

object CountTableRows {

  def main(args: Array[String]): Unit = {
    val tableName = args(0)
    val outputPath = args(1)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val count = spark.sql(s"SELECT COUNT(*) as count FROM $tableName")
    val countAsString = count.withColumn("count", count.col("count").cast("string"))

    countAsString.coalesce(1).write.mode("overwrite").text(s"hdfs://$outputPath")
    spark.close()
  }
}
