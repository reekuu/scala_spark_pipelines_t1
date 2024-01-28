package homework8

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

object DownloadCsvFiles {

  def main(args: Array[String]): Unit = {

    val filePath = args(0)
    val schema = args(1)
    val tableName = args(2)
    val timeStamp = args(3)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    // Скачаем CSV-файлы
    val filesDF = spark.read
      .option("header", "true")
      .csv(s"hdfs://$filePath")

    // Сохраним в Hive-таблицу добавив таймстамп
    filesDF
      .withColumn("ts_created", lit(timeStamp))
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(s"$schema.$tableName")

    spark.close()
  }
}
