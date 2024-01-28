package homework8

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

object DownloadPGTables {

  def main(args: Array[String]): Unit = {

    val url = args(0)
    val schema = args(1)
    val tableName = args(2)
    val username = args(3)
    val password = args(4)
    val timeStamp = args(5)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    // Скачаем Postgres-таблицу
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", username)
      .option("password", password)
      .load()

    // Сохраним в Hive-таблицу добавив таймстамп
    jdbcDF
      .withColumn("ts_created", lit(timeStamp))
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(s"$schema.$tableName")

    spark.close()
  }
}
