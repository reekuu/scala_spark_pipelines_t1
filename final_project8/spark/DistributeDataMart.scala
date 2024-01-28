package homework8

import org.apache.spark.sql.{SaveMode, SparkSession}

object DistributeDataMart {

  def main(args: Array[String]): Unit = {

    val url = args(0)
    val schema = args(1)
    val sourceTable = args(2)
    val destinationTable = args(3)
    val username = args(4)
    val password = args(5)
    val timeStamp = args(6)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    // Посчитаем витрину
    val dataMartDF = spark.sql(s"""
      WITH categorized AS (
        SELECT
          year,
          month,
          day,
          hour,
          CASE
            WHEN amount < 500 THEN '<500'
            WHEN amount BETWEEN 500 AND 1000 THEN '500-1000'
            WHEN amount BETWEEN 1001 AND 2000 THEN '1001-2000'
            WHEN amount BETWEEN 2001 AND 3000 THEN '2001-3000'
            WHEN amount BETWEEN 3001 AND 4000 THEN '3001-4000'
            WHEN amount BETWEEN 4001 AND 5000 THEN '4001-5000'
            WHEN amount BETWEEN 5001 AND 6000 THEN '5001-6000'
            ELSE '>6000'
          END AS category,
          IF(gender = 'Male' AND account = 'Has Account in foreign bank', 1, 0) AS mans
        FROM $schema.$sourceTable
        WHERE ts_created = '$timeStamp'
      ),
      total AS (
        SELECT COUNT(*) AS total_count
        FROM $schema.$sourceTable
        WHERE ts_created = '$timeStamp'
      )
      SELECT
        year,
        month,
        day,
        hour,
        c.category,
        ROUND(COUNT(*) / t.total_count * 100, 0) AS weight,
        ROUND(SUM(c.mans) / COUNT(*) * 100, 0) AS mans_weight
      FROM categorized c
      CROSS JOIN total t
      GROUP BY 1, 2, 3, 4, 5, t.total_count""")

    // Сохраним готовую витрину в Hive-таблицу с партиционированием по часам
    dataMartDF
      .coalesce(1)
      .write.mode(SaveMode.Append)
      .partitionBy("year", "month", "day", "hour")
      .saveAsTable(s"$schema.$destinationTable")

    // Сформируем дату для названия таблицы
    val year = timeStamp.substring(0, 4)
    val month = timeStamp.substring(5, 7)
    val day = timeStamp.substring(8, 10)
    val hour = timeStamp.substring(11, 13)

    // Сохраним готовую витрину в Postgres
    dataMartDF
      .drop("year", "month", "day", "hour")
      .write
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", url)
      .option("dbtable", s"$schema.${destinationTable}_$year$month${day}_UTC$hour")
      .option("user", username)
      .option("password", password)
      .mode(SaveMode.Overwrite)
      .save()

    spark.close()
  }
}
