package homework8

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object CombineWideTable {

  def main(args: Array[String]): Unit = {

    val schema = args(0)
    val tableName = args(1)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    import spark.implicits._

    // Прочитаем Hive-таблицы
    val cardDf = spark.table(s"$schema.card")
    val personDf = spark.table(s"$schema.person")
    val addressDf = spark.table(s"$schema.person_adress")
    val accountDf = spark.table(s"$schema.account")

    // Соединим таблицы
    val wideDf = cardDf
      .join(personDf, Seq("guid", "ts_created"))
      .join(addressDf, Seq("guid", "ts_created"))
      .join(accountDf, Seq("guid", "ts_created"))
      .withColumn("year", year($"ts_created"))
      .withColumn("month", month($"ts_created"))
      .withColumn("day", dayofmonth($"ts_created"))
      .withColumn("hour", hour($"ts_created"))

    // Сохраним "широкую" таблицу с партиционированием по датам на каждый час
    wideDf
      .coalesce(1)
      .write.mode(SaveMode.Append)
      .partitionBy("year", "month", "day", "hour")
      .saveAsTable(s"$schema.$tableName")

    spark.close()
  }
}
