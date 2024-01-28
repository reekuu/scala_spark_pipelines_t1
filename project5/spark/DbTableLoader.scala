package homework5

import org.apache.spark.sql.{SaveMode, SparkSession}

object DbTableLoader {

  def main(args: Array[String]): Unit = {

    val table_name = args(0)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://10.4.107.34:5432/testdb")
      .option("dbtable", table_name)
      .option("user", "testuser")
      .option("password", "testuser")
      .load()

    jdbcDF.repartition(9).write.mode(SaveMode.Overwrite).saveAsTable("student52." + table_name)

    spark.close()
  }
}
