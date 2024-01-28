package homework5

import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateFilteredTable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val wideDf = spark.sql("select * from student52.wide_table")

    // All women
    // over 50
    // with an account over 3000
    // from the province of Quebec (QC)
    // and a zip code beginning with 8
    val filteredResult = wideDf.filter(
      wideDf("gender") === "Female" &&
        wideDf("age") > 50 &&
        wideDf("amount") > 3000 &&
        wideDf("province") === "QC" &&
        wideDf("zip").startsWith("8")
    )

    filteredResult.write.mode(SaveMode.Overwrite).saveAsTable("student52.filteredResult")

    spark.close()
  }
}
