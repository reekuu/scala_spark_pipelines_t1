package homework0

import org.apache.spark.sql.SparkSession

object HelloWorldWriter {

  def main(args: Array[String]): Unit = {
    val outputPath = "/output/text.txt"

    val spark = SparkSession
      .builder()
      .appName("HelloWorldWriter")
      .getOrCreate()

    val helloWorldRDD = spark.sparkContext.parallelize(Seq("Hello, world!"))
    helloWorldRDD.coalesce(1).saveAsTextFile(s"hdfs://$outputPath")
    spark.close()
  }
}
