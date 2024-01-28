import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object student52_hw7 extends App {
  val spark = SparkSession.builder()
    .appName("student52_hw7")
    .master("local[4]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  // Сгенерируем Датафрейм с 50% перекосом в сторону ключа commonKey
  // Размер Датафрейма подберем так, чтобы программа не падала по памяти при локальном запуске
  val skewedData = Seq.fill(10_000_000)(("commonKey", 1)) ++
    (10_000_001 to 20_000_000).map(i => (s"uniqueKey$i", 1))
  val skewedDF = spark.sparkContext.parallelize(skewedData).toDF("key", "value")

  // Выполним агрегацию без соли - выполняется 2 минуты
  skewedDF.groupBy("key").count().orderBy($"count".desc).show()

  // Выполним аггрегацию с солью - выполняется 1 минуту
  val saltedDF = skewedDF.withColumn("salted_key", concat($"key", lit("#"), (rand() * 4).cast("int")))
  val saltedAggDF = saltedDF.groupBy("salted_key").count()
  val unSaltedDF = saltedAggDF.groupBy(substring_index($"salted_key", "#", 1).alias("key")).count()
  unSaltedDF.orderBy($"count".desc).show()

  spark.stop()
}