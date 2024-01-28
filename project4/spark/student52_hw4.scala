import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object student52_hw4 extends App {
  val spark = SparkSession.builder()
    .appName("student52_hw4")
    .master("local[1]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // Первое задание
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("file:///path/to/file/country_info.csv")

  val aggDf = df.groupBy($"country")
    .agg(
      count("*"),
      max($"name")
    )

  aggDf.repartition(10).explain()

  // Второе задание
  // 1. План начинается с чтения файла Top_Songs, выбираются только колонки Artist Name и Song Duration;
  // 2. Top_Songs фильтруется от пропусков в Artist Name, партиционируется и сортируется по полю Artist Name;
  // 3. Читается файл Artists, выбирается только колонка Name, фильтруется от пропусков и партиционируется;
  // 4. Artists сортируется по полю Name;
  // 5. INNER JOIN по совпадению полей `Artist Name` и `Name`;
  // 6. Пре-агрегация на партициях для подсчета суммарной длительности песен артиста;
  // 7. Финальная агрегация, когда результаты пре-агрегации объединяются.

  val topSongsDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/Top_Songs_US.csv")

  val artistsDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/Artists.csv")

  val joinedDF = topSongsDF
    .join(artistsDF, artistsDF("Name") === topSongsDF("Artist Name"))
    .filter($"Followers" > 50_000_000)

  joinedDF
    .groupBy("Artist Name")
    .agg(sum("Song Duration").as("Total Duration"))
    .explain()

  // Третье задание
  // 1. План начинается с чтения файла Artists, выбирается только колонка Name, фильтруется от пропусков,
  // фильтруется по количеству подписчиков, партиционируется и бродкастится на ноды;
  // 2. Читается файл Top_Songs, выбираются только колонки Artist Name и Song Duration,
  // фильтруется от пропусков, выполняется BroadcastHashJoin с Artists
  // 3. Пре-агрегация на партициях для подсчета суммарной длительности песен артиста,
  // финальная агрегация,когда результаты пре-агрегации объединяются.
  //
  // SortMergeJoin требует сортировки и перемешивания данных,
  // BroadcastHashJoin работает эффективнее и сокращает время выполняется с 40мс до 30мс.

  val broadcastJoinedDF = topSongsDF.as("t")
    .join(broadcast(artistsDF).as("a"), $"a.Name" === $"t.Artist Name")
    .filter($"Followers" > 50_000_000)

  broadcastJoinedDF
    .groupBy("Artist Name")
    .agg(sum("Song Duration").as("Total Duration"))
    .explain()
}
