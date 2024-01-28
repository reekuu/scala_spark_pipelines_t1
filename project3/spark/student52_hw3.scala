import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object student52_hw3 extends App {
  val spark = SparkSession.builder()
    .appName("student52_hw3")
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // 0.1. Создание небольшого датасета
  case class Person(id: Int, name: Option[String])
  val names = Seq(
    Person(1, Some("Дмитрий")),
    Person(2, Some("Аслан")),
    Person(3, None),
    Person(4, Some("Кирилл"))
  )
  val namesDS = spark.createDataset(names)
  namesDS.show()

  // 0.2. Добавление приписки "-id" к колонке name
  val newNamesDS = namesDS.withColumn("name",
    when($"name".isNotNull, concat($"name", lit("-"), $"id"))
  )
  newNamesDS.show()

  // 1. Чтение файла HR_data.csv в DataFrame
  val filepath = "src/main/resources/HR_data.csv"
  val hrDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(filepath)
  hrDf.show()

  // 2. Проставление рейтинга каждому сотруднику
  val windowSpec = Window.partitionBy("education", "gender").orderBy($"avg_training_score".desc)
  val rankedHrDf = hrDf.withColumn("rank",
    when($"education".isNotNull, dense_rank().over(windowSpec)).otherwise(lit("No info"))
  )
  rankedHrDf.show()

  // 3. Dataframe: Группировка по department, education и gender и агрегирование
  val groupedHrDf = hrDf.groupBy("department", "education", "gender")
    .agg(
      when($"education".isNull, lit("No info")).otherwise(min("age")).alias("min_age"),
      when($"education".isNull, lit("No info")).otherwise(max("age")).alias("max_age"),
      when($"education".isNull, lit("No info")).otherwise(round(avg("age"), 2)).alias("avg_age")
    ).orderBy("department", "education", "gender")
  groupedHrDf.show()

  // 4. Dataset: группировка по department, education и gender и агрегирование
  case class Employee(department: String, education: String, gender: String, age: Int)
  val hrDs = hrDf.select("department", "education", "gender", "age").as[Employee]

  val groupedHrDs = hrDs.groupByKey(row => (row.department, row.education, row.gender))
    .agg(
      when(any($"education".isNull), lit("No info")).otherwise(min("age")).alias("min_age").as[String],
      when(any($"education".isNull), lit("No info")).otherwise(max("age")).alias("max_age").as[String],
      when(any($"education".isNull), lit("No info")).otherwise(round(avg("age"), 2)).alias("avg_age").as[String]
    ).select(
      $"key._1".alias("department"),
      $"key._2".alias("education"),
      $"key._3".alias("gender"),
      $"min_age",
      $"max_age",
      $"avg_age"
    ).orderBy("department", "education", "gender")
  groupedHrDs.show()
}
