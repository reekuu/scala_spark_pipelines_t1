# Работа с данными в Hadoop и Postgresql

## Описание проекта

Данный проект включает в себя серию задач для работы с данными в Hadoop и Postgresql, используя Apache Spark для выполнения ETL-операций. Задачи включают загрузку данных из таблиц Postgresql в Hadoop, создание широкой таблицы из этих данных, а также создание и сохранение отфильтрованных данных по определенным критериям.

## Исходная задача

1. Загрузить три таблицы (`card`, `person`, `person_adress`) из Postgresql в соответствующую базу данных в Hadoop.
2. Создать одну широкую таблицу из загруженных данных.
3. Сохранить в отдельную таблицу данные всех женщин старше 50 лет с счетом более 3000 из провинции Квебек (QC), у которых почтовый индекс начинается на `8`.

## Инструкции по запуску

Для выполнения заданий необходимо использовать следующие команды `spark-submit`:

```bash
spark-submit --class homework5.DbTableLoader \
--master yarn \
--deploy-mode cluster \
--executor-memory 3G \
--num-executors 3 \
SparkCluster-1.0-SNAPSHOT-jar-with-dependencies.jar <имя_таблицы>
```

Выполнить данную команду для каждой из трех таблиц `card`, `person`, `person_adress`.

Для создания широкой таблицы используйте команду:

```bash
spark-submit --class homework5.CreateWideTable \
--master yarn \
--deploy-mode cluster \
--executor-memory 3G \
--num-executors 3 \
SparkCluster-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Для сохранения отфильтрованных данных выполните:

```bash
spark-submit --class homework5.CreateFilteredTable \
--master yarn \
--deploy-mode cluster \
--executor-memory 3G \
--num-executors 3 \
SparkCluster-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Описание Scala-скриптов

- `DbTableLoader.scala`: Загружает данные из таблицы Postgresql в Hadoop, используя параметры JDBC.
- `CreateWideTable.scala`: Создает широкую таблицу, соединяя данные из трех таблиц в Hadoop.
- `CreateFilteredTable.scala`: Фильтрует данные по заданным критериям и сохраняет результат в новой таблице.
