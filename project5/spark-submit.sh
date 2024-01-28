spark-submit --class homework5.DbTableLoader \
--master yarn \
--deploy-mode cluster \
--executor-memory 3G \
--num-executors 3 \
SparkCluster-1.0-SNAPSHOT-jar-with-dependencies.jar card

spark-submit --class homework5.DbTableLoader \
--master yarn \
--deploy-mode cluster \
--executor-memory 3G \
--num-executors 3 \
SparkCluster-1.0-SNAPSHOT-jar-with-dependencies.jar person

spark-submit --class homework5.DbTableLoader \
--master yarn \
--deploy-mode cluster \
--executor-memory 3G \
--num-executors 3 \
SparkCluster-1.0-SNAPSHOT-jar-with-dependencies.jar person_adress

spark-submit --class homework5.CreateWideTable \
--master yarn \
--deploy-mode cluster \
--executor-memory 3G \
--num-executors 3 \
SparkCluster-1.0-SNAPSHOT-jar-with-dependencies.jar

spark-submit --class homework5.CreateFilteredTable \
--master yarn \
--deploy-mode cluster \
--executor-memory 3G \
--num-executors 3 \
SparkCluster-1.0-SNAPSHOT-jar-with-dependencies.jar