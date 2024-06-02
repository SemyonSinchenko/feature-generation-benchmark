#!/usr/bin/bash
COMET_JAR=datafusion-comet/spark/target/comet-spark-spark3.4_2.12-0.1.0-SNAPSHOT.jar

spark-submit \
    --jars $COMET_JAR \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.exec.all.enabled=true \
    --conf spark.comet.explainFallback.enabled=true \
    impl/pyspark-comet-case-when.py \
    $1
