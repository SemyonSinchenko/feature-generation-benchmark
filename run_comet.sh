#!/usr/bin/bash
COMET_JAR=datafusion-comet/spark/target/comet-spark-spark3.4_2.12-0.1.0-SNAPSHOT.jar

spark-submit \
    --jars $COMET_JAR \
    --conf spark.sql.adaptive.coalescePartitions.enabled=false \
    --conf spark.driver.extraClassPath=$COMET_JAR \
    --conf spark.executor.extraClassPath=$COMET_JAR \
    --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
    --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
    --conf spark.comet.enabled=true \
    --conf spark.comet.exec.enabled=true \
    --conf spark.comet.exec.all.enabled=true \
    --conf spark.comet.explainFallback.enabled=true \
    --conf spark.comet.exec.shuffle.enabled=true \
    --conf spark.comet.exec.shuffle.mode=native \
    --conf spark.comet.memory.overhead.factor=0.3 \
    --conf spark.comet.rowToColumnar.enabled=true \
    $1 \
    $2
