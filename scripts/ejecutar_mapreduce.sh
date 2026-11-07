#!/bin/bash
# Compila y ejecuta los tres analisis MapReduce dentro del contenedor "namenode".
# Uso (desde la raiz del repositorio, con el cluster HDFS arriba):
#   docker cp . namenode:/tmp/mr
#   docker exec namenode bash /tmp/mr/scripts/ejecutar_mapreduce.sh
set -e

WORK=/tmp/mr
ENTRADA=/data/input/osb_ofertasrv_sedes-red-publica-reps.csv
SALIDA=/data/mapreduce

cd "$WORK"
mkdir -p classes logs resultados

echo "== Compilacion =="
javac -encoding UTF-8 -cp "$(hadoop classpath)" -d classes src/ConteoSedes.java
jar cf conteosedes.jar -C classes .
ls -l conteosedes.jar

echo "== Preparacion de HDFS =="
hdfs dfs -rm -r -f "$SALIDA"
hdfs dfs -mkdir -p "$SALIDA"

for modo in localidad subred zona nivel; do
  echo "== Job: $modo =="
  hadoop jar conteosedes.jar bigdata.mapreduce.ConteoSedes \
      -D mapreduce.framework.name=local \
      "$ENTRADA" "$SALIDA/$modo" "$modo" > "logs/job_$modo.log" 2>&1
  hdfs dfs -ls "$SALIDA/$modo"
  hdfs dfs -cat "$SALIDA/$modo/part-r-*" > "resultados/resultado_$modo.txt"
done

echo "== Listo =="
