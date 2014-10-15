#!/bin/sh
LIB=$(pwd)/lib
for jarf in $LIB/*.jar
do
CLPA=$CLPA:$jarf
HCLPA=$HCLPA,$jarf
done
CLPA=${CLPA:1:${#CLPA}-1}
HCLPA=${HCLPA:1:${#HCLPA}-1}
CLPD=$CLPA:/home/tuan.tran/.m2/repository/de/l3s/wikipedia/wikianalysis/0.1-SNAPSHOT/wikianalysis-0.1-SNAPSHOT.jar
HCLPDA=$HCLPA,/home/tuan.tran/.m2/repository/de/l3s/wikipedia/wikianalysis/0.1-SNAPSHOT/wikianalysis-0.1-SNAPSHOT.jar
export HADOOP_CLASSPATH="$CLPD:$HADOOP_CLASSPATH"
export HADOOP_MAPRED_HOME="/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/hadoop-0.20-mapreduce"
hadoop jar $1 $2 -libjars ${HCLPDA} -D mapred.output.compress=true -D mapred.output.compression.type=BLOCK -D mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec -D mapred.compress.map.output -D mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec -D mapred.task.timeout=1200000 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14} ${15} ${16} ${17} ${18} ${19} ${20} ${21}
