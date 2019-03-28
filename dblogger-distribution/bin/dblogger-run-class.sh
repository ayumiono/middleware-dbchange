#!/bin/bash
base_dir=$(dirname $0)/..
LOG_DIR="$base_dir/logs"
for file in "$base_dir"/libs/*;
do
  CLASSPATH="$CLASSPATH":"$file"
done
DBLOGGER_LOG4J_OPTS="-Dlogback.configurationFile=file:$base_dir/config/tools-logback.xml"
java $DBLOGGER_LOG4J_OPTS -cp $CLASSPATH "$@"