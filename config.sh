#!/bin/bash

HADOOP_CLASSPATH=$(find $1 -name '*.jar' | xargs echo | tr ' ' ':')

export CLASSPATH=$CLASSPATH:$HADOOP_CLASSPATH
