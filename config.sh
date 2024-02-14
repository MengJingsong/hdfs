#!/bin/bash

HADOOP_CLASSPATH=$(find /users/jason92/hadoop-$1 -name '*.jar' | xargs echo | tr ' ' ':')

export CLASSPATH=$CLASSPATH:$HADOOP_CLASSPATH
