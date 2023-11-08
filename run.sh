#!/bin/bash

if test -f $1.class; then
        rm $1.class
fi

HADOOP_CLASSPATH=$(find /users/jason92/local/hadoop-3.3.6 -name '*.jar' | xargs echo | tr ' ' ':')

# echo $HADOOP_CLASSPATH

# echo "javac -cp ".:$HADOOP_CLASSPATH" $1.java"

javac -cp ".:$HADOOP_CLASSPATH" $1.java

if test -f $1.class; then
        # echo "java -cp ".:$HADOOP_CLASSPATH" $1"
        java -cp ".:$HADOOP_CLASSPATH" $1 $2 $3 $4 $5 $6
fi
