#!/bin/bash

java hdfs_test $1 $2 $3 $4 > output.txt &

pid=$!

tail -f output.txt | while read line
do
    echo "[$line]"
    if [[ "$line" == *"exit"* ]]; then
        # sleep 0.01
        # kill -9 $pid
        break
    fi
done