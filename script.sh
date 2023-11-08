#!/bin/bash

# set -x

create_local_files() {
        start=$1
        end=$2

        i=$start
        while [ $i -lt $end ]
        do
                echo $i > xs-files/file-$i
                echo "$i is done"
                ((i += 1))
        done
}

create_hdfs_dirs() {
        i=$1

        n=$2
        while [ $i -lt $n ]
        do
                echo "xs-files-dir-$i"
                hdfs dfs -mkdir xs-files-dir-$i
                ((i += 1))
        done
}

global_start_time=$(date +%s%N)
case $1 in
        "local_files")
                create_local_files $2 $3
                ;;
        "hdfs_dirs")
                create_hdfs_dirs $2 $3
                ;;
        *)
                echo "invalid argument $1"
                ;;
esac
global_end_time=$(date +%s%N)
global_ms=$(($((global_end_time - global_start_time)) / 1000000))
echo "total time duration: $global_ms ms"
