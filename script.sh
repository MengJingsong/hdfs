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

parallel_put() {
        start=$1
        end=$2
        step=$3
        hdfs_dir=$4

        i=$start
        while [ $i -lt $end ]
        do
                j=$i
                ps=()
                start_time=$(date +%s%N)
                while [[ $j -lt $end ]] && [[ $j -lt $((i+step)) ]]
                do
                        hdfs dfs -put xs-files/file-$j $hdfs_dir &
                        p=$!
                        ps+=($!)
                        ((j += 1))
                done
                for p in ${ps[*]}; do
                        wait $p
                done
                end_time=$(date +%s%N)
                ms=$(($((end_time - start_time)) / 1000000))
                echo "put [$i - $((i+step))] have done, time duration: $ms ms"
                ((i += $step))
        done
}

global_start_time=$(date +%s%N)
case $1 in
        "local-files")
                create_local_files $2 $3
                ;;
        "hdfs-dirs")
                create_hdfs_dirs $2 $3
                ;;
        "hdfs-put")
                parallel_put $2 $3 $4 $5
                ;;
        *)
                echo "invalid argument $1"
                ;;
esac
global_end_time=$(date +%s%N)
global_ms=$(($((global_end_time - global_start_time)) / 1000000))
echo "total time duration: $global_ms ms"
