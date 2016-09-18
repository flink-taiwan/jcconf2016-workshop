#!/usr/bin/env bash

PROJECT_ROOT=`pwd | sed 's%\(.*/jcconf2016-workshop\)/.*%\1%'`

if [ ! -d "$PROJECT_ROOT/deploy" ]; then
    mkdir -p "$PROJECT_ROOT/deploy"
fi

cd "$PROJECT_ROOT/deploy"

if [ ! -d "$PROJECT_ROOT/deploy/flink" ]; then
    wget http://ftp.twaren.net/Unix/Web/apache/flink/flink-1.1.2/flink-1.1.2-bin-hadoop1-scala_2.10.tgz
    tar -xvf flink-1.1.2-bin-hadoop1-scala_2.10.tgz
    mv flink-1.1.2 flink
    rm flink-1.1.2-bin-hadoop1-scala_2.10.tgz
fi