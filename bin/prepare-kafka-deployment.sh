#!/usr/bin/env bash

PROJECT_ROOT=`pwd | sed 's%\(.*/jcconf2016-workshop\)/.*%\1%'`

if [ ! -d "$PROJECT_ROOT/deploy" ]; then
    mkdir -p "$PROJECT_ROOT/deploy"
fi

cd "$PROJECT_ROOT/deploy"

if [ ! -d "$PROJECT_ROOT/deploy/kafka" ]; then
    wget http://apache.stu.edu.tw/kafka/0.9.0.1/kafka_2.11-0.9.0.1.tgz
    tar -xvf kafka_2.11-0.9.0.1.tgz
    mv kafka_2.11-0.9.0.1 kafka
    rm kafka_2.11-0.9.0.1.tgz
fi