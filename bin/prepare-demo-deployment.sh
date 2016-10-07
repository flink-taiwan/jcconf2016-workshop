#!/usr/bin/env bash

PROJECT_ROOT=`pwd | sed 's%\(.*/jcconf2016-workshop\)/.*%\1%'`

if [ ! -d "$PROJECT_ROOT/deploy" ]; then
    mkdir -p "$PROJECT_ROOT/deploy"
fi

cd "$PROJECT_ROOT/deploy"

if [ ! -d "$PROJECT_ROOT/deploy/influxdb" ]; then
    wget http://dl.influxdata.com/influxdb/releases/influxdb-1.0.0_linux_amd64.tar.gz
    tar -xvf influxdb-1.0.0_linux_amd64.tar.gz
    mv influxdb-1.0.0-1 influxdb
    rm influxdb-1.0.0_linux_amd64.tar.gz
fi

if [ ! -d "$PROJECT_ROOT/deploy/grafana" ]; then
    wget http://grafanarel.s3.amazonaws.com/builds/grafana-3.1.1-1470047149.linux-x64.tar.gz
    tar -xvf grafana-3.1.1-1470047149.linux-x64.tar.gz
    mv grafana-3.1.1-1470047149 grafana
    rm grafana-3.1.1-1470047149.linux-x64.tar.gz
fi
