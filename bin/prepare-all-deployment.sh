#!/usr/bin/env bash

PROJECT_ROOT=`pwd | sed 's%\(.*/jcconf2016-workshop\)/.*%\1%'`

bash "$PROJECT_ROOT/bin/prepare-flink-deployment.sh"
bash "$PROJECT_ROOT/bin/prepare-kafka-deployment.sh"
bash "$PROJECT_ROOT/bin/prepare-demo-deployment.sh"