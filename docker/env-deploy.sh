#!/usr/bin/env bash

# This script is meant only for usage within the docker environment

GREEN='\033[1;32m'
LIGHT_GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}* Deploy Apache Flink cluster${NC}"

echo -e "${LIGHT_GREEN}  setting up password-less ssh to localhost ...${NC}"
sudo service ssh start > /dev/null
ssh-keygen -t rsa -N '' -f /home/jcconf/.ssh/id_rsa > /dev/null
cat /home/jcconf/.ssh/id_rsa.pub >> /home/jcconf/.ssh/authorized_keys
ssh-keyscan -H localhost 2> /dev/null >> /home/jcconf/.ssh/known_hosts

echo -e "${LIGHT_GREEN}  starting JobManager & TaskManager ...${NC}"
cd /home/jcconf/flink
bin/start-cluster.sh > /dev/null

echo -e "${GREEN}* Deploy Apache Kafka${NC}"

cd /home/jcconf/kafka

echo -e "${LIGHT_GREEN}  starting Zookeeper & Kafka broker ...${NC}"
bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null &
bin/kafka-server-start.sh config/server.properties > /dev/null &
sleep 5 # sleep a bit to make sure the Kafka processes complete startup before creating topoics

echo -e "${LIGHT_GREEN}  creating 'cleansedRides' topic ...${NC}"
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic cleansedRides > /dev/null

echo -e "${LIGHT_GREEN}  creating 'sawtoothWaves' topic ...${NC}"
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sawtoothWaves > /dev/null

echo -e "${GREEN}* Deploy InfluxDB & Grafana${NC}"
sudo service influxdb start > /dev/null
sudo service grafana-server start > /dev/null