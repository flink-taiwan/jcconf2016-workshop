# Apache Flink™ Training Workshop @ JCConf2016

This is the training material for the [Apache Flink](https://flink.apache.org/) workshop at JCConf2016.

The tutorial code is still work in progress! However, workshop attendees can still follow the below instructions to prepare for the event.

## Preparation Instructions for JCConf2016 Attendees

Attendees for the workshop are recommended to prepare the following before the event:

  1. **Clone this project:** `git clone https://github.com/flink-taiwan/jcconf2016-workshop.git`
  
     The project contains example code we will be going through, and a relatively large streaming
     dataset (at `dataset/nycTaxiRides.gz`).
     
  2. **Install [IntelliJ IDEA Community Version](https://www.jetbrains.com/idea/#chooseYourEdition)**
  
     Throughout the workshop, we will mostly be using IntelliJ IDEA to learn how to program streaming applications
     with Flink's API. You can also use other IDEs that support Maven, but it is recommended to use IntelliJ IDEA.
  
  3. **Build or pull the workshop docker container environment:** `docker pull flinktw/jcconf2016-workshop`
  
     Besides learning how to write Flink streaming programs, we will also be using Kafka, InfluxDB, and Grafana
     along with a standalone Flink cluster for some architectural practices. The container provides a prepared
     environment to setup these systems. Afterwards, you can startup the environment with:
     
     ```bash
    docker run -it --rm -h jcconf2016-flink \
               -p 8081:8081 \ # 8081 for Flink JobManager Web UI
               -p 9092:9092 \ # 9092 for Kafka broker server
               -p 2181:2181 \ # 2181 for Zookeeper Quorum
               -p 3000:3000 \ # 3000 for Grafana Web UI
               -p 8086:8086 \ # 8086 for InfluxDB server
               flinktw/jcconf2016-workshop bash
     ```
     
     If everything went well, you should be able to find the following layout within the environment:
     
     ```bash
     jcconf@jcconf2016-flink:~$ ls
     env-deploy.sh flink jcconf2016-workshop kafka

     jcconf@jcconf2016-flink:~$ ./env-deploy.sh
     * Deploy Apache Flink cluster
       setting up password-less ssh to localhost ...
       starting JobManager & TaskManager ...
     * Deploy Apache Kafka
       starting Zookeeper & Kafka broker ...
       creating 'cleansedRides' topic ...
       creating 'sawtoothWaves' topic ...
     * Deploy InfluxDB & Grafana

     jcconf@jcconf2016-flink:~$ jps
     625 TaskManager
     629 QuorumPeerMain
     358 JobManager
     631 Kafka
     877 Jps
     ```

# Disclaimer

Apache®, Apache Flink™, Flink™, and the Apache feather logo are trademarks of [The Apache Software Foundation](http://apache.org/).
