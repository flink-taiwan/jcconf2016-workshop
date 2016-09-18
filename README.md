# Apache Flink™ Training Workshop @ JCConf2016

This is the training material for the [Apache Flink](https://flink.apache.org/) workshop at JCConf2016.

The tutorial code is still work in progress! However, workshop attendees can still download this project for some pre-workshop preparation by following the instructions below.

## Preparation Instructions for JCConf2016 Attendees

The project contains a relatively large-sized sample streaming dataset (at `src/main/resources/nycTaxiRides.gz`), and scripts to download some required binaries for the tutorial environment. So, you may want to download these before the workshop.

First, clone the project:

```
$ git clone https://github.com/flink-taiwan/jcconf2016-workshop.git
```

Afterwards, install all the required software dependencies using [Maven](https://maven.apache.org/download.cgi):

```
cd jcconf2016-workshop
mvn clean install
```

The software dependencies that will be used by this tutorial will be installed in your local Maven repository.

Next, download software binaries for systems that we will be deploying for our tutorial environment:

```
cd jcconf2016-workshop
bin/prepare-all-deployment.sh
```

The above script will download and prepare all the required binaries. This might take a few minutes, as it needs to download binaries of Apache Flink, Apache Kafka, InfluxDB, and Grafana. After it finishes, you can checkout the downloaded binaries under the directory `jcconf2016-workshop/deploy/`.

# Disclaimer

Apache®, Apache Flink™, Flink™, and the Apache feather logo are trademarks of [The Apache Software Foundation](http://apache.org/).
