Stream Processing with Apache Flink 
------------------------------------

<p align="center">
    <img src="assets/cover.png" width="600" height="800">
</p>

This repository contains the code for the book **[Stream Processing: Hands-on with Apache Flink](https://leanpub.com/streamprocessingwithapacheflink)**.


### Table of Contents
1. [Environment Setup](#environment-setup)
2. [Register UDF](#register-udf)
3. [Deploy a JAR file](#deploy-a-jar-file)


### Environment Setup
In order to run the code samples we will need a Kafka and Flink cluster up and running.
You can also run the Flink examples from within your favorite IDE in which case you don't need a Flink Cluster.

If you want to run the examples inside a Flink Cluster run to start the Pulsar and Flink clusters.
```shell
docker-compose up
```

When the cluster is up and running successfully run the following command for redpanda:
```shell
./redpanda-setup.sh

```

or this command for kafka setup
```shell
./kafka-setup.sh
```


### Register UDF
```shell
CREATE FUNCTION maskfn  AS 'io.streamingledger.udfs.MaskingFn'      LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';
CREATE FUNCTION splitfn AS 'io.streamingledger.udfs.SplitFn'        LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';
CREATE FUNCTION lookup  AS 'io.streamingledger.udfs.AsyncLookupFn'  LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';

CREATE TEMPORARY VIEW sample AS
SELECT * 
FROM transactions 
LIMIT 10;

SELECT transactionId, maskfn(UUID()) AS maskedCN FROM sample;
SELECT * FROM transactions, LATERAL TABLE(splitfn(operation));

SELECT 
  transactionId,
  serviceResponse, 
  responseTime 
FROM sample, LATERAL TABLE(lookup(transactionId));
```

### Deploy a JAR file
1. Package the application and create an executable jar file
```shell
mvn clan package
```
2. Copy it under the jar files to be included in the custom Flink images

3. Start the cluster to build the new images by running
```shell
docker-compose up
```

4. Deploy the flink job
```shell
docker exec -it jobmanager ./bin/flink run \
  --class io.streamingledger.datastream.BufferingStream \
  jars/spf-0.1.0.jar
```

### Scheduler.py
The goal for the scheduler is to 
1. read the Flink metrics and collect useful information 
2. recompute the parallelism based on #1
3. apply the new parallelism

### Things we need to consider to adapt to the new env
1. the way how we collect the data, especially we need a more accurate measurement of input rate for the source operator
2. we need to collect the **processing rate**, **input rate** and **busy time** for each operator 
3. config specific to the job ex. ops = ['Source:_Source_One', 'Source:_Source_Two', 'FlatMap_tokenizer', 'Count_op', 'Sink:_Dummy_Sink']