# spark-ts-examples

Description
-----------

JVM examples showing how to use the `spark-ts` time series library for Apache Spark.

Minimum Requirements
--------------------

* Java 1.8
* Maven 3.0
* Apache Spark 1.6.0

Using this Repo
---------------

### Building

We use [Maven](https://maven.apache.org/) for building Java / Scala. To compile and build
the example jar, navigate to the `jvm` directory and run:

    mvn package

### Running

To submit one of the Java or Scala examples to a local Spark cluster, run the following command
from the `jvm` directory:

    spark-submit --class com.cloudera.tsexamples.Stocks target/spark-ts-examples-0.0.1-SNAPSHOT-jar-with-dependencies.jar

You can substitute any of the Scala or Java example classes as the value for the `--class`
parameter.

To submit a Python example, run the following command from the `python` directory:

    spark-submit --driver-class-path PATH/TO/sparkts-0.3.0-jar-with-dependencies.jar Stocks.py

The `--driver-class-path` parameter value must point to the Spark-TS JAR file, which can be
downloaded from the spark-timeseries [Github repo](https://github.com/sryza/spark-timeseries).
