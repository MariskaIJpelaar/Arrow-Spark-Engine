# Arrow-Spark-Engine
>NOTE: this readme is still under heavy development, as well as the project itself

## Installation -- Apache Spark
Arrow-Spark-Engine requires you to have Apache Spark version 3.3.0.
At the time of writing, this version is only available through source-code.
Thus, you must compile Apache Spark Core and install it to your local maven repository

First, clone the Apache Spark repository:
```bash
git clone https://github.com/apache/spark
```
Then, move into the `spark` directory, and compile the submodule `core`, with:
> NOTE: the other spark submodules will be required as well
> TODO: change, see: https://spark.apache.org/docs/latest/building-spark.html
```bash
./build/mvn -pl :spark-core_2.12 clean install
```
This may take a while.
After it is finished, you can add the jar `spark/core/target/spark-core_2.12-3.3.0-SNAPSHOT.jar` 
to your local maven repository with:
```bash
mvn install:install-file -Dfile=[PATH TO JAR] -DgroupId=org.apache.spark -DartifactId=spark-core_2.12 -Dversion=3.3.0-SNAPSHOT -Dpackaging=jar -DgeneratePom=true
```

## Usage