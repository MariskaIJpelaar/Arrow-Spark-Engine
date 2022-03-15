# Arrow-Spark-Engine
>NOTE: this readme is still under heavy development, as well as the project itself

## Installation -- Apache Spark
Arrow-Spark-Engine requires you to have Apache Spark version 3.3.0.
At the time of writing (10-03-2022), this version is only available through source-code.
Thus, you must compile Apache Spark Core and install it to your local maven repository

First, clone the Apache Spark repository:
```bash
git clone https://github.com/apache/spark
```
Then, move into the `spark` directory and build Spark with: 
```bash
./build/mvn -DskipTests clean package
```
This may take a while. For more information, please check the [Building Spark](https://spark.apache.org/docs/latest/building-spark.html) page.
Alternatively, you can build each submodule separately, e.g. with:
```bash
./build/mvn -pl :spark-streaming_2.12 clean install
```

After it is finished you should add the jars of the submodules we need to your local maven repository. 
You can do this with the following command: 
```bash
mvn install:install-file -Dfile=[PATH TO JAR] -DgroupId=org.apache.spark -DartifactId=[ARTIFACT-ID] -Dversion=3.3.0-SNAPSHOT -Dpackaging=jar -DgeneratePom=true
```
For example:
```bash
mvn install:install-file -Dfile=spark/core/target/spark-core_2.12-3.3.0-SNAPSHOT.jar -DgroupId=org.apache.spark -DartifactId=spark-core_2.12 -Dversion=3.3.0-SNAPSHOT -Dpackaging=jar -DgeneratePom=true
```
The jars you need are:
- `spark/core/target/spark-core_2.12-3.3.0-SNAPSHOT.jar`
- `spark/common/network-common/target/spark-network-common_2.12-3.3.0-SNAPSHOT.jar`
- `spark/common/network-shuffle/target/spark-network-shuffle_2.12-3.3.0-SNAPSHOT.jar`
- `spark/common/tags/target/spark-tags_2.12-3.3.0-SNAPSHOT.jar`
- `spark/common/unsafe/target/spark-unsafe_2.12-3.3.0-SNAPSHOT.jar`

## Usage
Build the project with 
```bash
mvn clean compile assembly:single
```
and use the jar as you will. You can find the jar in `target`
>Note: the jar includes all dependencies