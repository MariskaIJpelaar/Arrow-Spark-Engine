# Arrow-Spark-Engine

## Usage
Build the jar with dependencies with
```bash
mvn clean compile assembly:single
```
or without dependencies with
```bash
mvn package
```
and use the jar as you will. You can find the jar in `target`. 

To create both the dependency-jar and the sources-jar, run:
```bash
mvn clean compile verify assembly:single
```
>Note: dependencies jar should be >100 MB

## Making a local repository
If you do not want to build a dependency-jar and do not want to deal with dependencies yourself, 
you can create a local repository as follows:
```bash 
mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file -Dfile=target/Arrow-Spark-Engine-1.0-SNAPSHOT.jar -DgroupId=nl.tudelft.abs.ffiorini -DartifactId=Arrow-Spark-Engine -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DlocalRepositoryPath=.
```
Then, you can add it as a local repository
```xml
<repository>
  <id>local-patches</id>
  <url>file://${path.to.arrow.spark.engine}</url>
</repository>
```

```gradle.build
maven {
        url file:// + path_to_arrow_spark_engine
}
```

## Useful tricks
 - Finding dependency conflicts: `mvn enforcer:enforce`, see also: [Solving Dependency Conflicts in Maven](https://dzone.com/articles/solving-dependency-conflicts-in-maven) (2022-03-18)