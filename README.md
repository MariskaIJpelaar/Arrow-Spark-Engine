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
and use the jar as you will. You can find the jar in `target`
>Note: the jar includes all dependencies

## Useful tricks
 - Finding dependency conflicts: `mvn enforcer:enforce`, see also: [Solving Dependency Conflicts in Maven](https://dzone.com/articles/solving-dependency-conflicts-in-maven) (2022-03-18)