# Arrow Spark Engine -- Patcher
This local repository is made as a patcher for unsolvable dependencies or other dependency-related problems.
Important to note is that this is not our preferred method of solving dependency issues as it is difficult to easily apply updates.
If at some point we want to apply an update in our main project, we should check here to manually apply the update if required. 
The end of this README will list all dependencies to keep track of. 

## Making Changes
As this is a local repository, you need to install the jar after making changes and rebuilding. To rebuild, simply run `mvn package` in the current directory.
To install the generated jar, use:
```bash
mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file -Dfile=target/Arrow-Spark-Engine-Patch-1.0-SNAPSHOT.jar -DgroupId=nl.tudelft.abs.ffiorini -DartifactId=Arrow-Spark-Engine-Patch -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DlocalRepositoryPath=.
```

## Update List
Here we list all files to keep track of when applying an update. 
Note that the path in the original version and the path in the pather should be the same!

| GroupID          | Artifact     | Version | Path                                          |
|------------------|--------------|---------|-----------------------------------------------|
| org.apache.arrow | arrow-vector | 6.0.0   | org/apache/arrow/vector/types/pojo/Field.java |
|                  |              |         |                                               |
|                  |              |         |                                               |