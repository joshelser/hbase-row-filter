## Build

Update `hbase.version` in pom.xml to be the intended version for the test scenario, then `mvn package` to generate a
fatjar

## Load data

```java
java -cp target/hbase-rowfilter-0.0.1-SNAPSHOT.jar:/usr/local/lib/hbase/conf:/usr/local/lib/hbase/lib/hadoop-hdfs-2.7.3.2.6.1.0-SNAPSHOT.jar  com.github.joshelser.hbase.filter.DataLoad
java -cp target/hbase-rowfilter-0.0.1-SNAPSHOT.jar:/usr/local/lib/hbase/conf:/usr/local/lib/hbase/lib/hadoop-hdfs-2.7.3.2.6.1.0-SNAPSHOT.jar  com.github.joshelser.hbase.filter.AlphabeticDataLoad
```

## Read data

```java
java -cp target/hbase-rowfilter-0.0.1-SNAPSHOT.jar:/usr/local/lib/hbase/conf:/usr/local/lib/hbase/lib/hadoop-hdfs-2.7.3.2.6.1.0-SNAPSHOT.jar  com.github.joshelser.hbase.filter.ReadData
java -cp target/hbase-rowfilter-0.0.1-SNAPSHOT.jar:/usr/local/lib/hbase/conf:/usr/local/lib/hbase/lib/hadoop-hdfs-2.7.3.2.6.1.0-SNAPSHOT.jar  com.github.joshelser.hbase.filter.AlphabeticReadData
```
