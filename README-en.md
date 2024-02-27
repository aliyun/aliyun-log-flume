# Aliyun Log Flume

```Apache Flume``` is a open source project used for moving massive quantities of streaming data. Aliyun Log Flume implemented Flume Source and Sink for moving data between Loghub and external data source like HDFS, Kakfa. 

### Requirements
- Java 1.8+
- Maven 3.x+.

### Setup up Flume

#### Downlaod Flume

Download ```Apache Flume``` from the official site - http://www.apache.org/dyn/closer.lua/flume/1.9.0/apache-flume-1.9.0-bin.tar.gz, the latest version is ```1.9.0```. Copy the downloaded tarball in the directory of your server and extract contents using the following command:

```tar -xvf apache-flume-1.9.0-bin.tar.gz```
 
This command will create a new directory named apache-flume-1.9.0-bin and extract files into it. All official sinks/sources library are placed under the the directory apache-flume-1.9.0-bin/lib. There is also a directory apache-flume-1.9.0-bin/conf, we'll add our configuration file in that directory later.

#### Build this project

Go to the project root folder, and build aliyun-log-flume using the following command:

```mvn clean compile assembly:single -DskipTests```

After this command successfully executed, a new jar named ```aliyun-log-flume-1.0-SNAPSHOT.jar``` will be generated under directory target, all denpendencies of this project are packaged into the jar file as well. Then copy aliyun-log-flume-1.0-SNAPSHOT.jar to apache-flume-1.9.0-bin/lib.

### Configuration

Create a new configuration file ```flume-loghub.conf``` under apache-flume-1.9.0-bin/conf, and add our configuration of sink or source in this file, here is an example for collecting data from netcat to Loghub and from Loghub to HDFS:

##### Sink example

Loghub sink used to streaming data from flume to Loghub, here is an example for collecting data from ```netcat``` and send to Loghub:
```
agent.sources = netcatsource
agent.sinks = slssink
agent.channels = memoryChannel

# Configure the source:
agent.sources.netcatsource.type = netcat
agent.sources.netcatsource.bind = localhost
agent.sources.netcatsource.port = 44444

# Describe the sink:
agent.sinks.slssink.type = com.aliyun.loghub.flume.sink.LoghubSink
agent.sinks.slssink.endpoint = <Your Loghub endpoint>
agent.sinks.slssink.project = <Your Loghub project>
agent.sinks.slssink.logstore = <Your Loghub logstore>
agent.sinks.slssink.accessKeyId = <Your Accesss Key Id>
agent.sinks.slssink.accessKey = <Your Access Key>


# Configure a channel that buffers events in memory:
agent.channels.memoryChannel.type = memory
agent.channels.memoryChannel.capacity = 20000
agent.channels.memoryChannel.transactionCapacity = 100

# Bind the source and sink to the channel:
agent.sources.netcatsource.channels = memoryChannel
agent.sinks.slssink.channel = memoryChannel
```

#### Source example
Ingesting data from Loghub and save to HDFS:
```
agent.sources = slssrc
agent.sinks = hdfssink
agent.channels = memoryChannel

# Configure the source:
agent.sources.slssrc.type = com.aliyun.loghub.flume.source.LoghubSource
agent.sources.slssrc.endpoint = <Your Loghub endpoint>
agent.sources.slssrc.project = <Your Loghub project>
agent.sources.slssrc.logstore = <Your Loghub logstore>
agent.sources.slssrc.accessKeyId = <Your Accesss Key Id>
agent.sources.slssrc.accessKey = <Your Access Key>
agent.sources.slssrc.consumerGroup = consumer-group-test
agent.sources.slssrc.columns = <expected clomuns in order>
agent.sources.slssrc.separatorChar = ,
# query for SLS SPL in source, refer: https://help.aliyun.com/zh/sls/user-guide/spl-overview
agent.sources.slssrc.query = * | WHERE method = 'POST'

# Describe the sink:
agent.sinks.hdfssink.type = hdfs
agent.sinks.hdfssink.hdfs.path = hdfs://localhost:8020/user/root/test
agent.sinks.hdfssink.hdfs.writeFormat = Text
agent.sinks.hdfssink.hdfs.round = true
agent.sinks.hdfssink.hdfs.roundValue = 20
agent.sinks.hdfssink.hdfs.roundUnit = minute
agent.sinks.hdfssink.hdfs.rollSize = 0
agent.sinks.hdfssink.hdfs.rollCount = 0
agent.sinks.hdfssink.hdfs.fileType = DataStream
agent.sinks.hdfssink.hdfs.useLocalTimeStamp = true

# Configure a channel that buffers events in memory:
agent.channels.memoryChannel.type = memory
agent.channels.memoryChannel.capacity = 20000
agent.channels.memoryChannel.transactionCapacity = 100


# Bind the source and sink to the channel:
agent.sources.slssrc.channels = memoryChannel
agent.sinks.hdfssink.channel = memoryChannel
```
NOTE: For HDFS sink, we need to download HDFS libraries from https://hadoop.apache.org/releases.html , the latest version is 3.1.2, after extracted it, copy all libraries under hadoop-{hadoop-version}/share/hadoop/common and hadoop-{hadoop-version}/share/hadoop/common/lib to apache-flume-1.9.0-bin/lib , this libraries are required by HDFS sink.

### Start Flume
After the configuration file is created, run the following command under apache-flume-1.9.0-bin:
```
./bin/flume-ng agent --name agent --conf conf  --conf-file conf/flume-loghub.conf
```

