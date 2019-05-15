```Apache Flume``` is a open source project used for moving massive quantities of streaming data between different data sources like HDFS, Kakfa, Loghub. This project implemented Loghub Source for ingesting data from Loghub and Loghub Sink for collecting data from other source and publish to Loghub. 

### Requirements
- Java 1.8+
- Maven installed.

### Setup up Flume

#### Downlaod Flume

Download ```Apache Flume``` from the official site - http://www.apache.org/dyn/closer.lua/flume/1.9.0/apache-flume-1.9.0-bin.tar.gz, the latest version is ```1.9.0```. 

Copy the downloaded tarball in the directory of your server and extract contents using the following command:

```tar -xvf apache-flume-1.9.0-bin.tar.gz```
 
This command will create a new directory named apache-flume-1.9.0-bin and extract files into it. All official sinks/sources library are placed the the directory apache-flume-1.9.0-bin/lib. 

#### Build 

Go to the project root folder, and build aliyun-log-flume using the following command:

```mvn clean compile assembly:single -DskipTests```

After this command successfully executed, a new jar named ```aliyun-log-flume-1.0-SNAPSHOT.jar``` will be generated under directory target, all denpendencies of this project are packaged into the jar file as well. Then copy aliyun-log-flume-1.0-SNAPSHOT.jar to apache-flume-1.9.0-bin/lib.


### Usage
#### Sink
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
agent.sources.slssrc.type = com.aliyun.loghub.sink.LoghubSink
agent.sources.slssrc.endpoint = <Your Loghub endpoint>
agent.sources.slssrc.project = <Your Loghub project>
agent.sources.slssrc.logstore = <Your Loghub logstore>
agent.sources.slssrc.accessKeyId = <Your Accesss Key Id>
agent.sources.slssrc.accessKey = <Your Access Key>


# Configure a channel that buffers events in memory:
agent.channels.memoryChannel.type = memory
agent.channels.memoryChannel.capacity = 20000
agent.channels.memoryChannel.transactionCapacity = 100

# Bind the source and sink to the channel:
agent.sources.netcatsource.channels = memoryChannel
agent.sinks.slssink.channel = memoryChannel
```

#### Source
Loghub source used to consuming data from Loghub and send to other system like HDFS:
```
#
# Source: Loghub
# Sink: HDFS
#
agent.sources = slssource
agent.sinks = hdfssink
agent.channels = memoryChannel

# Configure the source:
agent.sources.slssrc.type = com.aliyun.loghub.source.LoghubSource
agent.sources.slssrc.endpoint = <Your Loghub endpoint>
agent.sources.slssrc.project = <Your Loghub project>
agent.sources.slssrc.logstore = <Your Loghub logstore>
agent.sources.slssrc.accessKeyId = <Your Accesss Key Id>
agent.sources.slssrc.accessKey = <Your Access Key>
agent.sources.slssrc.columns = <expected clomuns in order>
agent.sources.slssrc.seperator = ,

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
agent.sources.slssource.channels = memoryChannel
agent.sinks.hdfssink.channel = memoryChannel
```



