
Aliyun Log Flume
================

#### Flume

Flume是Apache开源的一个在各个大数据系统之间搬运数据的工具，用户需要在机器上运行Flume Agent, 每个Flume Agent进程可以包含Source，Sink和Channel三个组件。
- Source: 数据源，常见的Source有Kafka，文件等。
- Sink: 数据写入目标，如HDFS，Hive等。
- Channel: 数据在从Source获取之后写入Sink之前的缓冲队列，常见的channel有内存队列，Kafka等。

Flume中的每条数据以Event的形式存在，Event对象由两部分组成：
- body: 数据内容，以字节数组的形式。
- headers: 以key-value的形式组成，包含附加属性。


#### aliyun-log-flume 
aliyun-log-flume 是一个实现日志服务（Loghub）对接Flume的插件，可以通过Flume将日志服务和其他的数据
系统如HDFS，Kafka等系统打通。目前Flume官方支持的插件除了HDFS，Kafka之外还有Hive，HBase，ElasticSearch等，
除此之外对于常见的数据源在社区也都能找到对应的插件支持。
aliyun-log-flume 为Loghub 实现了Sink和Source 插件。
- Sink: Flume读取其他数据源的数据然后写入Loghub。
- Source: Flume消费Loghub然后写入其他系统如HDFS。

##### Loghub Sink
通过sink的方式可以将其他数据源的数据通过Flume接入Loghub。目前支持两种解析格式：
- SIMPLE：将整个Flume Event作为一个字段写入Loghub。
- DELIMITED：将整个Flume Event作为分隔符分隔的数据根据配置的列名解析成对应的字段写入Loghub。

支持的配置如下：

|名称|描述|默认值|必需|
|---|---|---|---|
|type| 固定为com.aliyun.loghub.flume.sink.LoghubSink | | Y |
|endpoint| Loghub endpoint| | Y |
|project| Loghub project| | Y |
|logstore| Loghub logstore| | Y |
|accessKeyId| Loghub accessKeyId| | Y |
|accessKey| Loghub accessKey| | Y |
|batchSize| 写入Loghub批数据大小|1000 | N |
|maxBufferSize| 缓存队列大小|1000 | N |
|serializer| Event序列化格式，支持DELIMITED, SIMPLE,或者自定义serializer，如果是自定义serializer，此处填完整类名称 |SIMPLE | N |
|columns| serializer为DELIMITED时，必须指定字段列表，用逗号分隔，顺序与实际的数据中字段顺序一致。| | N |
|separatorChar| serializer为DELIMITED时，用于指定数据的分隔符，必须为单个字符|, | N |
|quoteChar| serializer为DELIMITED时，用于指定Quote字符 |" | N |
|escapeChar| serializer为DELIMITED时，用于指定转义字符 | " | N |
|useRecordTime| 是否使用数据中的timestamp字段作为日志时间| false| N |

#### Loghub Source
通过Source的方式可以将Loghub的数据经过Flume投递到其他的数据源。目前支持两种输出格式：
- DELIMITED：数据以分隔符的方式写入Flume。
- JSON：数据以JSON的形式写入Flume。

支持的配置如下：

|名称|描述|默认值|必需|
|---|---|---|---|
|type| 固定为com.aliyun.loghub.flume.source.LoghubSource | | Y |
|endpoint| Loghub endpoint| | Y |
|project| Loghub project| | Y |
|logstore| Loghub logstore| | Y |
|accessKeyId| Loghub accessKeyId| | Y |
|accessKey| Loghub accessKey| | Y |
|heartbeatIntervalMs| 客户端和Loghub的心跳间隔，单位毫秒|30000 | N |
|fetchIntervalMs| Loghub数据拉取间隔，单位毫秒|100 | N |
|fetchInOrder| 是否按顺序消费|false | N |
|batchSize| 拉取批量大小 |1000 | N |
|consumerGroup| 拉取的消费组名称 | 随机产生 | N |
|initialPosition| 拉取起点位置，支持begin, end, timestamp。注意：如果服务端已经存在checkpoint，会优先使用服务端的checkpoint|begin | N |
|timestamp| 当我initialPosition为timestamp时，必须指定时间戳，Unix时间戳格式 | | N |
|deserializer| Event反序列化格式，支持DELIMITED, JSON,或者自定义deserializer，如果是自定义deserializer，此处填完整类名称 |DELIMITED | Y |
|columns| deserializer为DELIMITED时，必须指定字段列表，用逗号分隔，顺序与实际的数据中字段顺序一致。| | N |
|separatorChar| deserializer为DELIMITED时，用于指定数据的分隔符，必须为单个字符|, | N |
|quoteChar| deserializer为DELIMITED时，用于指定Quote字符 |" | N |
|escapeChar| deserializer为DELIMITED时，用于指定转义字符 | " | N |
|appendTimestamp| deserializer为DELIMITED时，是否将时间戳作为一个字段自动添加到每行末尾 | false | N |
|sourceAsField| deserializer为JSON时，是否将日志Source作为一个字段，字段名称为__source__ |false | N |
|tagAsField| deserializer为JSON时，是否将日志Tag作为字段，字段名称为__tag__:{tag名称}| false | N |
|timeAsField| deserializer为JSON时，是否将日志时间作为一个字段，字段名称为__time__ | false | N |
|useRecordTime| 是否使用日志的时间，用于Event header中指定时间戳，如果为false则使用系统时间| false| N |
