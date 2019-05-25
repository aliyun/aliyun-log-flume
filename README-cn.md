
Aliyun Log Flume
================

#### Flume

Flume是Apache开源的一个数据搬运工具，每个Flume Agent进程可以包含Source，Sink和Channel三个组件。
- Source: 数据源，常见的Source有Kafka，文件等。
- Sink: 数据写入目标，常见的有HDFS，Hive等。
- Channel: 数据在从Source获取之后写入Sink之前的缓冲队列，常见的channel有内存队列，Kakfa等。
此外，Flume中的数据以Event的形式存在，Event对象由两部分组成：
- body: 字节数组的形式，表示数据内容。
- headers: 以key-value的形式组成，包含附加属性。


#### aliyun-log-flume 
aliyun-log-flume 是一个实现日志服务（Loghub）和Flume对接的插件，可以通过Flume和其他的数据
系统如HDFS，Kfaka等系统打通。目前Flume官方支持的插件除了HDFS，Kakfa之外还有Hive，Hbase，
ElasticSearch等，初次之外对于常见的数据源在社区也都能找到对应的插件支持。aliyun-log-flume 
为Loghub 实现了Sink和Source 插件。

##### Loghub Sink
通过sink的方式可以将其他数据源的数据通过Flume接入SLS。目前支持两种解析格式：
- SIMPLE: 将整个Flume Event 作为一个字段写入Loghub。
- DELIMITED：将整个Flume Event 作为分隔符分隔的数据根据配置的列名解析成对应的字段写入Loghub。

#### Loghub Source
通过Source的方式可以将Loghub的数据经过Flume投递到其他的数据源。目前支持两种输出格式：
- DELIMITED，数据以分隔符的方式写入Flume。
- JSON，数据以JSON的形式写入Flume。