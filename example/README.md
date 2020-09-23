```
mvn clean compile assembly:single -DskipTests
mv target/aliyun-log-flume-1.2.jar $FLUME_HOME/lib
mv flume-env.sh $FLUME_HOME/conf
mv kafka2sls.properties $FLUME_HOME/conf
mv log4j.properties $FLUME_HOME/conf
cd $FLUME_HOME
./bin/flume-ng agent --conf ./conf --conf-file ./conf/kafka2sls.properties -n agent
```
