# mysql-collector
从MySQL数据中采集数据，发送到Kafka

默认采集processlist, 可以修改为任意合法的SQL语句
```
show full processlist
```

# 定时任务
```
*/5 * * * * flock -xn /tmp/mysql-processlist.lock -c '/apps/mysql-collector/mysql-collector.py'
```

* 每5分钟采集一次数据
* 执行前必须要获得排他文件锁，防止脚本同时执行

# 命令行参数说明
```
使用默认参数
./mysql-processlist.py

查看详细信息
./mysql-processlist.py -v

只测试SQL语句，不连接Kafka
./mysql-processlist.py -Tv

手动指定Kafka
./mysql-processlist.py -v --kafka-hosts=192.168.1.182:9092
./mysql-processlist.py -v --kafka-hosts=192.168.1.182:9092,192.168.1.182:9092

手动指定MySQL服务器
./mysql-processlist.py -v --host=127.0.0.1
./mysql-processlist.py -v --host=127.0.0.1 --port=3306

手动指定MySQL服务器和Kafka服务器
/apps/mysql-collector/mysql-collector.py -v --host=127.0.0.1 --port=3306 --kafka-hosts=10.19.33.244:9092,10.19.40.117:9092
```
