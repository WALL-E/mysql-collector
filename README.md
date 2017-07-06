# mysql-processlist
从MySQL数据中采集数据，发送到Kafka

默认采集processlist, 可以修改为任意合法的SQL语句
```
show full processlist
```

# 定时任务
```
*/5 * * * * flock -xn /tmp/mysql-processlist.lock -c '/root/mysql-processlist/mysql-processlist.py'
```

* 每5分钟采集一次数据
* 执行前必须要获得排他文件锁，防止脚本同时执行
