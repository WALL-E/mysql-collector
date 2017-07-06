# mysql-processlist
mysql processlist output to kafka

# 定时任务
```
*/5 * * * * flock -xn /tmp/mysql-processlist.lock -c '/root/mysql-processlist/mysql-processlist.py'
```

* 每5分钟采集一次数据
* 执行前必须要获得排他文件锁，防止脚本同时执行
