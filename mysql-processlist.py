#!/usr/local/bin/python2.7
# encoding: utf-8

import MySQLdb
import json
import time

host = "127.0.0.1"
port = 3306
user = "root"
passwd = ""
db = ""
connect_timeout = 3
charset="utf8"

conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db, connect_timeout=connect_timeout, charset=charset)
cursor = conn.cursor(MySQLdb.cursors.DictCursor)

sql = 'show full processlist'

cursor.execute(sql)
rows = cursor.fetchall()

for row in rows:
    row["timestamp"] = time.time()
content = json.dumps(rows, ensure_ascii=True, encoding='utf-8')

print content
