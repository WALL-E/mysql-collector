#!/usr/bin/python2.6
# encoding: utf-8
"""collect MySQL processlist to kafka

Usage:
  mysql-processlist.py [-h] [--user=user] [--passwd=passwd] [--host=host] [--port=port] [--db=db] [--charset=charset] [--connect-timeout=timeout] [-v] [--kafka-hosts=list] [--kafka-topic=topic] [--test-sql]
  mysql-processlist.py --version

Options:
  -u --user=root            MySQL username, default is root
  -p --passwd=''            MySQL password, default is ''
  -H --host=127.0.0.1       MySQL server host, default is 127.0.0.1
  -P --port=3306            MySQL server port, default is 3306
  -d --db=db                MySQL server port, default is 3306
  -c --charset=utf8         MySQL server charset, default is utf8
  -C --connect-timeout=3    MySQL server connect timeout, default is 3s
  -T --test-sql             Only execute sql, not send message to kafka
  -k --kafka-hosts=192.168.1.182:9092   Kafka host list, Separated by a comma
  -t --kafka-topic=MYSQL_PROCESSLIST    Kafka topic
  -h --help             Show this help message and exit
  -v --verbose          Print status messages
  --version             Show version and exit
"""

import sys
import json
import time
import logging

import MySQLdb
from docopt import docopt

try:
    from kafka import KafkaProducer
except ImportError:
    print "need kafka-python, please run depend.sh"
    sys.exit(1)

logging.basicConfig(filename="/var/log/mysql-processlist.log", filemode="a+", format="%(asctime)s-%(name)s-%(levelname)s-%(message)s", level=logging.DEBUG)

host = "127.0.0.1"
port = 3306
user = "root"
passwd = ""
db = ""
connect_timeout = 3
charset = "utf8"

kafka_hosts = "192.168.1.182:9092"
kafka_topic = "MYSQL_PROCESSLIST"

verbose = False
test_sql = False


def print_arguments():
    print "--arguments--"
    print "log:", "/var/log/mysql-processlist.log"
    print "host:", host
    print "port:", port
    print "user:", user
    print "passwd:", passwd
    print "db:", db
    print "connect_timeout:", connect_timeout
    print "charset:", charset
    print "verbose:", verbose
    print "test_sql:", test_sql
    print ""


def rebuild_options(arguments):
    global host, port, user, passwd, db, connect_timeout, charset
    global kafka_hosts, kafka_topic
    global verbose, test_sql
    if arguments["--host"]:
        host = arguments["--host"]
    if arguments["--port"]:
        port = int(arguments["--port"])
    if arguments["--user"]:
        user = arguments["--user"]
    if arguments["--passwd"]:
        passwd = arguments["--passwd"]
    if arguments["--user"]:
        user = arguments["--user"]
    if arguments["--db"]:
        db = arguments["--db"]
    if arguments["--connect-timeout"]:
        connect_timeout = int(arguments["--connect-timeout"])
    if arguments["--charset"]:
        charset = arguments["--charset"]
    if arguments["--kafka-hosts"]:
        kafka_hosts = arguments["--kafka-hosts"]
    if arguments["--kafka-topic"]:
        kafka_topic = arguments["--kafka-topic"]
    if arguments["--verbose"]:
        verbose = True
    if arguments["--test-sql"]:
        test_sql = True


def main():
    logging.debug("mysql-processlist starting")
    arguments = docopt(__doc__, version='1.0.0rc1')
    rebuild_options(arguments)

    if verbose:
        print_arguments()

    try:
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db, connect_timeout=connect_timeout, charset=charset)
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        sql = 'show full processlist'
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception, msg:
        logging.error("mysql-processlist %s" % (msg))
        if verbose:
            print "error: %s" % (msg)
        sys.exit(1)

    for row in rows:
        row["timestamp"] = time.time()

    if verbose:
        print "--content--"
        content = json.dumps(rows, ensure_ascii=True, encoding='utf-8')
        print content

    if test_sql:
        sys.exit(0)

    try:
        producer = KafkaProducer(bootstrap_servers=kafka_hosts.split(","))
        for row in rows:
            message = json.dumps(row, ensure_ascii=True, encoding='utf-8')
            producer.send(kafka_topic, message)
        producer.flush()
        producer.close()
    except Exception, msg:
        logging.error("mysql-processlist %s" % (msg))
        if verbose:
            print "error: %s" % (msg)
        sys.exit(1)

    if verbose:
        print "send %s message to kafka" % (len(rows))
    logging.info("mysql-processlist send %s messages to kafka" % (len(rows)))

    sys.exit(0)


if __name__ == '__main__':
    main()
