#!/usr/bin/python2.6
# encoding: utf-8
"""collect MySQL data/status and sedn to kafka

Usage:
  mysql-collector.py [-h] [--user=user] [--passwd=passwd] [--host=host] [--port=port] [--db=db] [--charset=charset] [--connect-timeout=timeout] [-v] [--kafka-hosts=list] [--kafka-topic=topic] [--test-sql]
  mysql-collector.py --version

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
  -t --kafka-topic=MYSQL_COLLECTOR    Kafka topic
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

logging.basicConfig(filename="/var/log/mysql-collector.log", filemode="a+", format="%(asctime)s-%(name)s-%(levelname)s-%(message)s", level=logging.INFO)

host = "127.0.0.1"
port = 3306
user = "root"
passwd = ""
db = ""
connect_timeout = 3
charset = "utf8"

kafka_hosts = "192.168.1.182:9092"
kafka_topic = "MYSQL_COLLECTOR"

verbose = False
test_sql = False


def print_arguments():
    print "--arguments--"
    print "log:", "/var/log/mysql-collector.log"
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

def mysql_query(sql):
    try:
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db, connect_timeout=connect_timeout, charset=charset)
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception, msg:
        logging.error("mysql-collector %s" % (msg))
        if verbose:
            print "error: %s" % (msg)
        return None

def send2kafka(name, results):
    try:
        producer = KafkaProducer(bootstrap_servers=kafka_hosts.split(","))
        for result in results:
            message = json.dumps(result, ensure_ascii=True, encoding='utf-8')
            producer.send(kafka_topic, message)
            if verbose:
                print "messages:", message
        producer.flush()
        producer.close()
    except Exception, msg:
        logging.error("mysql-collector %s" % (msg))
        if verbose:
            print "error: %s" % (msg)
        sys.exit(1)
    if verbose:
        print "send %s %s message to kafka" % (len(results), name)
    logging.info("mysql-collector send %s messages to kafka" % (len(results)))

def handler_processlist(name, results):
    objs = []
    for result in results:
        result["Timestamp"] = time.time()
        result["Server"] = "%s:%s" % (host, port)
        obj = {}
        obj["name"] = name
        obj["data"] = result
        objs.append(obj)
    return objs

plugins = [
     {
         "name": "processlist",
         "sql": "show full processlist",
         "handler": handler_processlist
     }
]

def main():
    logging.debug("mysql-collector starting")
    arguments = docopt(__doc__, version='1.0.0rc1')
    rebuild_options(arguments)

    if verbose:
        print_arguments()

    for plugin in plugins:
        results = mysql_query(plugin["sql"])
        if test_sql:
            print "sql:", plugin["sql"]
            print "results:", json.dumps(results, ensure_ascii=True, encoding='utf-8')
        else:
            if results is not None:
                messages = plugin["handler"](plugin["name"], results)
                send2kafka(plugin["name"], messages)

    sys.exit(0)


if __name__ == '__main__':
    main()
