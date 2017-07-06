#!/usr/bin/python2.6
# encoding: utf-8
"""collect MySQL processlist to kafka

Usage:
  mysql-processlist.py [-h] [--user=user] [--passwd=passwd] [--host=host] [--port=port] [--db=db] [--charset=charset] [--connect-timeout=timeout] [-v]
  mysql-processlist.py --version

Options:
  -u --user=root            MySQL username, default is root
  -p --passwd=''            MySQL password, default is ''
  -H --host=127.0.0.1       MySQL server host, default is 127.0.0.1
  -P --port=3306            MySQL server port, default is 3306
  -d --db=db                MySQL server port, default is 3306
  -c --charset=utf8         MySQL server charset, default is utf8
  -C --connect-timeout=3    MySQL server connect timeout, default is 3s
  -h --help             show this help message and exit
  -v --verbose          print status messages
  --version             show version and exit
"""

import sys
import json
import time
import logging

import MySQLdb
from docopt import docopt

logging.basicConfig(filename="/var/log/mysql-processlist.log", filemode="a+", format="%(asctime)s-%(name)s-%(levelname)s-%(message)s", level=logging.DEBUG)

host = "127.0.0.1"
port = 3306
user = "root"
passwd = ""
db = ""
connect_timeout = 3
charset="utf8"

verbose = False

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
    print ""

def rebuild_options(arguments):
    global host, port, user, passwd, db, connect_timeout, charset
    global verbose
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
    if arguments["--verbose"]:
         verbose = True

def main():
    logging.debug("mysql-processlist starting")
    arguments = docopt(__doc__, version='1.0.0rc1')
    rebuild_options(arguments)

    if verbose:
        print_arguments()

    try:
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db, connect_timeout=connect_timeout, charset=charset)
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        logging.debug("mysql-processlist connected mysql server")
    except Exception, msg:
        logging.error("mysql-processlist %s" % (msg))
        if verbose:
            print "error: %s" % (msg)
        sys.exit(1)

    sql = 'show full processlist'

    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    except Exception, msg:
        logging.error("mysql-processlist %s" % (msg))
        if verbose:
            print "error: %s" % (msg)
        sys.exit(1)

    for row in rows:
        row["timestamp"] = time.time()
    content = json.dumps(rows, ensure_ascii=True, encoding='utf-8')

    if verbose:
        print "--content--"
        print content

if __name__ == '__main__':
    main()
