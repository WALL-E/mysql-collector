#!/bin/bash

role=`id -u`
if test $role -ne 0
then
    echo " requires root privileges"
    exit 1
fi

yum install -y util-linux-ng-2.17.2-12.28.el6.x86_64
yum install -y python-setuptools

easy_install pip

pip install mysql
pip install docopt
pip install kafka-python
