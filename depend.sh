#!/bin/bash

role=`id -u`
if test $role -ne 0
then
    echo " requires root privileges"
    exit 1
fi

pip install mysql
pip install docopt
pip install kafka-python
