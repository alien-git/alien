#!/bin/bash
echo "Killing all the processes of my user: $USER"

kill -9 `ps -ef |grep $USER |awk '{print $2}'`

