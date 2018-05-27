#!/usr/bin/env bash

jps -l | grep Worker | awk '{print $1}' | xargs kill -9
jps -l | grep Master | awk '{print $1}' | xargs kill -9

./sbin/start-master.sh
./sbin/start-slave.sh spark://Max-MacBook-Pro-15.local:7077