#!/bin/bash

cd "/Users/amol/Downloads/kafka_2.12-2.5.0"

bin/zookeeper-server-start.sh config/zookeeper.properties & bin/kafka-server-start.sh config/server.properties 