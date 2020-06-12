import os
import subprocess
import sys
import atexit
from collections import defaultdict

KAFKA_DIR = "~/Downloads/kafka_2.12-2.5.0"

global child_processes
global status_dict

child_processes = defaultdict()
status_dict = defaultdict()


def kill_subprocesses():

	for key, process in child_processes.items():
		process.terminate()


def wait_subprocesses():

	for key, process in child_processes.items():
		status_dict[key] = process.wait()


atexit.register(kill_subprocesses)


def start_zookeeper(kafka_dir):

	pass


def start_kafka_server(kafka_dir):

	pass

def start_kafka_producer():

	child_processes['stream_producer'] = subprocess.Popen(["python", "StreamProducer.py"])


def start_kafka_consumer():

	child_processes['stream_consumer'] = subprocess.Popen(["python", "StreamConsumer.py"])


def start_app():

	child_processes['dash_app'] = subprocess.Popen(["python", "app.py"])


start_kafka_producer()
start_kafka_consumer()
start_app()
wait_subprocesses()