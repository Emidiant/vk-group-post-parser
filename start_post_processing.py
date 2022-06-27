#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
start_post_processing.py
~~~~~~~~~~~~~~~~~~~~~~~~

Launching wall-post processing with Spark Structured Streaming
"""
from post_processing.spark_consumer import SparkConsumer

def run_spark():
    spark_consumer = SparkConsumer()
    try:
        time_termination = 120
        spark_consumer.start_spark_session()
        # spark_consumer.start_spark_session(time_termination)
    except KeyboardInterrupt:
       spark_consumer.stop_spark()

if __name__ == "__main__":
    run_spark()
