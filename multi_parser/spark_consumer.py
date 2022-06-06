# -*- coding: utf-8 -*-

"""
spark_consumer.py
~~~~~~~~~~~~~~~~~

Считываем события из базы данных
"""

import findspark
from pyspark.sql.dataframe import DataFrame

from multi_parser.common_spark import deserialize_json, splitting_json_columns, parse_array_from_string
from start_vk_parser.common import get_logger
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType


class SparkConsumer:
    """
    Класс для обработки операций create в базе постов
    """
    def __init__(self):
        self.__log = get_logger(self.__class__.__name__)
        self.__log.info("Starting SparkSession")
        findspark.init()
        self.__spark = SparkSession.builder.appName('DebeziumKafkaStreamingConsumer').getOrCreate()

    def start_spark_session(self, time_termination: int = 60) -> None:
        """
        Start streaming micro-batch
        """
        self.__log.info(f"Spark session started: {self.__spark.sparkContext}")

        kafka = self.__spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "192.168.1.110:9092") \
            .option("subscribePattern", "dbserver1.public.*") \
            .load()

        kafka = kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)")

        query = kafka \
            .writeStream \
            .trigger(processingTime='30 seconds') \
            .outputMode("update") \
            .option("checkpointLocation", "/Users/emidiant/PycharmProjects/vk-parser/checkpoints/spark_1") \
            .queryName("qraw_spark_1") \
            .foreachBatch(self.dataframe_transform) \
            .start()

        query.awaitTermination(time_termination)
        self.__log.warning(f"Stop after {time_termination} seconds working")
        self.stop_spark()

    def dataframe_transform(self, new_raw: DataFrame, epoch_id: int):
        """
        Dataframe batch processing

        :param new_raw:     spark batch DataFrame
        :param epoch_id:    epoch id of micro batch
        """
        new_raw.persist()
        self.__log.debug(f"Epoch ID: {epoch_id}")
        new_raw = splitting_json_columns(new_raw)
        # структура вложений для attachments
        struct_attach = ArrayType(StructType([
            StructField('id', LongType()),
            StructField('date', LongType()),
            StructField('link', StringType()),
            StructField('path', StringType()) # todo удалить, т.к. будет парсинг на втором этапе
        ]))

        retrieve_array = F.udf(parse_array_from_string, struct_attach)

        new_raw.show()
        topics_schema = new_raw.select("topic").distinct().collect()
        topics_list = [top.topic for top in topics_schema if top.topic == "dbserver1.public.posts"]
        for topic in topics_list:
            self.__log.info(f"topic: {topic}")

            topic_key_value = new_raw.filter(new_raw.topic == topic)
            # deserialize json schema to spark dataframe struct
            topic_key_value, isKeyDefined = deserialize_json(topic_key_value)
            df_op_create = topic_key_value.filter(topic_key_value.op == "c") \
                .select(F.col("after.*")) \
                .select("id_increment", "domain", "attachments", F.col("date").alias("timestamp")) \
                .withColumn("attachments", F.explode(retrieve_array(F.col("attachments")))) \
                .select("id_increment", "domain", "timestamp", F.col("attachments.*")) \
                .where(F.col("id") != 0) \
                .select("id_increment", "domain", "timestamp", "link")

            df_op_create.show()

            df_op_create \
                .write \
                .mode('append') \
                .format("parquet") \
                .save('/Users/emidiant/PycharmProjects/vk-parser/data/images/parquet/' + topic.split('.')[2])
            # self.__log.debug(f"First row: {df_op_create.first()}")

    def stop_spark(self):
        """
        Stop spark session
        """
        self.__spark.stop()
        self.__log.info("Session stopped")

def run_spark():
    spark_consumer = SparkConsumer()
    try:
        time_termination = 120
        spark_consumer.start_spark_session(time_termination)
    except KeyboardInterrupt:
        spark_consumer.stop_spark()

if __name__ == "__main__":
    run_spark()
