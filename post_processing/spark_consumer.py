# -*- coding: utf-8 -*-

"""
spark_consumer.py
~~~~~~~~~~~~~~~~~

Считываем события из базы данных
"""
import os
from pyspark.sql.dataframe import DataFrame

from vk_common.common_spark import deserialize_json, splitting_json_columns, parse_array_from_string
from vk_common.common_python import get_logger
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
        self.__spark = SparkSession\
            .builder\
            .appName('DebeziumKafkaStreamingConsumer') \
            .getOrCreate()

    def start_spark_session(self, time_termination: int = None) -> None:
        """
        Start streaming micro-batch
        """
        self.__log.info(f"Spark session started: {self.__spark.sparkContext}")

        kafka_host = os.getenv("KAFKA_HOST", "localhost")
        kafka_port = os.getenv("KAFKA_PORT", 9092)

        self.__log.info(f"Kafka config: host={kafka_host}, port={kafka_port}")
        kafka = self.__spark \
            .readStream \
            .format("kafka") \
            .option("failOnDataLoss", "false") \
            .option("kafka.bootstrap.servers", f"{kafka_host}:{kafka_port}") \
            .option("subscribePattern", "dbserver1.public.*") \
            .load()

        kafka = kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)")

        checkpoint_location = "checkpoints/spark_1"
        query = kafka \
            .writeStream \
            .trigger(processingTime='30 seconds') \
            .outputMode("update") \
            .option("checkpointLocation", checkpoint_location) \
            .queryName("qraw_spark_1") \
            .foreachBatch(self.dataframe_transform) \
            .start()

        if time_termination:
            query.awaitTermination(time_termination)
            self.__log.warning(f"Stop after {time_termination} seconds working")
            self.stop_spark()
        else:
            query.awaitTermination()

    def dataframe_transform(self, new_raw: DataFrame, epoch_id: int):
        """
        Dataframe batch processing

        :param new_raw:     spark batch DataFrame
        :param epoch_id:    epoch id of micro batch
        """
        new_raw.persist()
        self.__log.debug(f"Epoch ID: {epoch_id}")
        new_raw = splitting_json_columns(new_raw)
        # included structure for attachments field
        struct_attach = ArrayType(StructType([
            StructField('id', LongType()),
            StructField('date', LongType()),
            StructField('link', StringType()),
            StructField('path', StringType())
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
                .select("id_increment", "target", "domain", "attachments", F.col("date").alias("timestamp")) \
                .withColumn("attachments", F.explode(retrieve_array(F.col("attachments")))) \
                .select("id_increment", "target", "domain", "timestamp", F.col("attachments.*")) \
                .where(F.col("id") != 0) \
                .select("id_increment", "target", "domain", "timestamp", "id", "link")

            df_op_create.show()

            # dir_parquet = '/Users/emidiant/PycharmProjects/vk-parser/data/images/parquet/' + topic.split('.')[2]
            dir_parquet = 'data/images/parquet/' + topic.split('.')[2]
            df_op_create \
                .write \
                .mode('append') \
                .format("parquet") \
                .save(dir_parquet)

    def stop_spark(self):
        """
        Stop spark session
        """
        self.__spark.stop()
        self.__log.info("Session stopped")
