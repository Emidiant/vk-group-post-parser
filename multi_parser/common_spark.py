# -*- coding: utf-8 -*-

"""
common_spark.py
~~~~~~~~~~~~~~~

Useful functions of Spark Dataframe
"""
import ast
import json
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, IntegerType, LongType, NullType, TimestampType,\
    ShortType, BooleanType, DoubleType, FloatType, StructField, StructType
import pyspark.sql.functions as F


def parse_array_from_string(x):
    """

    :param x:
    :return:
    """
    null_list_dict = [{
            "id": 0,
            "date": 0,
            "link": "",
            "path": ""
        }]
    if x == "[]" or x == "":
        return null_list_dict

    list_dict = ast.literal_eval(x)
    # if len(list_dict) == 0:
    #     list_dict = null_list_dict
    return ast.literal_eval(x)

def splitting_json_columns(new_raw: DataFrame) -> DataFrame:
    # строим стандартную схему для разбиения key и payload, чтобы иметь возможность считать schema
    temp_schema = StructType([
        StructField('schema', StringType()),
        StructField('payload', StringType())
    ])

    # раскрываем dataframe согласно схеме, переименовывая столбцы для определения их принадлежности к key и value
    new_raw = new_raw.select(
        F.from_json(new_raw.value, temp_schema).alias("value"),
        F.from_json(new_raw.key, temp_schema).alias("key"),
        "topic"
    ).filter(
        # производим удаление tombstone, которые возникают после удаления и необходимы только kafka
        new_raw['value'] != "null"
    ).select(
        F.col("key.schema").alias("key_schema"),
        F.col("key.payload").alias("key"),
        F.col("value.schema").alias("value_schema"),
        F.col("value.payload").alias("value"),
        "topic"
    )
    return new_raw


def deserialize_json(topic_key_value: DataFrame) -> (DataFrame, bool):
    """
    JSON schema deserializer

    :param topic_key_value:     dataframe with debezium json struct (schema + payload)
    :return:                    spark dataframe with deserialized json columns and key status
    """
    first_row = topic_key_value.first()
    key_sch = first_row.key_schema
    value_sch = first_row.value_schema

    # проверяем определен ли key
    isKeyDefined = True
    if key_sch is None:
        topic_key_value = topic_key_value.select(
            "key",
            F.from_json("value", schema_generator(value_sch)).alias("value")
        ) \
            .select(
            "key",
            F.col("value.*")
        )
        isKeyDefined = False
    else:
        topic_key_value = topic_key_value.select(
            F.from_json(topic_key_value.key, schema_generator(key_sch)).alias("key"),
            F.from_json(topic_key_value.value, schema_generator(value_sch)).alias("value")
        ) \
            .select(
            "key",
            F.col("value.*")
        )
    return topic_key_value, isKeyDefined

def schema_generator(schema: str) -> StructType:
    """
    Define spark schema for debezium data

    :param schema:  json schema from debezium connector
    :return:        spark schema for dataframe
    """
    dict_schema = json.loads(schema)
    struct_list = []

    for column in dict_schema['fields']:
        if 'fields' in column:
            inner_struct_list = []
            if column['field'] != 'source' and column['field'] != 'transaction':
                for f in column['fields']:
                    inner_struct_list.append(define_structure(f['field'], f['type']))
                struct_list.append(StructField(column['field'], StructType(inner_struct_list)))
        else:
            struct_list.append(define_structure(column['field'], column['type']))

    return StructType(struct_list)

def define_structure(field_name: str, format_type: str) -> StructField:
    """
    Mapping between debezium and spark data types

    :param field_name:      field name
    :param format_type:     debezium data type
    :return:
    """
    try:
        if format_type == 'datetime64[ns]':
            typo = TimestampType()
        elif format_type == 'int64':
            typo = LongType()
        elif format_type == 'int32':
            typo = IntegerType()
        elif format_type == 'int16':
            typo = ShortType()
        elif format_type == 'boolean':
            typo = BooleanType()
        elif format_type == 'float32':
            typo = DoubleType()
        elif format_type == 'float64':
            typo = FloatType()
        elif format_type == 'null':
            typo = NullType()
        else:
            typo = StringType()
    except:
        typo = StringType()
    return StructField(field_name, typo)
