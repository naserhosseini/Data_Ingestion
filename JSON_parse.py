import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from typing import List

common_event = StructType([
    StructField("col1_val", DateType()),
    StructField("col2_val", StringType()),
    StructField("col3_val", StringType()),
    StructField("col4_val", StringType()),
    StructField("col5_val", TimestampType()),
    StructField("col6_val", IntegerType()),
    StructField("col7_val", TimestampType()),
    StructField("col8_val", DecimalType()),
    StructField("col9_val", DecimalType()),
    StructField("col10_val", IntegerType()),
    StructField("col11_val", DecimalType()),
    StructField("col12_val", IntegerType()),
    StructField("col13_val", StringType())
])


def parse_json(line:str):
    record_type = line['event_type']
    record = json.load(line)
    try:
        # [logic to parse records]
        if record_type == "T":
            # [Get the applicable field values from json]
            event = record
            if record_type == "":  # [some key fields empty]
                record['event type'] = "T"
            else:
                return event
        elif record_type == "Q":
            event = record
            # [Get the applicable field values from json]
            if record_type == "":  # [some key fields empty]:
                record['event type'] = "Q"
            else:
                return event
    except None:
        event = record
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        record['event type'] = "B"
        return event


def apply_latest(line):
    return line.groupBy()


spark = SparkSession.builder.master('local').appName('JSON_Spring').getOrCreate()
spark.conf.set("fs.azure.account.key.springboard.blob.core.windows.net",
               "fbg587c3dYJWJ6A4wjUSOKjTvIPOPxWL05DuNpAPKLNwhpalKyVuK7/idU5hmSbi8WAQgK/tNo5IrikDMS7dbw==")
raw =spark.textFile("wasbs://spark@springboard.blob.core.windows.net/https://springboard.blob.core.windows.net/spark")
parsed = raw.map(lambda line: parse_json(line))
data = spark.createDataFrame(parsed)