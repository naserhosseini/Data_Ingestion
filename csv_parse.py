from pyspark.sql import SparkSession
from pyspark.sql.types import *
from typing import List

common_event = StructType([
    StructField("trade_dt", DateType()),
    StructField("arrival_tm", TimestampType()),
    StructField("rec_type", StringType()),
    StructField("symbol", StringType()),
    StructField("event_tm", TimestampType()),
    StructField("event_seq_nb", IntegerType()),
    StructField("exchange", StringType()),
    StructField("trade_pr", DecimalType()),
    StructField("bid_pr", DecimalType()),
    StructField("bid_size", IntegerType()),
    StructField("ask_pr", DecimalType()),
    StructField("ask_size", IntegerType()),
    StructField("partition", StringType())
])

def parse_csv(line: str):
    record_type_pos = 2
    record = line.split(",")
    try:
    # [logic to parse records]
        if record[record_type_pos] == "T":
            event = [record[0], record[1], record[2], record[3],
                     record[4], record[5], record[6], record[7], "", "", "", "", "T"]
            return event
        elif record[record_type_pos] == "Q":
            event = [record[0], record[1], record[2], record[3], record[4],
                     record[5], record[6], "", record[7], record[8],
                     record[9], record[10], "Q"]
            return event
    finally:
        event = ["", "", "", "", "", "", "", "", "", "", "", "", "B"]
    return event

spark = SparkSession.builder.master('local').appName('Nasr_Spring').getOrCreate()
spark.conf.set("fs.azure.account.key.springboard.blob.core.windows.net",
               "fbg587c3dYJWJ6A4wjUSOKjTvIPOPxWL05DuNpAPKLNwhpalKyVuK7/idU5hmSbi8WAQgK/tNo5IrikDMS7dbw==")
raw =spark.textFile("wasbs://spark@springboard.blob.core.windows.net/https://springboard.blob.core.windows.net/spark")
parsed = raw.map(lambda line: parse_csv(line))
data = spark.createDataFrame(parsed)