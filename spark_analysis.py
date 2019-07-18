from __future__ import print_function
import sys
import re
from operator import add
import pandas as pd
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import json
import boto
import boto3
from boto.s3.key import Key
import boto.s3.connection
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import *

#aws keys
aws_key_id = 'AKIAJ5W77XXXXXXXXXXXXXX'
aws_key = 'prU5Z1JNaOujbBGAi3XXXXXXXXXXXXXXXXX'

def main():
    conf = SparkConf().setAppName("first")
    sc = SparkContext(conf=conf)
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId",aws_key_id)
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey",aws_key)
    sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3-us-east-2.amazonaws.com")
    config_dict = {"fs.s3a.awsAccessKeyId":aws_key_id,
        "fs.s3a.awsSecretAccessKey":aws_key}
        bucket = "nikhil1234567890123"
        prefix = "/2019/04/*/*/*"
    filename ="s3a://{}/trump/{}".format(bucket,prefix)

rdd = sc.newAPIHadoopFile(filename,
                          "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                          "org.apache.hadoop.io.Text",
                          "org.apache.hadoop.io.LongWritable",
                          conf=config_dict)
spark = SparkSession.builder.appName("PythonWordCount").config("spark.files.overwrite","true").getOrCreate()

df = spark.read.json(rdd.map(lambda x: x[1]))
data_rm_na = df.filter(df["status_id"]!='None')
features_of_interest = ['rt_status_user_followers_count',\
                        'rt_status_user_friends_count',\
                        'rt_status_user_statuses_count',\
                        'rt_status_retweet_count',\
                        'rt_status_user_listed_count',\
                        'rt_status_user_id',\
                        'rt_status_created_at',\
                        'status_created_at',\
                        'rt_status_user_name',\
                        'searched_names',\
                        'rt_status_sentScore',\
                        'rt_status_favorite_count',\
                        'status_id']
                        
                        df_reduce= data_rm_na.select(features_of_interest)
                        df_reduce = df_reduce.withColumn("rt_status_user_followers_count", df_reduce["rt_status_user_followers_count"].cast(IntegerType()))
                        df_reduce = df_reduce.withColumn("rt_status_user_friends_count", df_reduce["rt_status_user_friends_count"].cast(IntegerType()))
                        df_reduce = df_reduce.withColumn("rt_status_user_statuses_count", df_reduce["rt_status_user_statuses_count"].cast(IntegerType()))
                        df_reduce = df_reduce.withColumn("rt_status_retweet_count", df_reduce["rt_status_retweet_count"].cast(IntegerType()))
                        df_reduce = df_reduce.withColumn("rt_status_user_listed_count", df_reduce["rt_status_user_listed_count"].cast(IntegerType()))
                        df_reduce = df_reduce.withColumn("rt_status_favorite_count", df_reduce["rt_status_favorite_count"].cast(IntegerType()))
                
                        
                        url_ = "jdbc:mysql:/xxxxxxxxx.xxxxxxxxxxxx.us-east-2.rds.amazonaws.com:3306/innodb"
                        table_name_ = "twitterdata"
                        mode_ = "overwrite"
                        
                        df_reduce.write.format("jdbc").option("url", url_)\
                        .option("dbtable", table_name_)\
                        .option("driver", "com.mysql.jdbc.Driver")\
                        .option("user", "xxxxxxxxx")\
                        .option("password", "xxxxxxxxxx")\
                        .mode(mode_)\
                        .save()
                        
                        if __name__ == "__main__":
                        main()
                        

