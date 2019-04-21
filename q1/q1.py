import pandas as pd
import numpy as np
import re
import findspark
findspark.init('/home/dave/spark-2.4.1-bin-hadoop2.7/')
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
#import regexp_extract, col, lit
from pyspark.sql import *
import sys

tweetsFile = "/home/dave/Documents/CS691/assignment2/data/training_set_tweets.txt"
usersFile = "/home/dave/Documents/CS691/assignment2/data/training_set_users.txt"

spark = SparkSession.builder.appName("q1").getOrCreate()
tweets = spark.read.text(tweetsFile)
users = spark.read.text(usersFile)

#split DF into Cols: uid, tid, tweet
split_col = pyspark.sql.functions.split(tweets['value'], '\t')
tweets = tweets.withColumn('uid', split_col.getItem(0))
tweets = tweets.withColumn('tid', split_col.getItem(1))
tweets = tweets.withColumn('tweet', split_col.getItem(2))
split_col = pyspark.sql.functions.split(tweets['value'], '(\t)(?:20)')
tweets = tweets.withColumn('date', split_col.getItem(1))

tweets = tweets.select('uid', 'tid', 'tweet', 'date')
#tweets.show()

#regex pattern for mentions
mentionPattern = '(?<!RT\s)(@\w+)'

#extract mentions and create new DF
mentions = tweets.withColumn('Mentions', regexp_extract(col('tweet'), mentionPattern, 1))
mentions = mentions.groupBy("Mentions").count().sort(desc("count"))

#clear null tuples
mentions = mentions.filter(mentions.Mentions.rlike('@'))

#redirect stdout and print results
sys.stdout = open('popular_mentions.txt', 'w')
print("Q1: 20 most mentions")
mentions.show(n=20)

spark.stop()
