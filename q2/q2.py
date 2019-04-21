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
import os

fn1 = sys.argv[1]
if os.path.exists(fn1):
        tweetsFile = fn1
fn2 = sys.argv[2]
if os.path.exists(fn2):
        usersFile = fn2

spark = SparkSession.builder.appName("q2").getOrCreate()
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

#regex pattern for retweets
RTPattern = '(?:RT\s)(@\w+)'

#extract retweets and create new DF
retweets = tweets.withColumn('Retweets', regexp_extract(col('tweet'), RTPattern, 1))
retweets = retweets.groupBy("Retweets").count().sort(desc("count"))

#clear null tuples
retweets = retweets.filter(retweets.Retweets.rlike('@'))

#redirect stdio and print result to file
sys.stdout = open('most_retweeted_users.txt', 'w')

print("Q2: 10 most retweeted users")
retweets.show(n=10)

spark.stop()
