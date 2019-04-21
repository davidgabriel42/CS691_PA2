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

spark = SparkSession.builder.appName("Q1").getOrCreate()
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

#process users to DF
split_users = pyspark.sql.functions.split(users['value'], '\t')
users = users.withColumn('uid', split_users.getItem(0))
users = users.withColumn('loc', split_users.getItem(1))
users = users.select('uid','loc')

LAPattern = '(Los\sAngeles)'
#extract mentions and create new DF

users = users.filter(users.loc.rlike(LAPattern))
#users.printSchema()
#users.show(n=20)

#09/16/2009 - 09/20/2009 extract tweets from these dates
datePattern = '(09-09-)(16|17|18|19|20)'
tweets = tweets.filter(tweets.date.rlike(datePattern))
#tweets.show()

#join on user id
users = users.select(col('uid').alias('uid1'), col('loc'))
join = tweets.join(users, tweets.uid == users.uid1)
#join.show()

LA = join.groupBy("uid").count().sort(desc("count"))

#redirect stdio and print result to file
sys.stdout = open('most_tweeted_users.txt', 'w')
print("Q3: 10 most tweeted users by uid from LA 9/16-9/20/2009")
LA.show(n=10)

spark.stop()
