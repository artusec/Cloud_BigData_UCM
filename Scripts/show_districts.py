from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext

import string
import sys

if len(sys.argv) != 2:
	print "Usage: show_districts [file]"
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("show_districts")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)

	file = sys.argv[1]

	data = spark.read.format("csv").option("header", "true").load(file)

	data.select('DISTRITO').distinct().orderBy("DISTRITO").show(data.count())
