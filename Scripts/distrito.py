from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext

import string
import sys

if len(sys.argv) != 2:
	print("Usage: distrito_mas_accidentes [file]")
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("distrito_mas_Accidentes")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)
	
	fi = sys.argv[1]
	
	dataFrame = spark.read.format("csv").option("header", "true").load(fi)
	
	df_select = dataFrame.select('DISTRITO')
	
	df = df_select.groupBy('DISTRITO').count()
	
	df_final = df.orderBy(df["count"].desc())

	df_final.show(df_final.count(), False)
	
	
	


