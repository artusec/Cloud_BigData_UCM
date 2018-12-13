from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext

import string
import sys

if len(sys.argv) != 2:
	print "Usage: dia_mas_accidentes [file]"
	exit(-1)
else:
	conf = SparkConf().setMaster('local[4]').setAppName("dia_mas_Accidentes")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)
	
	fi = sys.argv[1]
	
	dataFrame = spark.read.format("csv").option("header", "true").load(fi)
	
	df_select = dataFrame.select('FECHA')
	
	df = df_select.groupBy('FECHA').count()
	
	df_final = df.orderBy(df["count"].desc()).show()

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
