from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext

import string
import sys

if len(sys.argv) != 2:
	print "Usage: distrito_mas_accidentes [file]"
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("Distrito_mas_Accidentes")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)
	sqlContext = SQLContext(spark)
	
	archivo_input = sys.argv[1]
	
	text = sc.textFile(archivo_input)
	rdd = text.filter(lambda line: "FECHA" not in line)\
	.map(lambda line: line.split(","))\
	.map(lambda line: (line[3], 1))\
	.reduceByKey(lambda x, y: (x+y))\
	.map(lambda line: Row(Distrito=line[0], Num=line[1]))
	
	data = spark.createDataFrame(rdd).createOrReplaceTempView("rdd")
	
	pr = spark.sql("select * from rdd where Num = (select max(Num) from rdd)")
	pr.repartition(1).write.csv("output")
	
	#Otra forma, pero no se como guardarlo en un archivo en vez de
	# hacer take()
	#sortBy(lambda x: -x[1]
	#print(rdd.take(1))
	
	


