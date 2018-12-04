from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext

import string
import sys

#Esto no se usa aun, porque falta extraer el mes de la fecha
#que esta en formato dd/mm/aaa. Solo es para que la salida
#sea algo mas bonica
def switch_func(fecha):
	switcher = {
		01: "January",
		02: "February",
		03: "March",
		04: "April",
		05: "May",
		06: "June",
		10: "October",
		11: "November",
		12: "December"
	}

if len(sys.argv) != 2:
	print "Usage: dia_mas_accidentes [file]"
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("dia_mas_Accidentes")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)
	sqlContext = SQLContext(spark)
	
	archivo_input = sys.argv[1]
	
	text = sc.textFile(archivo_input)
	rdd = text.filter(lambda line: "FECHA" not in line)\
	.map(lambda line: line.split(","))\
	.map(lambda line: (line[0], 1))\
	.reduceByKey(lambda x, y: (x+y))\
	.map(lambda line: Row(Fecha=line[0], Num=line[1]))\
	.sortBy(lambda x: -x[1])\
	
	data = spark.createDataFrame(rdd).createOrReplaceTempView("rdd")
	
	pr = spark.sql("select * from rdd where Num = (select max(Num) from rdd)")
	
	pr.repartition(1).write.csv("output")

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
