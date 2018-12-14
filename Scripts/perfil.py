from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext

import string
import sys

23, 25

if len(sys.argv) != 2:
	print "Usage: perfil_mas_accidentes [file]"
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("perfil_mas_Accidentes")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)
	sqlContext = SQLContext(spark)
	
	fi = sys.argv[1]
	
	text = sc.textFile(fi)
	rdd = text.filter(lambda line: "FECHA" not in line)\
	.filter(lambda line: "NO ASIGNADO" not in line)\
	.filter(lambda line: "DESCONOCIDA" not in line)\
	.map(lambda line: line.split(","))\
	.map(lambda line: Row(Sexo=line[23], Edad=line[25], Num=1))\
	
	data = spark.createDataFrame(rdd).createOrReplaceTempView("rdd")
	
	pr = spark.sql("select Sexo, Edad, count(Num) as Cuenta from rdd group by Sexo, Edad\
	order by Cuenta desc")
	
	print pr.toPandas()
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
