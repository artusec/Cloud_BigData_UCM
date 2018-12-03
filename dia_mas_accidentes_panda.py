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
	
	fi = sys.argv[1]
	
	dataFrame = spark.read.format("csv").option("header", "true").load(fi)
	
	df_select = dataFrame.select('FECHA')
	
	df = df_select.groupBy('FECHA').count()
	
	df_final = df.orderBy(df["count"].desc()).show()

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
