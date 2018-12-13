from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext

import string
import sys

if len(sys.argv) != 2:
	print "Usage: tipo_vehiculo_mas_accidentes [file]"
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("tipo_vehiculo_mas_accidentes")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)

	file = sys.argv[1]

	data = spark.read.format("csv").option("header", "true").load(file)
	print(type(data['Tipo Vehiculo']))

	vehicles = data.select('Tipo Vehiculo').filter(data['Tipo Vehiculo'] != "NO ASIGNADO").filter(data['Tipo Vehiculo'] != "VARIOS")

	vehicles_grouped = vehicles.groupBy('Tipo Vehiculo').count()

	vehicles_result = vehicles_grouped.orderBy(vehicles_grouped["count"].desc()).show()





