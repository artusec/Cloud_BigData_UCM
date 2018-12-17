from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext
import matplotlib.pyplot as plt
import pandas as pd
import string
import sys

if len(sys.argv) != 2:
	print("Usage: tipo_vehiculo_mas_accidentes [file]")
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("tipo_vehiculo_mas_accidentes")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)

	file = sys.argv[1]

	data = spark.read.format("csv").option("header", "true").load(file)	
	
	vehicles = data.select('Tipo Vehiculo').filter(data['Tipo Vehiculo'] != "NO ASIGNADO").filter(data['Tipo Vehiculo'] != "VARIOS")

	vehicles_grouped = vehicles.groupBy('Tipo Vehiculo').count()

	vehicles_final = vehicles_grouped.orderBy(vehicles_grouped["count"].desc())
		
	vehicles_final.show()
	
	vehicles_final_pd = vehicles_final.toPandas()
	
	vehicles_final_pd.plot(x="Tipo Vehiculo", y="count", kind="barh")
	
	plt.show()



