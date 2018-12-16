from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import mean, desc, size
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import array_contains
import numpy as np


import string
import sys
import re 

if len(sys.argv) < 3:
	print("Usage: contexto_mas_accidentes_por_distrito [file] [district]")
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("contexto_mas_accidentes_por_distrito")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)

	file = sys.argv[1]
	district = sys.argv[2].upper()
	if len(sys.argv) > 3: 
		i = 3
		while i < len(sys.argv): 
			district += " " + sys.argv[i].upper()
			i = i + 1
	print(district)

	data = spark.read.format("csv").option("header", "true").load(file)

	cpfa_granizo = data.select('CPFA Granizo').filter(data['CPFA Granizo'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPFA Granizo']).count()
	cpfa_hielo = data.select('CPFA Hielo').filter(data['CPFA Hielo'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPFA Hielo']).count()
	cpfa_niebla = data.select('CPFA Niebla').filter(data['CPFA Niebla'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPFA Niebla']).count()
	cpfa_seco = data.select('CPFA Seco').filter(data['CPFA Seco'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPFA Seco']).count()
	cpfa_nieve = data.select('CPFA Nieve').filter(data['CPFA Nieve'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPFA Nieve']).count()
	cpsv_mojada = data.select('CPSV Mojada').filter(data['CPSV Mojada'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPSV Mojada']).count()
	cpsv_aceite = data.select('CPSV Aceite').filter(data['CPSV Aceite'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPSV Aceite']).count()
	cpsv_barro = data.select('CPSV Barro').filter(data['CPSV Barro'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPSV Barro']).count()
	cpsv_grava = data.select('CPSV Grava Suelta').filter(data['CPSV Grava Suelta'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPSV Grava Suelta']).count()
	cpsv_hielo = data.select('CPSV Hielo').filter(data['CPSV Hielo'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPSV Hielo']).count()
	cpsv_seca = data.select('CPSV Seca y Limpia').filter(data['CPSV Seca y Limpia'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy(data['CPSV Seca y Limpia']).count()

	schema = StructType([
		StructField("Situation", StringType(), True),
		StructField("Number of accidents", IntegerType(), True)
		])
	print("--------------------------- DISTRITO: " + district + "--------------------------")
	valid = False
	
	cpfa_granizo_counted = 0
	cpfa_hielo_counted = 0
	cpfa_niebla_counted = 0
	cpfa_seco_counted = 0
	cpfa_nieve_counted = 0
	cpsv_mojada_counted = 0
	cpsv_aceite_counted = 0
	cpsv_barro_counted = 0
	cpsv_grava_counted = 0
	cpsv_hielo_counted = 0
	cpsv_seca_counted = 0	

counter = np.array([cpfa_granizo_counted , cpfa_hielo_counted, cpfa_niebla_counted, cpfa_seco_counted, cpfa_nieve_counted, cpsv_mojada_counted,
cpsv_aceite_counted, cpsv_barro_counted, cpsv_grava_counted, cpsv_hielo_counted, cpsv_seca_counted])
data = np.array([cpfa_granizo, cpfa_hielo, cpfa_niebla, cpfa_seco, cpfa_nieve, cpsv_mojada, cpsv_aceite, cpsv_barro, cpsv_grava, cpsv_hielo, cpsv_seca])
	
for i in np.arange(0,11):
	aux = data[i].select('count').collect()
	if(len(aux) > 0): 
		aux[0] = str(aux[0])
		aux2 = re.sub(r'[^0-9]', '', ''.join(aux))
		counter[i] = int(aux2)
		valid = True

if(valid == True): 
	df_result1 = spark.createDataFrame([ 
									("Condiciones Meteorologicas: Granizo", int(counter[0])),
									("Condiciones Meteorologicas: Hielo", int(counter[1])), 
									("Condiciones Meteorologicas: Niebla", int(counter[2])),
									("Condiciones M Seco y Despejado", int(counter[3])), 
									("Condiciones Meteorologicas: Nieve", int(counter[4])), 
									("Condiciones de la Via: Mojada", int(counter[5])), 
									("Condiciones de la Via: Derrape por aceite", int(counter[6])), 
									("Condiciones de la Via: Derrape por barro", int(counter[7])), 
									("Condiciones de la Via: Via con grava", int(counter[8])), 
									("Condiciones de la Via: Derrape por hielo", int(counter[9])), 
									("Condiciones de la Via: Siniestro en via seca y despejada", int(counter[10]))
									], schema)
	df_result1.orderBy(df_result1["Number of accidents"].desc()).show(df_result1.count(), False)
else: 
	print("We couldn't find a district with the name you searched")

	
	

	





	

	
