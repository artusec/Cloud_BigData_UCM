from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import mean, desc
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import array_contains


import string
import sys
import re 

if len(sys.argv) < 3:
	print "Usage: contexto_mas_accidentes_por_distrito [file] [district]"
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

	cpfa_granizo = data.select('CPFA Granizo').filter(data['CPFA Granizo'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPFA Granizo').count()
	cpfa_hielo = data.select('CPFA Hielo').filter(data['CPFA Hielo'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPFA Hielo').count()
	cpfa_niebla = data.select('CPFA Niebla').filter(data['CPFA Niebla'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPFA Niebla').count()
	cpfa_seco = data.select('CPFA Seco').filter(data['CPFA Seco'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPFA Seco').count()
	cpfa_nieve = data.select('CPFA Nieve').filter(data['CPFA Nieve'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPFA Nieve').count()
	cpsv_mojada = data.select('CPSV Mojada').filter(data['CPSV Mojada'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPSV Mojada').count()
	cpsv_aceite = data.select('CPSV Aceite').filter(data['CPSV Aceite'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPSV Aceite').count()
	cpsv_barro = data.select('CPSV Barro').filter(data['CPSV Barro'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPSV Barro').count()
	cpsv_grava = data.select('CPSV Grava Suelta').filter(data['CPSV Grava Suelta'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPSV Grava Suelta').count()
	cpsv_hielo = data.select('CPSV Hielo').filter(data['CPSV Hielo'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPSV Hielo').count()
	cpsv_seca = data.select('CPSV Seca y Limpia').filter(data['CPSV Seca y Limpia'] != "NO").filter(data['DISTRITO'].contains(district)).groupBy('CPSV Seca y Limpia').count()

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
	cpfa_niebla_counted = 0
	cpsv_seca_counted = 0
	cpsv_mojada_counted = 0
	cpsv_aceite_counted = 0
	cpsv_barro_counted = 0
	cpsv_grava_counted = 0
	cpsv_hielo_counted = 0

	cpfa_granizo_aux = cpfa_granizo.select('count').collect()
	if(len(cpfa_granizo_aux) > 0): 
		cpfa_granizo_aux[0] = str(cpfa_granizo_aux[0])
		cpfa_granizo_aux2 = re.sub(r'[^0-9]', '', ''.join(cpfa_granizo_aux))
		cpfa_granizo_counted = int(cpfa_granizo_aux2)
		valid = True

	cpfa_hielo_aux = cpfa_hielo.select('count').collect()
	if(len(cpfa_hielo_aux) > 0): 
		cpfa_hielo_aux[0] = str(cpfa_hielo_aux[0])
		cpfa_hielo_aux2 = re.sub(r'[^0-9]', '', ''.join(cpfa_hielo_aux))
		cpfa_hielo_counted = int(cpfa_hielo_aux2)
		valid = True

	cpfa_niebla_aux = cpfa_niebla.select('count').collect()
	if(len(cpfa_niebla_aux) > 0):
		cpfa_niebla_aux[0] = str(cpfa_niebla_aux[0])
		cpfa_niebla_aux2 = re.sub(r'[^0-9]', '', ''.join(cpfa_niebla_aux))
		cpfa_niebla_counted = int(cpfa_niebla_aux2)
		valid = True

	cpfa_seco_aux = cpfa_seco.select('count').collect()
	if(len(cpfa_seco_aux) > 0):
		cpfa_seco_aux[0] = str(cpfa_seco_aux[0])
		cpfa_seco_aux2 = re.sub(r'[^0-9]', '', ''.join(cpfa_seco_aux))
		cpfa_seco_counted = int(cpfa_seco_aux2)
		valid = True

	cpfa_nieve_aux = cpfa_nieve.select('count').collect()
	if(len(cpfa_nieve_aux) > 0):
		cpfa_nieve_aux[0] = str(cpfa_nieve_aux[0])
		cpfa_nieve_aux2 = re.sub(r'[^0-9]', '', ''.join(cpfa_nieve_aux))
		cpfa_nieve_counted = int(cpfa_nieve_aux2)
		valid = True

	cpsv_mojada_aux = cpsv_mojada.select('count').collect()
	if(len(cpsv_mojada_aux) > 0):
		cpsv_mojada_aux[0] = str(cpsv_mojada_aux[0])
		cpsv_mojada_aux2 = re.sub(r'[^0-9]', '', ''.join(cpsv_mojada_aux))
		cpsv_mojada_counted = int(cpsv_mojada_aux2)
		valid = True

	cpsv_aceite_aux = cpsv_aceite.select('count').collect()
	if(len(cpsv_aceite_aux) > 0):
		cpsv_aceite_aux[0] = str(cpsv_aceite_aux[0])
		cpsv_aceite_aux2 = re.sub(r'[^0-9]', '', ''.join(cpsv_aceite_aux))
		cpsv_aceite_counted = int(cpsv_aceite_aux2)
		valid = True

	cpsv_barro_aux = cpsv_barro.select('count').collect()
	if(len(cpsv_barro_aux) > 0):
		cpsv_barro_aux[0] = str(cpsv_barro_aux[0])
		cpsv_barro_aux2 = re.sub(r'[^0-9]', '', ''.join(cpsv_barro_aux))
		cpsv_barro_counted = int(cpsv_barro_aux2)
		valid = True

	cpsv_grava_aux = cpsv_grava.select('count').collect()
	if(len(cpsv_grava_aux) > 0):
		cpsv_grava_aux[0] = str(cpsv_grava_aux[0])
		cpsv_grava_aux2 = re.sub(r'[^0-9]', '', ''.join(cpsv_grava_aux))
		cpsv_grava_counted = int(cpsv_grava_aux2)
		valid = True

	cpsv_hielo_aux = cpsv_hielo.select('count').collect()
	if(len(cpsv_hielo_aux) > 0):
		cpsv_hielo_aux[0] = str(cpsv_hielo_aux[0])
		cpsv_hielo_aux2 = re.sub(r'[^0-9]', '', ''.join(cpsv_hielo_aux))
		cpsv_hielo_counted = int(cpsv_hielo_aux2)
		valid = True

	cpsv_seca_aux = cpsv_seca.select('count').collect()
	if(len(cpsv_seca_aux) > 0):
		cpsv_seca_aux[0] = str(cpsv_seca_aux[0])
		cpsv_seca_aux2 = re.sub(r'[^0-9]', '', ''.join(cpsv_seca_aux))
		cpsv_seca_counted = int(cpsv_seca_aux2)
		valid = True


	if(valid == True): 
		df_result = spark.createDataFrame([("Condiciones Meteorologicas: Granizo", cpfa_granizo_counted),
										("Condiciones Meteorologicas: Hielo", cpfa_hielo_counted), 
										("Condiciones Meteorologicas: Niebla", cpfa_nieve_counted),
										("Condiciones Meteorologicas: Seco y Despejado", cpfa_seco_counted), 
										("Condiciones Meteorologicas: Nieve", cpfa_nieve_counted), 
										("Condiciones de la Via: Mojada", cpsv_mojada_counted), 
										("Condiciones de la Via: Derrape por aceite", cpsv_aceite_counted), 
										("Condiciones de la Via: Derrape por barro", cpsv_barro_counted), 
										("Condiciones de la Via: Via con grava", cpsv_grava_counted), 
										("Condiciones de la Via: Derrape por hielo", cpsv_hielo_counted), 
										("Condiciones de la Via: Siniestro en via seca y despejada", cpsv_seca_counted)
										], schema)
		df_result.orderBy(df_result["Number of accidents"].desc()).show(20, False)
	else: 
		print("We couldn't find a district with the name you searched")

	
	

	





	

	
