from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext
import string
import sys
import pandas as pd

if len(sys.argv) != 4:
	print("Usage: chooseStreet.py [input_file] [distrito] [day]")
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("horasDistrito")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)
	
	fi = sys.argv[1]
	distrito = sys.argv[2].upper()
	day = sys.argv[3].upper()

	data = pd.read_csv("input.csv")

	data['DISTRITO'] = data['DISTRITO'].str.rstrip(' ')

	data_distrito_day = data[(data['DISTRITO'] == distrito) & (data['DIA SEMANA'] == day)]
	
	lugares_distrito_day = data_distrito_day[['RANGO HORARIO', 'LUGAR ACCIDENTE']]

	result  = lugares_distrito_day.groupby(['LUGAR ACCIDENTE', 'RANGO HORARIO']).size().to_frame('COUNT').sort_values(['LUGAR ACCIDENTE', 'COUNT'], ascending=[True, False])

	print(result.to_string())
