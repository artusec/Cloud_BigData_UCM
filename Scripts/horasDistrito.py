from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext
import matplotlib.pyplot as plt
import string
import sys
import pandas as pd

if len(sys.argv) != 4:
	print("Usage: horasDistrito.py [input_file] [distrito] [day]")
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
	
	horas_distrito_day = data_distrito_day[['RANGO HORARIO']]

	total = horas_distrito_day.count()

	totalInt = total[0]

	horas_distrito_day_count = horas_distrito_day.groupby(['RANGO HORARIO']).size().to_frame('COUNT').reset_index()

	horas_distrito_day_count['PERCENT'] = horas_distrito_day_count['COUNT'] / totalInt * 100
	
	result = horas_distrito_day_count.sort_values('PERCENT', ascending=False)

	print(result.to_string())

	result.plot(x='RANGO HORARIO', y='COUNT', kind='barh')

	plt.show()
