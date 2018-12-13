from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext
import string
import sys
import pandas as pd

if len(sys.argv) < 4:
	print("Usage: chooseStreet.py [input_file] [distrito] [day]")
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("chooseStreet")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)
	
	fi = sys.argv[1]
	distrito = sys.argv[2].upper()
	day = sys.argv[3].upper()

	data = pd.read_csv("input.csv")

	data['DISTRITO'] = data['DISTRITO'].str.rstrip(' ')

	data_distrito_day = data[(data['DISTRITO'] == distrito) & (data['DIA SEMANA'] == day)]
	
	lugares_distrito_day = data_distrito_day[['LUGAR ACCIDENTE']]

	total = lugares_distrito_day.count()

	totalInt = total[0]

	lugares_distrito_day_count = lugares_distrito_day.groupby(['LUGAR ACCIDENTE']).size().to_frame('COUNT')

	lugares_distrito_day_count['PERCENT'] = lugares_distrito_day_count['COUNT'] / totalInt * 100
	
	result = lugares_distrito_day_count.sort_values('PERCENT', ascending=False)

	print(result.to_string())
