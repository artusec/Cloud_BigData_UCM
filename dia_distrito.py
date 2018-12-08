from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext
import string
import sys
import pandas as pd

if len(sys.argv) < 3:
	print("Usage: dia_distrito.py [input_file] [day_of_week]")
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("dia_distrito")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)
	
	fi = sys.argv[1]
	day = sys.argv[2].upper()

	data = pd.read_csv("input.csv")

	distritos_dia = data[data['DIA SEMANA'] == day]

	distritos = distritos_dia[['DISTRITO']]

	distritos_count = distritos.groupby(['DISTRITO']).size().to_frame('COUNT')
	
	result = distritos_count.sort_values('COUNT', ascending=False)

	print(result.to_string())
