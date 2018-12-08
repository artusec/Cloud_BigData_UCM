from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext
import string
import sys
import pandas as pd

if len(sys.argv) < 3:
	print("Usage: dia_distrito.py [input_file] [distrito]")
	exit(-1)
else:
	conf = SparkConf().setMaster('local').setAppName("distrito_calle")
	sc = SparkContext(conf = conf)
	spark = SparkSession(sc)
	
	fi = sys.argv[1]
	distrito = sys.argv[2].upper()

	data = pd.read_csv("input.csv")

	data['DISTRITO'] = data['DISTRITO'].str.rstrip(' ')

	data_distrito = data[data['DISTRITO'] == distrito]
	
	lugares_distrito = data_distrito[['LUGAR ACCIDENTE']]

	lugares_distrito_count = lugares_distrito.groupby(['LUGAR ACCIDENTE']).size().to_frame('COUNT')
	
	result = lugares_distrito_count.sort_values('COUNT', ascending=False)

	print(result.to_string())
