# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from os.path import isfile, join
import os
import codecs

conf = SparkConf().setAppName("JobGuide").setMaster("local")
sc = SparkContext(conf=conf)
def formatM(month):
	return str(month) + 'month'

files_path = ['./Crawler/Job/2013.txt', './Crawler/Job/2014.txt', './Crawler/Job/2015.txt', './Crawler/Job/2016.txt', './Crawler/Job/2017.txt']

for path in files_path:
	file = sc.textFile(path)
	data = file.flatMap(lambda x: x.split('\n'))\
				.map(lambda x: x.split(','))\
				.map(lambda x: (x[1], 1))\
				.reduceByKey(lambda a, b: a + b)

	name = path[-8:]
	txt = codecs.open('%s/Result/Job/%s' % ('./Crawler', name), 'w', encoding = 'utf-8')
	for each in data.top(12):
		txt.write('%s,%s\n' % (each[0], each[1]))
		print '%s, %s' % (formatM(each[0]), each[1])
	txt.close()
