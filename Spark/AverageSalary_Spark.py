# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from os.path import isfile, join
from operator import add
from datetime import datetime
import os
import pyspark
import codecs
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sc = pyspark.SparkContext()

def dosomething(a, b):
	a1 = a.split(',')
	b1 = b.split(',')

	num = int(a1[0]) + int(b1[0])
	total = int(a1[1]) + int(b1[1])
	return str(num) + ',' + str(total)

files_path = ['./Crawler/Average/2013.txt', './Crawler/Average/2014.txt', './Crawler/Average/2015.txt', './Crawler/Average/2016.txt', './Crawler/Average/2017.txt']

for path in files_path:
	print path
	file = sc.textFile(path)
	data = file.flatMap(lambda x: x.split('\n')).map(lambda x: x.split(','))\
				.map(lambda x: (x[0], '1,' + str(x[1])))\
				.reduceByKey(lambda a, b :dosomething(a, b))\
				.take(18)

	tmp_path = path[-8:]
	txt = codecs.open('%s/Result/Average/%s' % ('./Crawler', tmp_path), 'w', encoding = 'utf-8')
	for (name, string) in data:
		average = string.split(',')
		print '%s,%d' % (name, int(average[1])/int(average[0]))
		txt.write('%s,%s\n' % (name, str(int(average[1])/int(average[0]))))
	print "|*" * 10 + path + "*|"*10
	txt.close()
