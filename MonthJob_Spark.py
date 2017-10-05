from pyspark import SparkContext, SparkConf
import os

conf = SparkConf().setAppName("JobGuide").setMaster("local")
sc = SparkContext(conf=conf)


PATH = os.path.dirname(os.path.realpath(__file__))

RDDread = sc.textFile('2015.txt')
Nread = sc.textFile('2016.txt')

data = RDDread.flatMap(lambda x: x.split('\n')).map(lambda x: x.split(','))\
       .map(lambda x: (x[1], 1))\
       .reduceByKey(lambda a, b: a + b)

ndata = Nread.flatMap(lambda x: x.split('\n')).map(lambda x: x.split(','))\
       .map(lambda x: (x[1], 1))\
       .reduceByKey(lambda a, b: a + b)

def formatM(month):
    return str(month) + 'month'
# Lists the most popular

for each in data.top(12):
    print(formatM(each[0]), each[1])
print("********************2015***************************")


# Lists the most popular
for each in ndata.top(12):
    print(formatM(each[0]), each[1])
print("*********************2016**************************")
