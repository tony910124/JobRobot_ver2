# -*- coding: utf-8 -*-
import os
import re
import Config
import pymysql
from os.path import isfile, join
import codecs

sql_Aveage = "INSERT INTO ptt_spark_average (year, work_area, salary) VALUES (%s, %s, %s)"
sql_Average_print = "INSERT INTO ptt_spark_average (year) %s"

sql_Job = "INSERT INTO ptt_spark_job (year, month, total) VALUES (%s, %s, %s)"
sql_print = "INSERT INTO ptt_spark_job (id) %s"

db = pymysql.connect(host=Config.DB_HOST, user=Config.DB_USER,
            port=Config.DB_PORT, password=Config.DB_PASSWD,
            db=Config.DB_NAME, charset='utf8')
cursor = db.cursor()

AVERAGE_PATH = Config.SPARK_AVERAGE_PATH
JOB_PATH = Config.SPARK_JOB_PATH

def main():
	update(AVERAGE_PATH)
	update(JOB_PATH)
	db.commit()

def update(path):
	regex = re.compile(r"\d{4}\.txt")
	file_name = [f for f in os.listdir(path) if isfile(join(path, f)) and regex.match(f)]
	
	for name in file_name:
		full_path = join(path, name)
		file = codecs.open(full_path, 'r', 'utf-8')
		for line in file:
			tmp = line.split(',')
			print name[:4]
			print tmp
			if path == AVERAGE_PATH:
				cursor.execute(sql_Aveage, (name[:4], tmp[0], int(tmp[1])))
			if path == JOB_PATH:
				cursor.execute(sql_Job, (name[:4], tmp[0], int(tmp[1])))
if __name__ == '__main__':
	main()