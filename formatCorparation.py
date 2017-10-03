# -*- coding: utf-8 -*-
import os
import re
import json
import Config
import pymysql
from os.path import isfile, join
from os import sep

sql = "INSERT INTO ptt_corporation_rank (corparation, rank) \
						VALUES (%s, %s)"
sql_print = "INSERT INTO ptt_corporation_rank (id) %s"
db = pymysql.connect(host=Config.DB_HOST, user=Config.DB_USER,
            port=Config.DB_PORT, password=Config.DB_PASSWD,
            db=Config.DB_NAME, charset='utf8')
cursor = db.cursor()

def main():
	regex = re.compile(r".+_[0-9]+_parsed\.json")
	file = [f for f in os.listdir(TMP_PATH) if isfile(join(TMP_PATH, f)) and regex.match(f)]
	year = ""
	pre_year = ""
	#in ptt_article table is already ignore error, so the id number will go wrong
	#count the error doc and then the doc_i minus error doc, the number is ptt_article table id 
	error_doc = 0
	for filename in file:
		fullPath = join(TMP_PATH, filename)
		docs = json.load(open(fullPath))
		for doc_i in range(len(docs)): #len(docs)
			#if no message then go next

			if 'error' in docs[doc_i]:
				error_doc += 1
				continue
			if len(docs[doc_i]['messages']) == 0 :
				continue

			
			print docs[doc_i]['article_id']
	


if __name__ == "__main__":
	main()