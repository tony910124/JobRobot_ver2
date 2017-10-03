# -*- coding: utf-8 -*-
import os
import re
import json
import Config
import pymysql
from os.path import isfile, join
from os import sep

sql = "INSERT INTO ptt_comments (article_id, push_content, push_datatime, push_tag, push_userid) \
						VALUES (%s, %s, %s, %s, %s)"
sql_print = "INSERT INTO ptt_comments (id) %s"
db = pymysql.connect(host=Config.DB_HOST, user=Config.DB_USER,
            port=Config.DB_PORT, password=Config.DB_PASSWD,
            db=Config.DB_NAME, charset='utf8')
cursor = db.cursor()

TMP_PATH = Config.TMP_PATH

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


			if docs[doc_i]['date'] == None or not(docs[doc_i]['date'][-4:].isdigit()):
				year = pre_year
			else:
				year = docs[doc_i]['date'][-4:]
				pre_year = year
			#print docs[doc_i]['date'][-4:]
			#print year
			print docs[doc_i]['article_id']
			updateToSQL(docs[doc_i]['messages'], doc_i + 1 - error_doc, year)
			

	db.commit()


def updateToSQL(messages, article_id, year):
	for message in messages:
		#print message['push_ipdatetime']
		try:
			time = re.sub('[/:]', ' ', message['push_ipdatetime'])
			time = time.split(' ')
			time.insert(0, year)
			for i in range(len(time)):
				if len(time[i]) > 4:
					time.remove(time[i])
					break
			#print time
			cursor.execute(sql, (article_id, 
								message['push_content'],
								"%s-%s-%s %s:%s:00" % (time[0], time[1], time[2], time[3], time[4]),
								message['push_tag'],
								message['push_userid']))
			print sql_print % article_id
		except Exception as e:
			print article_id
			print time

if __name__ == "__main__":
	main()