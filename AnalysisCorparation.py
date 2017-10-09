# -*- coding: utf-8 -*-
import os
import re
import Config
import pymysql


sql_get = 'SELECT corporation FROM ptt_content_analyze'

sql_insert = 'INSERT INTO ptt_corporation_rank(corporation, rank) VALUES (%s, %s)'

db = pymysql.connect(host=Config.DB_HOST, user=Config.DB_USER,
                port=Config.DB_PORT, password=Config.DB_PASSWD,
                db=Config.DB_NAME, charset='utf8')
cursor = db.cursor()

def main():
	regexs = [
		u'原{0,1}徵人{0,1}(公司){0,1}(單位){0,1}[為:：-]{0,1}',
		u'派[駐遣]至{0,1}(公司){0,1}(單位){0,1}[:：-]{0,1}'
	]

	normal = [
		u'公司',
		u'企業',
		u'工作室',
		u'工作坊',
		u'基金會',
		u'集團',
		u'聯合會'
	]

	corporation_list = []

	corporation_list_rank = []

	#num = 0
	#get corporation list
	cursor.execute(sql_get)
	corporation_list = cursor.fetchall()
	for row_i in range(len(corporation_list)): 
		for corporation in corporation_list[row_i]:
			if corporation == None:
				continue

			for regex in regexs:
				corporation = formatCorparation(regex, normal, corporation)

			for regex in normal:
				regResult = re.search(regex, corporation)
				if regResult != None:
					corporation = corporation[:regResult.end()]
					break
			
			duplicate = False
			for tmp in corporation_list_rank:
				if corporation in tmp:
					duplicate = True
					break
			if duplicate == False:
				#num += 1
				#corporation_list_rank.append(corporation)
				#print corporation
				cursor.execute(sql_insert, (corporation, 0))
	
	#print len(corporation_list)
	#print num
	#print corporation_list_rank
	db.commit()	



def formatCorparation(first_regex, second_regex_list, content):
	regResult = re.search(first_regex, content)
	if regResult != None:
		for regex in second_regex_list:
			regTMP = re.search(regex, content[regResult.end():])
			if regTMP != None:
				content = content[regResult.end():]
				content = content[:regTMP.end()]
			return content
	return content

if __name__ == '__main__':
	main()