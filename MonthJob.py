# -*- coding: utf-8 -*-
import os
import Config
import pymysql
import codecs


db = pymysql.connect(host=Config.DB_HOST, user=Config.DB_USER,
            port=Config.DB_PORT, password=Config.DB_PASSWD,
            db=Config.DB_NAME, charset='utf8')
cursor = db.cursor()


sql = "SELECT ptt_articles.year, ptt_articles.month, ptt_articles.article_uuid \
       FROM ptt_articles, ptt_content_analyze \
       WHERE ptt_articles.article_uuid = ptt_content_analyze.article_id"

def main():
    year_dic = {}
    
    try:
        cursor.execute(sql)
        result = cursor.fetchall()

        print len(result)
        for row in result:
            #print row
            try:
                if row[0] in year_dic:
                    if row[1] in year_dic[row[0]]:
                        year_dic[row[0]][row[1]].append(row[2])
                    else:
                        year_dic[row[0]][row[1]] = []
                else:
                    year_dic[row[0]] = {}
            except Exception as e:
                print e
    except Exception as e:
        print e


    for year in year_dic:
        file = codecs.open('%s/Job/%s.txt' % (Config.scriptPath, year), 'w', 'utf-8')
        for month in year_dic[year]:
            for article in year_dic[year][month]:
                file.write('%s,%s\n' % (article, month))
        file.close()
    
    #print years

if __name__ == "__main__":
    main()