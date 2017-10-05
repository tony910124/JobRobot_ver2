# -*- coding: utf-8 -*-
import os
import Config
import pymysql
import codecs


db = pymysql.connect(host=Config.DB_HOST, user=Config.DB_USER,
            port=Config.DB_PORT, password=Config.DB_PASSWD,
            db=Config.DB_NAME, charset='utf8')
cursor = db.cursor()


sql = "SELECT ptt_articles.year, ptt_content_analyze.work_type, ptt_content_analyze.salary \
       FROM ptt_articles, ptt_content_analyze \
       WHERE ptt_articles.article_uuid = ptt_content_analyze.article_id"

def main():
    #file = codecs.open('Average_Salary.txt', 'w', 'utf-8')
    year_dic = {}
    years = []

    
    try:
        cursor.execute(sql)
        result = cursor.fetchall()

        print len(result)
        for row in result:
            try:
                if row[0] in year_dic:
                    year_dic[row[0]].append(row[1:])
                else:
                    year_dic['%s' % row[0].decode('utf-8')] = []
            except Exception as e:
                print e
    except Exception as e:
        print e


    for year in year_dic:
        file = codecs.open('%s/Average/%s.txt' % (Config.scriptPath, year), 'w', 'utf-8')
        for job_detail in year_dic[year]:
            file.write('%s,%s\n' % (job_detail[0], job_detail[1]))
        file.close()
    
    #print years

if __name__ == "__main__":
    main()