# -*- coding: utf-8 -*-
import os
import Config
import pymysql


db = pymysql.connect(host=Config.DB_HOST, user=Config.DB_USER,
            port=Config.DB_PORT, password=Config.DB_PASSWD,
            db=Config.DB_NAME, charset='utf8')
cursor = db.cursor()


sql = "SELECT ptt_articles.year, ptt_content_analyze.work_type, ptt_content_analyze.salary \
       FROM ptt_articles, ptt_content_analyze \
       WHERE ptt_articles.article_uuid = ptt_content_analyze.article_id"

def main():
    cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        print '%s, %s, %s' % (row[0], row[1], row[2])


if __name__ == "__main__":
    main()


"""
import json
import os
import re
import pymysql
import Config
import requests
import random
from os.path import isfile, join
from shutil import copyfile
from pprint import pprint

DB_HOST = '140.118.70.162'
DB_USER = 'jobguide'
DB_PASSWD = 'zNW3hw1HjMsQvOc9'
DB_NAME = 'jobguide'
TMP_PATH = Config.TMP_PATH


DB = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWD, db=DB_NAME, port=13306, charset='utf8')

CURSOR = DB.cursor()

SQL = "INSERT INTO ptt_articles(title, author, board, content, date, ip, article_uuid) VALUES (%s, %s, %s, %s, %s, %s, %s)

def main():
    #f_2015 = open('2015.txt', 'w')
    #f_2016 = open('2016.txt', 'w')
    job_type = ['資訊科技', '傳產製造', '工商服務', '民生服務', '文教傳播']
    salary = [31000, 32000, 33000, 34000, 35000, 36000, 37000, 38000, 39000, 40000, 41000, 42000, 43000, 44000,45000, 46000, 47000, 48000, 49000, 50000, 51000, 52000, 53000, 54000, 55000, 56000, 57000, 58000, 59000, 60000]

    for i in range(0, 2000):
        print '%s,%d' % (job_type[random.randint(0,len(job_type) - 1)], salary[random.randint(0,len(salary) - 1)])


    mathe.append(random.randint(1, 12))

    for i in range(0, 6000):
        f_2015.writelines('%d,%d ' %  (id_list[i], mathe[i]))



    id_list = random.sample(range(10000000, 99999999), 6000)
    mathe = []
    for i in range(0, 6000):
        mathe.append(random.randint(1, 12))

    for i in range(0, 6000):
        f_2016.writelines('%d,%d ' % (id_list[i], mathe[i]))



    command = "cd %s && python ../pttcrawler.py -b %s -i %d %d" % (os.path.basename(TMP_PATH), 'job', 1, -1)
    #command = "python pttcrawler.py -b %s -i %d %d" % (PTT_BOARD, pageStart, pageEnd)
    print command
    os.system(command)

    # Modify JSON Format
    regex = re.compile(".+-[0-9]+\.json")
    jsonFiles = [f for f in os.listdir(TMP_PATH) if isfile(join(TMP_PATH, f)) and regex.match(f)]
    for filename in jsonFiles:
        fullPath = join(TMP_PATH, filename)
        file = open(fullPath, 'r')
        lines = file.readlines()
        lines[0] = '[\n'
        lines[len(lines) - 1] = ']'
        file.close()

        newFilename = "%s_%d_parsed.json" % (filename[:-5], int(time.time()))
        newFullpath = join(TMP_PATH, newFilename)
        file = open(newFullpath, 'w')
        file.writelines(lines)
        file.close()
    file = open("job-200-300.json", 'r')
    lines = file.readlines()
    lines[0] = '[\n'
    lines[len(lines) - 1] = '}\n]'
    file.close()

    file = open("job-200-300_update.json", 'w')
    file.writelines(lines)
    file.close()
    with open('job-200-300_update.json','r') as json_file:
        data = json.open(json_file)

    regex = re.compile(r".+-[0-9]+_parsed\.json")
    jsonFiles = [f for f in os.listdir(TMP_PATH) if isfile(join(TMP_PATH, f)) and regex.match(f)]
    regex = re.compile(r"[0-9]+")

    for filename in jsonFiles:
        fullPath = join(TMP_PATH, filename)
        temp = json.load(open(fullPath))

        #data = json.open('C:\Users\user\Desktop\PTTCrawler\PttJobCrawler\JobRobot\job-200-300_update.json')

        for data in temp:
            if data['author'] == '' or data['article_title'] == '' or data['date'] == '':
                CURSOR.execute(SQL, (data['article_title'],
                         data['author'],
                         data['board'],
                         data['content'],
                         data['date'],
                         data['ip'],
                         data['article_id'] ))

    CURSOR.execute(SQL, (data[0]['article_title'],
                         data[0]['author'],
                         data[0]['board'],
                         data[0]['content'],
                         date_time,
                         data[0]['ip'],
                         data[0]['article_id'] ))
    #json_data[0]['date'][0] = ' '

    #DB.commit()
    for data in json_data:
        print (data['author'])

def format_date(datetime):
    temp = datetime.split(' ')
    format_datetime = '%s-%s-%s %s' %(temp[4], month(temp[1]), temp[2], temp[3])
    return format_datetime

def month(month):
    return{
        'Jan' : '01',
        'Feb' : '02',
        'Mar' : '03',
        'Apr' : '04',
        'May' : '05',
        'Jun' : '06',
        'Jul' : '07',
        'Aug' : '08',
        'Sep' : '09',
        'Oct' : '10',
        'Nov' : '11',
        'Dec' : '12'
    }[month]




if __name__ == "__main__":
        main()
        """
