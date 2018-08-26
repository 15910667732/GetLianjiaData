#coding:utf-8

import json
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
import re
import csv
import sys
import codecs
import datetime


def generate_allurl(query_url,user_in_nub):  # 生成url
    #url = 'http://' + user_in_city + '.lianjia.com/ershoufang/fangshan/pg{}/'
    #https://bj.lianjia.com/ershoufang/fangshan/pg1rs%E9%98%B3%E5%85%89/
    for url_next in range(1, int(user_in_nub)):
        yield query_url.format(url_next)
        #print(query_url.format(url_next))


def get_allurl(generate_allurl):  # 分析url解析出每一页的详细url
    get_url = requests.get(generate_allurl, 'lxml')
    print(generate_allurl)
    if get_url.status_code == 200:
        re_set = re.compile('<a class.*?href="(.*?)" target="_blank".*?data-sl="">[^<]*</a>')
        re_get = re.findall(re_set, get_url.text)
        #print(re_get ,"\r\n")
        return re_get


def open_url(re_get):  # 分析详细url获取所需信
    res= requests.get(re_get)
    

    if res.status_code == 200:
        info = {}
        soup = BeautifulSoup(res.text, 'lxml')
        try:
            info['数据日期']=datetime.date.today().strftime('%Y-%m-%d')
            info['标题'] = soup.select('.main')[0].text
            info['总价'] = soup.select('.total')[0].text + u'万'
            info['每平方售价'] = soup.select('.unitPriceValue')[0].text
            info['参考总价'] = soup.select('#tax-text')[0].text
            info['建造时间'] = soup.select('.subInfo')[2].text
            info['小区名称'] = soup.select('.info')[0].text
            info['所在区域'] = soup.select('.info a')[0].text + ':' + soup.select('.info a')[1].text
            info['链家编号'] = str(re_get)[34:].rsplit('.html')[0]


            #获取基本信息
            for i in soup.select('.base li'):
                i = str(i)
                ptn=re.compile(r'<li><span class="label">(.*)</span>(.*)</li>')
                result=re.findall(ptn, i)
                if len(result)>=1:
                    if len(result[0])>=1:
                        info[result[0][0]] = result[0][1]
                    else:
                        info[result[0][0]] = ""
                else:
                    info['未知']=""

            #获取交易信息
            for i in soup.select('.transaction li'):
                i = str(i)

                result=re.findall(r'.*?\n<span class="label">(.*)</span>\n.*?<span>(.*)</span>.*?', i)
                if len(result)>=1:
                    if len(result[0])>=1:
                        info[result[0][0]] = result[0][1]
                    else:
                        info[result[0][0]] = ""
                else:
                    info['未知']=""

 
        except:
            print('1wrong')

        
                
        info['URL']=re_get
                
        print(info)

        return info


def update_to_MongoDB(one_page):  # update储存到MongoDB
    if db[Mongo_TABLE].update({'链家编号': one_page['链家编号']}, {'$set': one_page}, True): #去重复
        print('储存MongoDB 成功!')
        return True
    return False


def pandas_to_xlsx(info):  # 储存到xlsx
    pd_look = pd.DataFrame(info)
    pd_look.to_excel('链家二手房.xlsx', sheet_name='链家二手房')


def writer_to_text(list,csvfile):  # 储存到text
    jsonfile='C:\\study\\house\\'+save_filename+'.txt'
    
    try:
        with open(jsonfile,'a') as f:
            f.write(json.dumps(list,ensure_ascii=False)+ '\n')
            f.close()
            
    except:
        print('write file wrong')

def trans(jsonfile,csvfile):
    jsonData = codecs.open(jsonfile, 'r')
    # csvfile = open(csvfile, 'w') # 此处这样写会导致写出来的文件会有空行
    # csvfile = open(csvfile, 'wb') # python2下
    csvfile = open(csvfile, 'w', newline='') # python3下
    writer = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_ALL)
    flag = True
    for line in jsonData:
        dic = json.loads(line[0:-1])
        if flag:
            # 获取属性列表
            keys = list(dic.keys())
            #print (keys)
            writer.writerow(keys) # 将属性列表写入csv中
            flag = False
        # 读取json数据的每一行，将values数据一次一行的写入csv中
        writer.writerow(list(dic.values()))
    jsonData.close()
    csvfile.close()

def main(url,csvfile):

    #k=open_url(url)
    #print k
    writer_to_text(open_url(url),csvfile)    #储存到text文件
    
    
    # update_to_MongoDB(list)   #储存到Mongodb


if __name__ == '__main__':
    user_in_city = 'bj'
    user_in_nub = '2'

    #查询阳光二手房价
    url = 'http://' + user_in_city + '.lianjia.com/ershoufang/fangshan/pg{}rs%E9%98%B3%E5%85%89/'
    save_filename='ygsy'

    for i in generate_allurl(url,user_in_nub):
        #print(i)
        url_list=get_allurl(i)
        for url in url_list:
            print(url)
            pass
            main(url,save_filename)
            break

    jsonfile='C:\\study\\house\\'+save_filename+'.txt'
    csvfile='C:\\study\\house\\'+save_filename+'.csv'
    trans(jsonfile,csvfile)
