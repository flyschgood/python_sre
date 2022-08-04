#!-*- coding:utf8 -*-
"""
  Copyright (c) 2020,掌阅科技
  All rights reserved.
  摘    要：
  创 建 者：lijun
  创建日期：2021-12-25_20:55:05
  项目：  数据工程高优先级任务未就绪 超时监控
"""

import requests
import json
import time
import socket
import pymysql
import datetime
#import emoji
import pandas as pd
from impala.dbapi import connect
from multiprocessing import Pool,Manager
#from datetime import datetime, timedelta ,timezone

#def getYesterday():
#    today = datetime.date.today()
#    oneday = datetime.timedelta(days=1)
#    yesterday = today - oneday
#    return yesterday

def getYesterday(d):
    today = datetime.date.today()
    oneday = datetime.timedelta(days=d)
    yesterday = today - oneday
    return yesterday




def get_table_length(ip,port,path,dsdate):
    url="http://{}:{}/webhdfs/v1{}/{}?user.name=hive&op=GETCONTENTSUMMARY".format(ip,port,path,dsdate)
    #params={'user.name': 'hive','op': 'GETCONTENTSUMMARY'}
    #payload = json.dumps(params)
    headers = {'content-type': 'application/json'}
    response = requests.get(url=url, headers=headers)
    res=response.json()
    print(res)
    try:
        ilength=res['ContentSummary']['length']
        print(path,dsdate,"存在")
    except:
    	print(path ,"不存在")
    	ilength=10000
    return ilength

def send_to_phone():
    send_url="https://www.linkedsee.com/alarm/channel/"
    send_params={"receiver": "18224504837","type": "phone","title": "数据工程P0","content": "数据工程SLA告警","genre": "play"}
    send_payload = json.dumps(send_params)
    send_headers = {'content-type': 'application/json','Servicetoken': 'b08e142c62bdc73680b6297881b99be3'}
    proxy =  { 'https' : "192.168.7.24:44551"}
    response = requests.post(url=send_url,proxies=proxy,headers=send_headers,data=send_payload)



def send_to_feishu(df,sid,iname,send_params):
    #send_url="http://opsbot.ireader.mobi/apis/feishu/messages/customsend/card/dataplatform"
    #lte=datetime.datetime.now().isoformat()
    #print(lte)
    #send_params={"title":"impala超时查询", "body": "**时间:** {} \n**查询id**: {} \n**查询用户:** {} \n**查询持续时间**: {} \n**项目:**  impala \n**负责人:** <at email=lijun@zhangyue.com></at> \n".format(lte,df.iat[1,0],df.iat[1,1],df.iat[1,2]), "action": "http://10.100.94.71:5601/app/dashboards#/view/ad5b0190-1b57-11ec-bac7-3de2982bf0e7?_g=(filters:!(),query:(language:kuery,query:''),refreshInterval:(pause:!t,value:0),time:(from:now-1h,to:now))", "to_user": "", "title_color": "blue"}
    #send_params={"title":"数据工程高优先级任务未就绪", "body": "**影响:**\n **平台:** {} \n  **一级菜单:** {} \n **二级菜单:** {} \n **未就绪表:** {} \n **负责人:** @{} \n".format(df['platform'],df['first_level_menu'],df['Secondary_level_menu'],df['data_table'],df['person_in_charge']), "action": "https://q7w8vltyes.feishu.cn/wiki/wikcnE1rKAlcqduDsttSJn91SoD?sheet=wWdVvs", "to_user": "", "title_color": "blue"}
    '''测试'''
    #send_url="https://open.feishu.cn/open-apis/bot/v2/hook/2fbc7bdf-0f3d-4ecb-ad69-facb9a111a9b"
    send_url="https://open.feishu.cn/open-apis/bot/v2/hook/afb4f2a4-d9c4-4a0a-84aa-945f5399dc1e"
    '''正式'''
    #send_url="https://open.feishu.cn/open-apis/bot/v2/hook/24bbd8e6-5927-457b-8840-37ebf463735c"
    #send_url="https://open.feishu.cn/open-apis/bot/v2/hook/f8d73d85-fb5c-4fc3-94e8-f864783d26b9"
    #send_url="https://open.feishu.cn/open-apis/bot/v2/hook/da8b65c6-5eb6-41f7-acac-6109e6d33465"
    #with open('/data/scripts/python/card.cps', 'r') as f:
    #    send_params=json.loads(f.read())
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    print(send_params)
    #send_params=json.loads(send_params)
    send_payload = json.dumps(send_params)
    send_headers = {'content-type': 'application/json'}
    proxy =  { 'https' : "192.168.7.24:44551"}
    response = requests.post(url=send_url,proxies=proxy,headers=send_headers,data=send_payload)

def datetime_toString(dt):
    return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")


if __name__ == '__main__':
    #conn=connect(host='10.100.105.249', port=21050)
    conn=connect(host='10.100.105.249', port=10010,auth_mechanism='PLAIN',user="hive",password="hive",database="zy_dw")
    #cursor = conn.cursor()
    '''参数设置'''
    lte=datetime.datetime.now().isoformat()
    now = datetime.datetime.now()
    ihour=(datetime.datetime.now()-datetime.timedelta(hours=0)).strftime("%H")
    today=(datetime.datetime.now()-datetime.timedelta(hours=0)).strftime("%Y-%m-%d")
    #today = datetime.date.today()
    sid=''
    iname='王国庆'
    send_params={'msg_type': 'interactive', 'card': {'config': {'wide_screen_mode': True}, 'header': {'template': 'red', 'title': {'content': '数据工程-小时级别数据为0', 'tag': 'plain_text'}}, 'elements': [{'fields': [{'is_short': True, 'text': {'content': '** 时间**\n2021-02-23 20:17:51', 'tag': 'lark_md'}}, {'is_short': True, 'text': {'content': '** 事件 ID: **\n小时级别数据延迟', 'tag': 'lark_md'}}, {'is_short': False, 'text': {'content': '', 'tag': 'lark_md'}}, {'is_short': True, 'text': {'content': '** 延迟表：**\n延迟小时表:bmh.read.commercial.di', 'tag': 'lark_md'}}, {'is_short': True, 'text': {'content': '** 一级负责人：**\n<at id=ou_833c66c11857a89b0b6351bd9d58b519>李军</at>', 'tag': 'lark_md'}}, {'is_short': False, 'text': {'content': '', 'tag': 'lark_md'}}, {'is_short': True, 'text': {'content': "** 监控到数据表为0的小时：** \n  ", 'tag': 'lark_md'}}], 'tag': 'div'}]}}
    yds=getYesterday(1)

    #allsql=''' SELECT  biz_info        ,concat('最新数据时间: ',MAX(concat(biz_ds,if(schedual_type = '小时',concat(' ',biz_hour),'')))) AS biz_date FROM cps_schedual_info GROUP BY  biz_info ORDER BY biz_info LIMIT 4;'''
    #allsql='''select * from zy_dw.dwd_table_output_monitor_di where err_ds="{}";'''.format(yds)
    allsql='''select * from zy_dw.dwd_table_output_monitor_hi where ds="{}" and hour="{}"'''.format(today,ihour)
    #allsql='''select * from zy_dw.dwd_table_output_monitor_hi where ds="{}" and hour="{}";'''.format(today,'10')

    #print(allsql)
    df = pd.read_sql(allsql, conn)
    #print(df)
    #ntime=datetime.datetime.now()
    #rtime=datetime_toString(df.iloc[0]['result2'])
    #ctime=(ntime-rtime).seconds
    #atime=ctime/60/60
    #send_params['card']['elements'][0]['fields'][4]['text']['content']='** 一级负责人：**\n @{}'.format(iname)
    send_params['card']['elements'][0]['fields'][4]['text']['content']='** 一级负责人：**\n<at id={}>{}</at>'.format(sid,iname)
    send_params['card']['elements'][0]['fields'][0]['text']['content']='** 时间**\n{}'.format(lte)
    #send_params['card']['elements'][0]['fields'][-1]['text']['content']="** 任务延迟{}小时：** \n{}".format(atime,df)
    #print(ctime)
    #print(df)
    for i in range(len(df)):
        #print(df.iloc[i]['dwd_table_output_monitor_hi.table_rows'])
        if df.iloc[i]['dwd_table_output_monitor_hi.table_rows'] <= 0 :
            #print(df.iloc[i]['dwd_table_output_monitor_hi.table_rows'])
            send_params['card']['elements'][0]['fields'][3]['text']['content']='** 延迟表**\n{}'.format(df.iloc[i]['dwd_table_output_monitor_hi.table_name'])
            send_params['card']['elements'][0]['fields'][-1]['text']['content']="** 监控到数据表为0的小时：** \n{} {} hour".format(df.iloc[i]['dwd_table_output_monitor_hi.err_ds'],df.iloc[i]['dwd_table_output_monitor_hi.err_hour'])
            send_to_feishu(df,sid,iname,send_params)
            send_to_phone()
          
        #print('d')
