#!/data/server/python3.7/bin/python3
#!-*- coding:utf8 -*-
"""
  Copyright (c) 2020,掌阅科技
  All rights reserved.
  摘    要：
  创 建 者：lijun
  创建日期：2022-05-24_17:55:05
  项目：  数据工程高优先级任务未就绪 超时监控
"""

import requests
import json
import time
import socket
import pymysql
import datetime
import pandas as pd
from multiprocessing import Pool,Manager
#from datetime import datetime, timedelta ,timezone

def getYesterday(d):
    today = datetime.date.today()
    oneday = datetime.timedelta(days=d)
    yesterday = today - oneday
    return yesterday


def get_table_length(ip,port,path,dsdate,dshour):
    url="http://{}:{}/webhdfs/v1{}/{}/{}?user.name=hive&op=GETCONTENTSUMMARY".format(ip,port,path,dsdate,dshour)
    #params={'user.name': 'hive','op': 'GETCONTENTSUMMARY'}
    #payload = json.dumps(params)
    headers = {'content-type': 'application/json'}
    response = requests.get(url=url, headers=headers)
    res=response.json()
    #print(res)
    try:
        ilength=res['ContentSummary']['length']
        print(path,dsdate,"存在")
    except:
    	print(path ,"不存在")
    	ilength=10
    return ilength


def getNameNode():
    for namenode in NAMENODES:
        url = 'http://{}:{}/webhdfs/v1?user.name=hive&op=GETCONTENTSUMMARY'.format(namenode, 50070)
        headers = {'content-type': 'application/json'}
        response = requests.get(url=url, headers=headers)
        namenode_res = response.json()
        if "RemoteException" in namenode_res:
            if namenode_res["RemoteException"]["message"].find("READ is not supported in state standby") >= 0:
                continue
            else:
                return namenode
    return namenode



def send_to_feishu(df,sid,iname,send_params):
    '''测试'''
    #send_url="https://open.feishu.cn/open-apis/bot/v2/hook/2fbc7bdf-0f3d-4ecb-ad69-facb9a111a9b"
    '''正式'''
    send_url="https://open.feishu.cn/open-apis/bot/v2/hook/afb4f2a4-d9c4-4a0a-84aa-945f5399dc1e"
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

def send_to_phone():
    send_url="https://www.linkedsee.com/alarm/channel/"
    send_params={"receiver": "18224504837","type": "phone","title": "数据工程P0","content": "数据工程SLA告警","genre": "play"}
    send_payload = json.dumps(send_params)
    send_headers = {'content-type': 'application/json','Servicetoken': 'b08e142c62bdc73680b6297881b99be3'}
    proxy =  { 'https' : "192.168.7.24:44551"}
    response = requests.post(url=send_url,proxies=proxy,headers=send_headers,data=send_payload)

if __name__ == '__main__':
    conn = pymysql.connect(host='127.0.0.1', db='dp', user='root', passwd='ops-cm', port=int(3306))
    now = datetime.datetime.now()
    fsql='''SELECT * from upstream_table_monitor_hour'''
    df = pd.read_sql(fsql, conn)
    lte=datetime.datetime.now().isoformat()
    NAMENODES = ['10.100.105.12', '10.100.105.13']
    namenode_enable = getNameNode()
    sid=''
    iname='郭金'
    send_params={'msg_type': 'interactive', 'card': {'config': {'wide_screen_mode': True}, 'header': {'template': 'red', 'title': {'content': '数据工程-P0上游数据延迟', 'tag': 'plain_text'}}, 'elements': [{'fields': [{'is_short': True, 'text': {'content': '** 时间**\n2021-02-23 20:17:51', 'tag': 'lark_md'}}, {'is_short': True, 'text': {'content': '** 事件 ID：：**\nP0数据延迟', 'tag': 'lark_md'}}, {'is_short': False, 'text': {'content': '', 'tag': 'lark_md'}}, {'is_short': True, 'text': {'content': '** 延迟表：**\n延迟小时表:bmh.read.commercial.di', 'tag': 'lark_md'}}, {'is_short': True, 'text': {'content': '** 一级负责人：**\n<at id=ou_833c66c11857a89b0b6351bd9d58b519>李军</at>', 'tag': 'lark_md'}}, {'is_short': False, 'text': {'content': '', 'tag': 'lark_md'}}, {'is_short': True, 'text': {'content': "** 任务天级别延迟状态：** \n  ", 'tag': 'lark_md'}}], 'tag': 'div'}]}}
    #send_params['card']['elements'][0]['fields'][4]['text']['content']='** 一级负责人：**\n<at id={}>{}</at>'.format(sid,iname)
    #send_params['card']['elements'][0]['fields'][4]['text']['content']='** 一级负责人：**\n @{}{}'.format(sid,iname)
    send_params['card']['elements'][0]['fields'][0]['text']['content']='** 时间**\n{}'.format(lte)
    #print(fsql)
    for i in range(len(df)):
        #print(i)
        #print(df.iloc[i]['data_table'])
        iname=df.iloc[i]['person_in_charge']
        send_params['card']['elements'][0]['fields'][4]['text']['content']='** 一级负责人：**\n<at id={}>{}</at>'.format(sid,iname)
        path=df.iloc[i]['table_path']
        if  path:
            table_hour=int(df.iloc[i]['err_hour'])
            idate=((datetime.datetime.now()-datetime.timedelta(hours=table_hour)).strftime("%Y-%m-%d"))
            ihour=((datetime.datetime.now()-datetime.timedelta(hours=table_hour)).strftime("%H"))
            dsdate='ds={}'.format(idate)
            dshour='hour={}'.format(ihour)
            lenght=get_table_length(namenode_enable,50070,path,dsdate,dshour)
            #print(lenght)
            if lenght <= 100 :
                send_params['card']['elements'][0]['fields'][3]['text']['content']='** 延迟表**\n{} \n**表大小:** {}'.format(df.iloc[i]['table_name'],lenght)
                send_params['card']['elements'][0]['fields'][-1]['text']['content']="** 监控到数据表为0的小时：** \n{} d {} hour\n {}".format(dsdate,dshour,df.iloc[i]['table_info'])
                send_to_feishu(df.iloc[i],sid,iname,send_params)
                if df.iloc[i]['flag'] == "1":
                    #print("11111111")
                    send_to_phone()
