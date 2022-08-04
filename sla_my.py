#!/data/server/python3/bin/python3
#!-*- coding:utf8 -*-
"""
  Copyright (c) 2020,掌阅科技
  All rights reserved.
  摘    要：
  创 建 者：guojin
  创建日期：2022-06-14
  项目：  数据工程SLA
"""
import hashlib
import hmac
import sys
import email
import requests
import json
import time
import socket
import pymysql
import operator
#import datetime
import pandas as pd
#from impala.dbapi import connect
#from pyhive import hive
from multiprocessing import Pool,Manager
from datetime import datetime,date,timedelta, timezone

PROXY_HTTPS = {'https': "192.168.7.24:44551"}

def get_feishu_openid(email):
    tenant_access_token = get_feishu_app_tenant_access_token()
    openid_base_url = "https://open.feishu.cn/open-apis/user/v1/batch_get_id"
    openid_url = openid_base_url + "?emails={}".format(email)
    header = {"Content-Type": "application/json; charset=utf-8",
              "Authorization": "Bearer {}".format(tenant_access_token)}
    resp = requests.get(openid_url, headers=header, proxies=PROXY_HTTPS)
    if 'email_users' not in resp.json()['data']:
        return ""
    elif email not in resp.json()['data']['email_users']:
        return ""
    return resp.json()['data']['email_users'][email][0]['open_id']

def get_feishu_app_tenant_access_token():
    app_id = "cli_a14f2e9ffabcd00b"
    app_secret = "0WjTDFUUiO55Quo5fkCuJLnCGhiFBjSU"
    header = {"content-type": "application/json; charset=utf-8"}
    tenant_access_token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = json.dumps({"app_id": app_id, "app_secret": app_secret})
    resp = requests.post(tenant_access_token_url, headers=header, proxies=PROXY_HTTPS, data=payload)
    return resp.json()['tenant_access_token']
   

#获取以前的日期
def get_ds(d):
    return datetime.strftime(date.today() + timedelta(days = -d),'%Y-%m-%d')

#获取hdfs文件大小
def get_table_length(ip,port,path,dsdate):
    url="http://{}:{}/webhdfs/v1{}/{}?user.name=hive&op=GETCONTENTSUMMARY".format(ip,port,path,dsdate)
    #params={'user.name': 'hive','op': 'GETCONTENTSUMMARY'}
    #payload = json.dumps(params)
    headers = {'content-type': 'application/json'}
    response = requests.get(url=url, headers=headers)
    res=response.json()
    try:
        ilength=res['ContentSummary']['length']
        #print(path,dsdate,"存在")
    except:
    	#print(path ,"不存在")
    	ilength=1
    return ilength

#获取当前namenode
def getNameNode():
    for namenode in NAMENODES:
        url = 'http://{}:{}/webhdfs/v1/warehouse/zy_dm.db?user.name=hive&op=LISTSTATUS'.format(namenode, 50070)
        response = requests.get(url=url, headers=headers)
        namenode_res = response.json()
        if "RemoteException" in namenode_res:
            continue
        else:
            return namenode
    return namenode

def send_to_feishu1(send_params):
    print('群告警')

def send_to_feishu(send_params):
    print('群告警')
    '''正式'''
    send_url="https://open.feishu.cn/open-apis/bot/v2/hook/afb4f2a4-d9c4-4a0a-84aa-945f5399dc1e"
    #send_url="https://open.feishu.cn/open-apis/bot/v2/hook/1144aebd-8bd7-45d9-b620-015de2c1ba5d"
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    send_payload = json.dumps(send_params)
    send_headers = {'content-type': 'application/json'}
    proxy =  { 'https' : "192.168.7.24:44551"}
    response = requests.post(url=send_url,proxies=proxy,headers=send_headers,data=send_payload)

def send_to_feishu_dj(send_params):
    print('群告警')
    '''正式'''
    send_url="https://open.feishu.cn/open-apis/bot/v2/hook/21075535-b869-455e-ab8c-dd2fb0c6fd27"
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    send_payload = json.dumps(send_params)
    send_headers = {'content-type': 'application/json'}
    proxy =  { 'https' : "192.168.7.24:44551"}
    response = requests.post(url=send_url,proxies=proxy,headers=send_headers,data=send_payload)


def send_to_phone(phone):
    print('电话告警')
    send_url="https://www.linkedsee.com/alarm/channel/"
    send_params={"receiver":phone,"type": "phone","title": "数据工程P0","content": "宝贝，来自小哥哥的关心，你有没有想好吃啥？","genre": "play"}
    send_payload = json.dumps(send_params)
    send_headers = {'content-type': 'application/json','Servicetoken': 'b08e142c62bdc73680b6297881b99be3'}
    proxy =  { 'https' : "192.168.7.24:44551"}
    response = requests.post(url=send_url,proxies=proxy,headers=send_headers,data=send_payload)

def send_to_error(table_name,partition,file_length):
    #pd.read_sql("insert into sla_error_table (table_name,ds) values ('{}','{}');".format(table_name,err_ds),conn_mysql)
    with conn_mysql_tables.cursor() as cursor:
        sql = '''insert into sla_error (table_name,partition,file_length) values ('{}','{}','{}');'''.format(table_name,partition,file_length)
        cursor.execute(sql)
    conn_mysql_tables.commit()

def get_workflow(project,workflow):
   fsql_mysql_workflows='''select name,state,run_times,flag,start_time t_ds_process_instance from t_ds_process_instance where project_id=(select id from t_ds_project where name='{}' limit 1) and substring_index(name,'-',1)='{}' order by start_time desc limit 1'''.format(project,workflow)
   df_mysql_workflows=pd.read_sql(fsql_mysql_workflows,conn_mysql_workflows)
   if(len(df_mysql_workflows)==0):
       return '6'
   else:
       state=df_mysql_workflows.iloc[0]['state']
       return str(state)

if __name__ == '__main__':
    #yes=getYesterday(2);
    #print(yes)
    send_to_phone('15939807257');
