#!/usr/bin/python3
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
from typing import List
import socket
import pymysql
import traceback
import time
#import datetime
import operator
import pandas as pd
from impala.dbapi import connect
from pyhive import hive
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
   
#几小时以前的日期
def get_ds(h):
    return datetime.strftime(datetime.now() + timedelta(hours = -h),'%Y-%m-%d')

#获取几小时前的小时
def get_hour(h):
    return datetime.strftime(datetime.now() + timedelta(hours = -h),'%H')

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

def send_to_feishu1(sid,iname,send_params):
    print(str(send_params)+'群告警')

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
    send_params={"receiver":phone,"type": "phone","title": "数据工程P0","content": "数据工程SLA告警","genre": "play"}
    send_payload = json.dumps(send_params)
    send_headers = {'content-type': 'application/json','Servicetoken': 'b08e142c62bdc73680b6297881b99be3'}
    proxy =  { 'https' : "192.168.7.24:44551"}
    response = requests.post(url=send_url,proxies=proxy,headers=send_headers,data=send_payload)

def delete_from_error(table_name,partition):
    with conn_mysql_error.cursor() as cursor:
        sql = '''delete from sla_error where table_name='{}' and partition='{}';'''.format(table_name,partition)
        cursor.execute(sql)
    conn_mysql_error.commit()

def get_workflow_hour(project,workflow,err_hour):
    fsql_mysql_workflows='''select name,state,run_times,flag,start_time from (select name,state,run_times,flag,start_time from t_ds_process_instance where project_id=(select id from t_ds_project where name='{}' limit 1) and substring_index(name,'-',1)='{}' order by start_time desc limit {}) t order by start_time limit 1;'''.format(project,workflow,err_hour)
    print(fsql_mysql_workflows)
    df_mysql_workflows=pd.read_sql(fsql_mysql_workflows,conn_mysql_workflows)
    print(fsql_mysql_workflows)
    if(len(df_mysql_workflows)==0):
        print('工作流查询为空')
        state='6'
    else:
        for i in range(len(df_mysql_workflows)):
            state=str(df_mysql_workflows.iloc[i]['state'])
            print(state)
            if(state!='7'):
                break
    return str(state)

def check_partition_exist(table_name,ds,hour):
    try:
        hive_basicdata_conn=connect(host='10.100.105.249', port=10010,auth_mechanism='PLAIN',user="hive",password="hive",database="basicdata") 
        basic_sql='''desc {} partition(ds='{}',hour='{}')'''.format(table_name,ds,hour)
        basic_df = pd.read_sql(basic_sql, hive_basicdata_conn) 
        #for i in range(len(basic_df)):
          #  if(str(basic_df.iloc[i]).startswith('Execution failed')):
           #     return '1'
        return '0'
    except Exception as result:
        print("check_partition_exist报错,table_name:"+table_name+ds+hour)
        return '1'

if __name__ == '__main__':
 try:
    headers = {'content-type': 'application/json'} 
    NAMENODES = ['10.100.105.12', '10.100.105.13']
    namenode=getNameNode()
    print("当前namenode:"+namenode)
    #查找未修复列表
    conn_mysql_zhiban=pymysql.connect(host='10.100.94.71', db='dp', user='cm', passwd='cm-ops', port=int(3306))
    conn_mysql_error=pymysql.connect(host='10.100.94.71', db='dp', user='cm', passwd='cm-ops', port=int(3306))

    #
    zhiban_date=datetime.strptime('2022-06-23 00:00:00', "%Y-%m-%d %H:%M:%S")
    fsql_mysql_zhiban='''select name,email,phone from sla_user where id={};'''.format((datetime.now()-zhiban_date).days%8)
    df_mysql_user=pd.read_sql(fsql_mysql_zhiban, conn_mysql_zhiban)
    
    zhiban_name = df_mysql_user.iloc[0]['name'] 
    zhiban_email = df_mysql_user.iloc[0]['email']
    zhiban_phone = df_mysql_user.iloc[0]['phone']
    zhiban_feishu_id = get_feishu_openid(zhiban_email)
    
    fsql_mysql_error='''select sla_error.table_name,sla_table.table_path,sla_table.person_in_charge,sla_error.partition,sla_error.file_length from sla_error left join sla_table on sla_error.table_name=sla_table.table_name'''

    send_params={"msg_type":"interactive","card":{"config":{"wide_screen_mode":True},"header":{"template":"yellow","title":{"content":"数据工程-仍未修复的表","tag":"plain_text"}},"elements":[{"fields":[{"is_short":True,"text":{"content":"** 时间**\n2021-02-23 20:17:51","tag":"lark_md"}},{"is_short":False,"text":{"content":"** 表类型**\np0结果表","tag":"lark_md"}},{"is_short":False,"text":{"content":"** 表类型**\np0结果表","tag":"lark_md"}}],"tag":"div"}]}}


    #conn_hive=hive.Connection(host="10.100.105.249", port=10010,username="hive",database="zy_dw",auth="NOSASL")
    #查工作流的信息
    print('当前时间:'+time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    #fsql_hive='''SELECT table_name from zy_dw.dwd_table_output_monitor_di limit 1'''
    df_mysql_error=pd.read_sql(fsql_mysql_error, conn_mysql_error)
    error_list=''

    for i in range(len(df_mysql_error)):
        table_name=str(df_mysql_error.iloc[i]['table_name'])
        table_path=str(df_mysql_error.iloc[i]['table_path'])
        partition=str(df_mysql_error.iloc[i]['partition'])
        old_file_length=str(df_mysql_error.iloc[i]['file_length'])
        person_in_charge=str(df_mysql_error.iloc[i]['person_in_charge'])
        jiankong_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        #获取文件大小
        file_length=str(get_table_length(namenode,'50070',table_path,partition))+'b'
        print(table_name+" "+str(file_length)+" "+str(old_file_length))
        #如果分区文件大小发生了变化，则认为已修复
        if(file_length!='1b' and old_file_length!=file_length):
            delete_from_error(table_name,partition)
        else:
            error_list+=table_name+'\t'+partition+'\n'
            
    send_params['card']['elements'][0]['fields'][0]['text']['content']='**时间:**'+jiankong_time
    send_params['card']['elements'][0]['fields'][1]['text']['content']='**   \n**'
    send_params['card']['elements'][0]['fields'][2]['text']['content']='**仍未修复的表:**\n'+error_list
    #报警到群
    send_to_feishu(send_params)

 except Exception as e:
     print("main方法执行报错")
     traceback.print_exc()
