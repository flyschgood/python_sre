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

def send_to_error(table_name,partition,file_length):
    #pd.read_sql("insert into sla_error_table (table_name,ds) values ('{}','{}','{}');".format(table_name,err_ds),conn_mysql)
    with conn_mysql_tables.cursor() as cursor:
        sql = '''insert into sla_error (table_name,partition,file_length) values ('{}','{}','{}');'''.format(table_name,partition,file_length)
        cursor.execute(sql)
    conn_mysql_tables.commit()

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
    #now = datetime.datetime.now()
    #ihour=(datetime.datetime.now()-datetime.timedelta(hours=8)).strftime("%H")
    #sid='ou_6ea0e8fae5b8b1afb71ccbc0e7d9d833'
     #查找值班人
    conn_mysql_zhiban=pymysql.connect(host='10.100.94.71', db='dp', user='cm', passwd='cm-ops', port=int(3306))

    #郭金值班的日期
    zhiban_date=datetime.strptime('2022-06-23 00:00:00', "%Y-%m-%d %H:%M:%S")
    fsql_mysql_zhiban='''select name,email,phone from sla_user where id={};'''.format((datetime.now()-zhiban_date).days%9)
    print(fsql_mysql_zhiban)
    df_mysql_user=pd.read_sql(fsql_mysql_zhiban, conn_mysql_zhiban)
    
    zhiban_name = df_mysql_user.iloc[0]['name'] 
    zhiban_email = df_mysql_user.iloc[0]['email']
    zhiban_phone = df_mysql_user.iloc[0]['phone']
    zhiban_feishu_id = get_feishu_openid(zhiban_email)

    #send_params={"aaa":"666"}
    send_params={"msg_type":"interactive","card":{"config":{"wide_screen_mode":True},"header":{"template":"blue","title":{"content":"数据工程-小时级数据延迟","tag":"plain_text"}},"elements":[{"fields":[{"is_short":True,"text":{"content":"** 时间**\n2021-02-23 20:17:51","tag":"lark_md"}},{"is_short":True,"text":{"content":"** 表类型**\np0结果表","tag":"lark_md"}},{"is_short":False,"text":{"content":"** 延迟表**\nidata_dws.t_dws_midu_cps_summary_data_d","tag":"lark_md"}},{"is_short":True,"text":{"content":"** 延迟分区**\nds=2022-06-13/hour=13","tag":"lark_md"}},{"is_short":True,"text":{"content":"** 项目**\ndw_idata","tag":"lark_md"}},{"is_short":False,"text":{"content":"** 工作流**\nt_dws_midu_cps_summary_data_d","tag":"lark_md"}},{"is_short":False,"text":{"content":"** 任务节点**\nt_dws_midu_cps_summary_data_d","tag":"lark_md"}},{"is_short":True,"text":{"content":"** 表责任人：**\n<at id=ou_833c66c11857a89b0b6351bd9d58b519>曹畅</at>","tag":"lark_md"}},{"is_short":True,"text":{"content":"** 当日值班：**\n<at id=ou_833c66c11857a89b0b6351bd9d58b519>肖东方</at>","tag":"lark_md"}},{"is_short":False,"text":{"content":"** 影响范围**\np0基线","tag":"lark_md"}},{"is_short":False,"text":{"content":"** 文件大小**\n0b","tag":"lark_md"}}],"tag":"div"}]}}

    #conn_hive=hive.Connection(host="10.100.105.249", port=10010,username="hive",database="zy_dw",auth="NOSASL")
    #查工作流的信息
    conn_mysql_workflows=pymysql.connect(host='10.100.102.139', db='zy_scheduler', user='zy_scheduler', passwd='uwAL4w1JQxDVrnszfHA', port=int(3306))
    #监控表的信息列表
    conn_mysql_tables=pymysql.connect(host='10.100.94.71', db='dp', user='cm', passwd='cm-ops', port=int(3306))
    #ihour="07"
    ihour=datetime.now().strftime('%H')
    print('当前时间:'+time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    #fsql_hive='''SELECT table_name from zy_dw.dwd_table_output_monitor_di limit 1'''
    fsql_mysql_tables='''SELECT table_name,table_path,err_ds,err_hour,table_info,person_in_charge,project,workflow,task,table_type,flag from sla_table where update_type='h' and table_type like '%高优%';'''
    df_mysql_tables=pd.read_sql(fsql_mysql_tables, conn_mysql_tables)
    #定义basic相关变量
    basic_all=0
    basic_no_partition=0
    basic_list=[]
    basic_list_str='' 
    basic_flag='异常'
    jiankong_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    for i in range(len(df_mysql_tables)):
        table_name=str(df_mysql_tables.iloc[i]['table_name'])
        table_path=str(df_mysql_tables.iloc[i]['table_path'])
        err_ds=str(df_mysql_tables.iloc[i]['err_ds'])
        err_hour=str(df_mysql_tables.iloc[i]['err_hour'])
        table_info=str(df_mysql_tables.iloc[i]['table_info'])
        person_in_charge=str(df_mysql_tables.iloc[i]['person_in_charge'])
        project=str(df_mysql_tables.iloc[i]['project'])
        workflow=str(df_mysql_tables.iloc[i]['workflow'])
        task=str(df_mysql_tables.iloc[i]['task'])
        table_type=str(df_mysql_tables.iloc[i]['table_type'])
        flag=str(df_mysql_tables.iloc[i]['flag'])
        jiankong_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        #循环获取表对应hdfs目录大小
        #partition="ds="+get_ds(int(err_ds))+"/hour="+get_hour(int(err_hour))
        if(not operator.contains(err_ds,'=') and not operator.contains(err_hour,'=')):
           partition="ds="+get_ds(int(err_ds))+"/"+"hour="+get_hour(int(err_hour))
        elif(not operator.contains(err_ds,'=') and operator.contains(err_hour,'=')):
           partition="ds="+get_ds(int(err_ds))+"/"+err_hour.split('=')[0]+"="+get_hour(int(err_hour.split('=')[1]))
        elif(operator.contains(err_ds,'=') and not operator.contains(err_hour,'=')):
           partition=err_ds.split('=')[0]+"="+get_ds(int(err_ds.split('=')[1]))+"/"+"hour="+get_hour(int(err_hour))
        elif(operator.contains(err_ds,'=') and operator.contains(err_hour,'=')):
           partition=err_ds.split('=')[0]+"="+get_ds(int(err_ds.split('=')[1]))+"/"+err_hour.split('=')[0]+"="+get_hour(int(err_hour.split('=')[1]))
        else:
           print("ERROR:分区处理错误"+table_name)
        file_length=get_table_length(namenode,'50070',table_path,partition)
        print(table_name+"\t"+str(file_length))
        send_params['card']['elements'][0]['fields'][0]['text']['content']='**时间**\n'+jiankong_time
        send_params['card']['elements'][0]['fields'][1]['text']['content']='**表类型**\n'+table_type
        send_params['card']['elements'][0]['fields'][2]['text']['content']='**延迟表**\n'+table_name
        send_params['card']['elements'][0]['fields'][3]['text']['content']='**延迟分区**\n'+partition
        send_params['card']['elements'][0]['fields'][4]['text']['content']='**项目**\n'+project
        send_params['card']['elements'][0]['fields'][5]['text']['content']='**工作流**\n'+workflow
        send_params['card']['elements'][0]['fields'][6]['text']['content']='**任务节点**\n'+task
        send_params['card']['elements'][0]['fields'][7]['text']['content']='**表责任人**\n'+person_in_charge
        send_params['card']['elements'][0]['fields'][8]['text']['content']='**当日值班**\n'+'<at id="{}">{}</at>'.format(zhiban_feishu_id,zhiban_name)
        send_params['card']['elements'][0]['fields'][9]['text']['content']='**影响范围**\n'+table_info
        #如果是basic要先确定是否刷了元数据，即有这个分区
        if(table_name.startswith("basicdata.")):
            basic_all+=1
            is_partition_exist=check_partition_exist(table_name,get_ds(int(err_ds)),get_hour(int(err_hour)))
            #如果扫描分区出错，返回1,说明没这个分区了
            #如果记录当前无分区的个数，发到告警群
            if(is_partition_exist=='1'):
                basic_no_partition+=1
                basic_list.append(table_name)
            #continue

        if(file_length<100):
           if(flag=='0'):
               print('flag=0,table_name:'+table_name)
               #send_to_feishu(send_params)
               if(operator.contains(table_type,'得间')):
                   send_to_feishu_dj(send_params)
               send_to_error(table_name,partition,file_length)
           elif(flag=='1'):
               print('flag=1,table_name:'+table_name)
               #send_to_feishu(send_params)
               send_to_phone(zhiban_phone)
               if(operator.contains(table_type,'得间')):
                   send_to_feishu_dj(send_params)
               send_to_error(table_name,partition,file_length)
           elif(flag.startswith('2_')):
               workflow_status=get_workflow_hour(project,workflow,err_hour)
               if(workflow_status=='7'):
                   print('工作流正常运行!')
               else:
                   print('工作流异常:'+workflow_status)
                   if(flag=='2_0'):
                       #send_to_feishu(send_params)
                       if(operator.contains(table_type,'得间')):
                           send_to_feishu_dj(send_params)
                       print('flag=2_,table_name:'+table_name)
                   elif(flag=='2_1'):
                       #send_to_feishu(send_params)
                       if(operator.contains(table_type,'得间')):
                           send_to_feishu_dj(send_params)
                       send_to_phone(zhiban_phone)
                       print('flag=2_,table_name:'+table_name)
                   else:
                       #send_to_feishu(send_params)
                       if(operator.contains(table_type,'得间')):
                           send_to_feishu_dj(send_params)
                       send_to_phone(zhiban_phone)
                       print('触发了2_0和2_1的意外情况')
                   send_to_error(table_name,partition,file_length)
                   
           else:
               #send_to_feishu(send_params);
               if(operator.contains(table_type,'得间')):
                   send_to_feishu_dj(send_params)
               send_to_phone(zhiban_phone)
               send_to_error(table_name,partition,file_length)
               print('触发了除了0,1,2_的ERROR情况')
    for l in basic_list:
           basic_list_str+=l+'\n'

    send_params_basic={"msg_type":"interactive","card":{"config":{"wide_screen_mode":True},"header":{"template":"green","title":{"content":"数据工程-basic落表统计","tag":"plain_text"}},"elements":[{"fields":[{"is_short":True,"text":{"content":"** 时间**\n2021-02-23 20:17:51","tag":"lark_md"}},{"is_short":True,"text":{"content":"** basic总数**\np0结果表","tag":"lark_md"}},{"is_short":True,"text":{"content":"** 未落表数**\n2021-02-23 20:17:51","tag":"lark_md"}},{"is_short":True,"text":{"content":"** 状态:**\n2021-02-23 20:17:51","tag":"lark_md"}},{"is_short":False,"text":{"content":"** 未落表basic:**\n2021-02-23 20:17:51","tag":"lark_md"}}],"tag":"div"}]}}
    if(basic_all-basic_no_partition>=43):
        basic_flag='正常'
        send_params_basic['card']['elements'][0]['fields'][1]['text']['content']='**basic层状态**\n'+basic_flag
    else:
        send_params_basic['card']['elements'][0]['fields'][1]['text']['content']='**basic层状态**\n'+basic_flag+'<at id="{}">{}</at>'.format(zhiban_feishu_id,zhiban_name)
        #send_to_phone(zhiban_phone)

    send_params_basic['card']['elements'][0]['fields'][0]['text']['content']='**时间**\n'+jiankong_time
    send_params_basic['card']['elements'][0]['fields'][2]['text']['content']='**basic总数**\n'+str(basic_all)
    send_params_basic['card']['elements'][0]['fields'][3]['text']['content']='**未落表数**\n'+str(basic_no_partition)
    send_params_basic['card']['elements'][0]['fields'][4]['text']['content']='**未落表basic:**\n'+basic_list_str

    #群里报basic统计
    #send_to_feishu(send_params_basic)

 except Exception as e:
     print("main方法执行报错")
     traceback.print_exc()
