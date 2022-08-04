#!/data/server/python3.7/bin/python3
# !-*- coding:utf8 -*-
"""
  Copyright (c) 2020,掌阅科技
  All rights reserved.
  摘    要：
  创 建 者：lijun
  创建日期：2021-12-25_20:55:05
  项目：  数据工程高优先级任务未就绪 超时监控
"""

import base64
import datetime
import hashlib
import hmac
import json
import sys
import time

import pandas as pd
import pymysql
import requests

FEISHU_TEST_ROBOT_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/ac6607e7-9e17-44ec-af2d-25bd89728843"
FEISHU_TEST_ROBOT_KEY = "MQhhlk05pQWNBEqNKiuRob"
FEISHU_P0_GROUP_ROBOT_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/4f4077ff-b5bf-4ca6-ad83-dc9cfa49f994"
FEISHU_P0_GROUP_ROBOT_KEY = "gXChIXc28CzpSBNdkPmxed"
FEISHU_TECH_GROUP_ROBOT_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/698ec2b1-6251-47a5-b82e-317e5d69430f"
FEISHU_TECH_GROUP_ROBOT_KEY = "F3DKj8ITs1FKLWKkLbjPib"
FEISHU_BMH_GROUP_ROBOT_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/49ee9776-5dd7-4fa2-9490-13deb1999db1"
FEISHU_BMH_GROUP_ROBOT_KEY = "ghof8HEn9ItrCgX1AGg4hd"
FEISHU_AB_GROUP_ROBOT_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/8e495126-f273-4767-bcee-1ee7f796cfc8"
FEISHU_AB_GROUP_ROBOT_KEY = "MupfPJ1PE0dn9Vh4blICMg"
PROXY_HTTP = {'http': "192.168.7.24:44551"}
PROXY_HTTPS = {'https': "192.168.7.24:44551"}
NAMENODES = ['10.100.105.12', '10.100.105.13']

# 小时级查询3小时以前的
# 参数：
# 1：查询类型day/hour
# 2：查询条件priority的值
# 3：发送群组prod：技术大群，p0：p0小群，BMH：BMH小群，AB：AB小群
# 4：定时任务，0：不定时，6：6点执行任务，7：7点执行任务
BEFORE_HOUR = 3
if sys.argv[3] == "prod":
    FEISHU_ROBOT_URL = FEISHU_TECH_GROUP_ROBOT_URL
    FEISHU_ROBOT_KEY = FEISHU_TECH_GROUP_ROBOT_KEY
    MYSQL_PASS = "ops-cm"
elif sys.argv[3] == "p0":
    FEISHU_ROBOT_URL = FEISHU_P0_GROUP_ROBOT_URL
    FEISHU_ROBOT_KEY = FEISHU_P0_GROUP_ROBOT_KEY
    MYSQL_PASS = "ops-cm"
elif sys.argv[3] == "BMH":
    FEISHU_ROBOT_URL = FEISHU_BMH_GROUP_ROBOT_URL
    FEISHU_ROBOT_KEY = FEISHU_BMH_GROUP_ROBOT_KEY
    MYSQL_PASS = "ops-cm"
elif sys.argv[3] == "AB":
    FEISHU_ROBOT_URL = FEISHU_AB_GROUP_ROBOT_URL
    FEISHU_ROBOT_KEY = FEISHU_AB_GROUP_ROBOT_KEY
    MYSQL_PASS = "ops-cm"
else:
    FEISHU_ROBOT_URL = FEISHU_TEST_ROBOT_URL
    FEISHU_ROBOT_KEY = FEISHU_TEST_ROBOT_KEY
    MYSQL_PASS = "123456"


class PeriodType:
    DAY = 1
    HOUR = 2


def getYesterday(days):
    today = datetime.date.today()
    oneday = datetime.timedelta(days=int(days))
    yesterday = today - oneday
    return yesterday


def getBeforeHours(hours=0):
    before_seconds = hours * 3600
    ds, hour = time.strftime("%Y-%m-%d %H", time.localtime(int(time.time()) - before_seconds)).split(" ")
    return ds, hour


def get_table_length(ip, port, path, dsdate, hour=''):
    if hour:
        url = "http://{}:{}/webhdfs/v1{}/{}/hour={}?user.name=hive&op=GETCONTENTSUMMARY".format(ip, port, path, dsdate,
                                                                                                hour)
    else:
        url = "http://{}:{}/webhdfs/v1{}/{}?user.name=hive&op=GETCONTENTSUMMARY".format(ip, port, path, dsdate)
    # params={'user.name': 'hive','op': 'GETCONTENTSUMMARY'}
    # payload = json.dumps(params)
    headers = {'content-type': 'application/json'}
    response = requests.get(url=url, headers=headers)
    res = response.json()
    print(res)
    try:
        ilength = res['ContentSummary']['length']
        print(path, dsdate, "存在")
    except:
        print(path, "不存在")
        ilength = 0
    return ilength


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


def gen_sign(timestamp, secret):
    # 拼接timestamp和secret
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    hmac_code = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    # 对结果进行base64处理
    sign = base64.b64encode(hmac_code).decode('utf-8')
    return sign


def send_to_feishu_webhook(msg_dict, period_type):
    send_headers = {'content-type': 'application/json'}
    if period_type == PeriodType.DAY:
        msg_content = "***数据延迟通告：天级***\n\n"
        # msg_content += "test: {}\n".format('<at user_id="{}">Name</at>'.format('ou_f065772164c8c37e5d12f3618e220c93'))
    elif period_type == PeriodType.HOUR:
        msg_content = "***数据延迟通告：小时级***\n\n"
    msg_content += "成功表数: {}\n".format(len(msg_dict["success"]))
    # 不报告成功表名
    # for df in dfs:
    #     msg_content += "**表名:** {} \n".format(df['data_table'])
    msg_content += "未就绪表数: {}\n\n".format(len(msg_dict["fail"]))
    # msg_content += "表时间：{}\n".format(time_str)
    if len(msg_dict["fail"]) != 0:
        for df in msg_dict["fail"]:
            if df['type'] == PeriodType.DAY:
                time_str = getYesterday(df['ds_days'])
            elif df['type'] == PeriodType.HOUR:
                ds, hour = getBeforeHours(BEFORE_HOUR)
                time_str = ds + ", hour:" + hour
            open_id = ""
            if df['email']:
                open_id = get_feishu_openid(df['email'])
            msg_content += "表时间：{}\n**平台:** {} \n  **一级菜单:** {} \n **二级菜单:** {} \n **未就绪表:** {} \n **负责人:** {}\n\n\n".format(
                time_str, df['platform'], df['first_level_menu'], df['Secondary_level_menu'], df['data_table'],
                '<at user_id="{}">Name</at>'.format(open_id))
    now_ts = str(int(time.time()))
    feishu_sign = gen_sign(now_ts, FEISHU_ROBOT_KEY)
    send_params = {"msg_type": "text",
                   "content": {"text": msg_content},
                   "timestamp": now_ts,
                   "sign": feishu_sign,
                   }
    send_payload = json.dumps(send_params)
    requests.post(url=FEISHU_ROBOT_URL, proxies=PROXY_HTTPS, headers=send_headers, data=send_payload)

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

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        arg_period = sys.argv[1]
        if arg_period == "day":
            period_type = PeriodType.DAY
            # time_str = getYesterday()
        elif arg_period == "hour":
            period_type = PeriodType.HOUR
            ds, hour = getBeforeHours(BEFORE_HOUR)
            # time_str = ds + ", hour:" + hour
        else:
            exit()
        try:
            query_time = sys.argv[4]
            priority = sys.argv[2]
        except:
            query_time = 0
    else:
        exit()
    conn = pymysql.connect(host='127.0.0.1', db='dp', user='root', passwd=MYSQL_PASS, port=int(3306))
    # 默认查询时间参数为0，否则使用参数时间
    fsql = '''SELECT * from bi_alert WHERE type={} and querytime={} and priority='{}';'''.format(period_type, int(query_time), priority)
    df = pd.read_sql(fsql, conn)
    fail_list = []
    success_list = []
    send_msg_dict = {}
    namenode_enable = getNameNode()
    if len(df) == 0:
        exit(1)
    for i in range(len(df)):
        print(i)
        # print(df.iloc[i]['data_table'])
        path = df.iloc[i]['data_table']
        if path:
            if period_type == PeriodType.DAY:
                idate = getYesterday(df.iloc[i]['ds_days'])
                dsdate = '{}={}'.format(df.iloc[i]['ds'], idate)
                # print(path,idate)
                lenght = get_table_length(namenode_enable, 50070, path, dsdate)
            elif period_type == PeriodType.HOUR:
                # 获取两个小时之前的日期和小时
                ds, hour = getBeforeHours(BEFORE_HOUR)
                dsdate = 'ds={}'.format(ds)
                lenght = get_table_length(namenode_enable, 50070, path, dsdate, hour)
            if lenght <= 40:
                fail_list.append(df.iloc[i])
            else:
                success_list.append(df.iloc[i])

    send_msg_dict["fail"] = fail_list
    send_msg_dict["success"] = success_list
    send_to_feishu_webhook(send_msg_dict, period_type)
