from chinese_calendar import is_workday, is_holiday
import chinese_calendar as calendar
import time, datetime
import random
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

def is_weekday(date):
        '''
        判断是否为工作日
        '''
        Y = date.year
        M = date.month
        D = date.day
        april_last = datetime.date(Y, M, D)
        return is_workday(april_last)
 
def is_holidays(date):
        '''
        判断是否为节假日
        '''
        Y = date.year
        M = date.month
        D = date.day
        april_last = datetime.date(Y, M, D)
        return is_holiday(april_last)
 
def is_festival(date):
    """
    判断是否为节日
    注意：有些时间属于相关节日的调休日也会显示出节日名称，可参考源码https://pypi.org/project/chinesecalendar/
    """
    Y = date.year
    M = date.month
    D = date.day
    april_last = datetime.date(Y, M, D)
    on_holiday, holiday_name = calendar.get_holiday_detail(april_last)
    return on_holiday 
 
def send_to_feishu(send_params):
    print('群告警')
    '''正式'''
    send_url="https://open.feishu.cn/open-apis/bot/v2/hook/0e4ac30e-1af1-4e81-a773-7321d600745d"
    #send_url="https://open.feishu.cn/open-apis/bot/v2/hook/1144aebd-8bd7-45d9-b620-015de2c1ba5d"
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    send_payload = json.dumps(send_params)
    send_headers = {'content-type': 'application/json'}
    proxy =  { 'https' : "192.168.7.24:44551"}
    response = requests.post(url=send_url,proxies=proxy,headers=send_headers,data=send_payload)
 
if __name__ == "__main__":
    today = datetime.datetime.now()
    week = is_weekday(today)
    holiday = is_holidays(today)
    festival = is_festival(today)
    
    person=["夏宝宁","韦元勇","胡正嵩","李志伟","朱斌","罗祖兵","肖东方","郭金","王国庆","李辉","何毛宁","王伟","李鹏","刘科志","雷中原","徐梁攀","李国江","冯明宇"]
    f1=["犯了花痴","昨夜偷牛","三点看球","偷偷想谁","bug太多","在上厕所","上班摸鱼","打球上瘾","上班睡觉","十分嚣张","家里有矿","提前溜走","手机没电","为人低调","地铁看美女","挖到金矿","中了彩票","换了对象","giao了个giao","急于上分","下楼抽烟"]
    f2=["卡也不要了。","没想起来打卡。","连打卡也忘了。","已卸载打卡功能。","没有打卡。","忘记打卡。","就是不打卡。","看着打卡功能，邪魅一笑。","过于激动，打卡失败。"]
    f3=["请您火速补卡。","请您尽快打卡。","请您回公司一百米打卡。","请您吃饱饭后进行打卡。","请您下次记得打卡。","请您珍惜工资，尽快打卡。","请您小心一点，轻触打卡。","请您低调工作，高调打卡。","请您打卡并写份检查。","请您掏出手机触摸打卡。","请您回到工位赶快打卡。","请您放下水杯，把卡打了。"]

    send_params={"msg_type":"interactive","card":{"config":{"wide_screen_mode":True},"header":{"template":"green","title":{"content":"打卡提醒：","tag":"plain_text"}},"elements":[{"fields":[{"is_short":False,"text":{"content":"** 时间**\n2021-02-23 20:17:51","tag":"lark_md"}}],"tag":"div"}]}}

    
    if(is_workday(today)):
        result='时间已到，能打卡请打卡，不能打卡请及时补卡。\n目测 '+person[random.randint(0,len(person)-1)]+' 同学:'+f1[random.randint(0,len(f1)-1)]+','+f2[random.randint(0,len(f2)-1)]+f3[random.randint(0,len(f3)-1)]+'<at id="all"></at>'
        send_params['card']['elements'][0]['fields'][0]['text']['content']=""+result
         
        send_to_feishu(send_params)
