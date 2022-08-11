#!/data/server/python3/bin/python3
# !-*- coding:utf8 -*-

from pyhive import hive
from pyhive.exc import OperationalError


def rename_table(db_table):
    db_table_drop = db_table + '_delete'
    # alter table zy_dm.dm_pub_active_user_di rename to zy_dm.dm_pub_active_user_di_drop
    hql = 'alter table ' + db_table + ' rename to ' + db_table_drop
    hql = hql.replace("\n", "")
    return hql


# 一、建立hive连接
conn = hive.Connection(host='10.100.105.249', port=10010, username='hive', database='default')

# 二、获取cursor对象
# 我们要使用连接对象获得一个cursor对象,接下来,我们会使用cursor提供的方法来进行工作,这些方法包括两大类:1.执行命令,2.接收返回值
# 1、cursor用来执行命令的方法:
# 　    callproc(self, procname, args):用来执行存储过程,接收的参数为存储过程名和参数列表,返回值为受影响的行数
# 　    execute(self, query, args):执行单条sql语句,接收的参数为sql语句本身和使用的参数列表,返回值为受影响的行数
# 　    executemany(self, query, args):执行单挑sql语句,但是重复执行参数列表里的参数,返回值为受影响的行数
# 　    nextset(self):移动到下一个结果集
#
# 2、cursor用来接收返回值的方法:
# 　    fetchall(self):接收全部的返回结果行.
# 　    fetchmany(self, size=None):接收size条返回结果行.如果size的值大于返回的结果行的数量,则会返回cursor.arraysize条数据.
# 　    fetchone(self):返回一条结果行.
# 　    scroll(self, value, mode='relative'):移动指针到某一行.如果mode='relative',则表示从当前所在行移动value条,如果mode='absolute',则表示从结果集的第一 行移动value条.
cursor = conn.cursor()

# 三、读取配置文件中的hive表的表名进行rename
successful_rows = 0
unsuccessful_rows = 0
fs = open("./rename_table.txt", "r")
successful_fs = open("./hive_rename_successful.txt", "a")
unsuccessful_fs = open("./hive_rename_unsuccessful.txt", "a")
try:
    for line in fs:
        hql = rename_table(line)
        try:
            # 返回值为受影响的行数
            cursor.execute(hql)
            print("rename table:" + line)
        except Exception as e:
            # print("失败原因e = " + str(e))
            unsuccessful_fs.write(line)
            unsuccessful_rows = unsuccessful_rows + 1
            continue
        successful_fs.write(line)
        successful_rows = successful_rows + 1
finally:
    print("成功的总行数：" + str(successful_rows))
    print("失败的总行数：" + str(unsuccessful_rows))
    cursor.close()
    conn.close()
    fs.close()
    successful_fs.close()
    unsuccessful_fs.close()

# 执行语句(102.20下运行)：
# /data/server/python3/bin/python3 ./hdfs_warehouse_modifytime.py

# 安装python模块的路径：cd /data/server/python3/lib/python3.6/site-packages/
# 安装python模块的命令：/data/server/python3/bin/pip3 模块名

# tee作用：将print的内容写入指定文件，同时也会打印到控制台
# /data/server/python3/bin/python3 ./hdfs_warehouse_modifytime.py | tee ./warehouse_mofification_time.txt
