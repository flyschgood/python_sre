#!/data/server/python3/bin/python3
# !-*- coding:utf8 -*-

from pyhdfs import HdfsClient
from datetime import datetime

NAMENODES = ["10.100.105.12:50070", "10.100.105.13:50070"]
HDFS_USER_NAME = "hdfs"

PROXIES = {"http": "http://192.168.7.24:44551", "https": "http://192.168.7.24:44551"}

client = HdfsClient(hosts=NAMENODES, user_name=HDFS_USER_NAME)


def get_write_fs(file_name):
    fs = open(file_name, 'w', encoding='utf-8')
    return fs


def warehouse_monitor(fs, compute_time):
    for file_status in client.list_status('/warehouse'):
        name = file_status.pathSuffix

        for sub_file_status in client.list_status('/warehouse' + '/' + name):
            if sub_file_status.type != "DIRECTORY":
                continue
            sub_name = sub_file_status.pathSuffix
            if sub_name.startswith('.'):
                continue

            # 目录地址
            data_path = '/warehouse' + '/' + name + '/' + sub_name
            print("data_path = " + data_path)

            # 目录大小
            directory_size = client.get_content_summary(data_path)['length']
            print("directory_size = " + str(directory_size))

            # 表名
            if (not name.endswith('.db')) or sub_name.startswith('_') or sub_name.__contains__('.'):
                table_name = '非标准路径'
            else:
                db = name.split('.')[0]
                print("db = " + db)
                table = sub_name
                print("table = " + table)
                table_name = db + "." + table
                print("table_name = " + table_name)

            # 最后修改时间(时间戳)
            modification_time = client.get_file_status(data_path)['modificationTime']
            print("modification_time = " + str(modification_time))

            # 最后修改时间(%Y-%m-%d %H:%M:%S)
            modification_time_fmt = datetime.fromtimestamp(modification_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
            print("modification_time_fmt = " + modification_time_fmt)

            # 结果
            line = ','.join([data_path, str(directory_size), table_name, str(modification_time), modification_time_fmt,
                             compute_time]) + '\n'
            fs.write(line)


def get_basic_modifytime(pathsubffix):
    match = False
    sub_path_list = []
    for file_status in client.list_status(pathsubffix):
        if file_status.type != "DIRECTORY":
            continue
        name = file_status.pathSuffix
        if '.hive-staging_hive_' in name or '_impala_insert_staging' in name or pathsubffix + "/" + name == '/basicdata/kafka/ds=2022-03-02':
            continue
        if '=' in name:
            match = True
        sub_path_list.append(pathsubffix + "/" + name)

    if match:
        # 目录地址
        data_path = pathsubffix
        print("data_path = " + data_path)

        # 目录大小
        directory_size = client.get_content_summary(data_path)['length']
        print("directory_size = " + str(directory_size))

        # 最后修改时间(时间戳)
        file_status = client.get_file_status(pathsubffix)
        modification_time = file_status['modificationTime']
        print("modification_time = " + str(modification_time))

        # 最后修改时间(%Y-%m-%d %H:%M:%S)
        modification_time_fmt = datetime.fromtimestamp(modification_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
        print("modification_time_fmt = " + modification_time_fmt)

        yield data_path, str(directory_size), str(modification_time), modification_time_fmt, compute_time
    else:
        for sub_path in sub_path_list:
            yield from get_basic_modifytime(sub_path)


def basicdata_monitor(fs, compute_time):
    for data_path, directory_size, modification_time, modification_time_fmt, compute_time in get_basic_modifytime(
            '/basicdata'):
        # 结果
        line = ','.join(
            [data_path, directory_size, '非标准路径', modification_time, modification_time_fmt, compute_time]) + '\n'
        fs.write(line)


if __name__ == '__main__':
    # 统计时间
    compute_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        # 获取basicdata下目录
        fs = get_write_fs('./warehouse_mofification_time.txt')
        basicdata_monitor(fs, compute_time)
        # fs.close()

        # 获取warehouse下目录
        # fs = get_write_fs('./warehouse_mofification_time.txt')
        warehouse_monitor(fs, compute_time)
        fs.close()
    except Exception:
        print("发生异常")

# 执行语句(102.20下运行)：
# /data/server/python3/bin/python3 ./hdfs_warehouse_modifytime.py

# 安装python模块的路径：cd /data/server/python3/lib/python3.6/site-packages/
# 安装python模块的命令：/data/server/python3/bin/pip3 模块名

# tee作用：将print的内容写入指定文件，同时也会打印到控制台
# /data/server/python3/bin/python3 ./hdfs_warehouse_modifytime.py | tee ./warehouse_mofification_time.txt
