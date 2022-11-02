#!/usr/bin/env python
# encoding: utf-8
"""
@CreateTime: 2022/07/07 09:41
@Author: lxc
@LastEditTime: 
@Desctiption:观测数据上报接口，1分钟上传一次
"""

import datetime
import time
import pymysql
import socket
import configparser
import requests


def conn_mysql():
    """连接MySQL数据库"""
    try:
        conn = pymysql.connect(host=local_db_ip, user=local_db_user, password=local_db_pw, database=local_db_dbName, port=local_db_port, autocommit=True)
        return conn
    except Exception as e:
        print(f"[{now_time}]-ERROR [conn_mysql] {e}")
        return False


def select_sql(sql):
    """查询MySQL中的数据"""
    try:
        conn = conn_mysql()
        if conn:
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            return results
    except Exception as e:
        print(f"[{now_time}]-ERROR [select_sql] {e}")
        return False


def update_sql(sql):
    """更新MySQL中的数据"""
    try:
        conn = conn_mysql()
        if conn:
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            return True
    except Exception as e:
        print(f"[{now_time}]-ERROR [update_sql] {e}")
        return False


def conn_socket(ip, port):
    """创建服务器套接字"""
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # 允许重用本地地址和端口
    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # 在客户端开启心跳维护
    tcp_sock.settimeout(10)  # 设置超时时间10s
    try:
        tcp_sock.connect((ip, port))
        # print(tcp_sock)
        # print("connect success!")
        return tcp_sock
    except Exception as e:
        print(f"[{now_time}]-ERROR [conn_socket] {ip}:{port} {e}")
        return False


def send_socket(data):
    """发送数据"""
    sock = conn_socket(ip, port)
    res_result = []
    if sock:
        if isinstance(data, list):
            for each in data:
                try:
                    sock.send(each.encode("utf-8"))     # 发送数据，将数据以字节流形式发出
                    # sock.send(each)     # 发送数据，将数据以字节流形式发出
                    time.sleep(1)
                    res_result.append(True)
                    print(f"[{now_time}]-SUCCESS [send_socket] \"{each}\"")
                except Exception as e:
                    print(f"[{now_time}]-ERROR [send_socket] \"{data}\" {e}")
                    res_result.append(False)
    sock.close()     # 关闭套接字
    return res_result



def post_data(data):
    """将数据上传到指定的接口"""
    res_result = []
    url = config.get("url", 'observeUrl')           # 观测数据上报接口需要请求的URL
    ranch_id = config.get("observePostData", 'ranchId')
    aquafarm_name = config.get("observePostData", 'aquafarmName')

    for i in data:
        # i = {'Date': '2022-08-30', 'Time': '13:48:54', 'Depth': 2.0, 'DO': 56.0, 'Temp': 3.5, 'Chl': 5.0, 'Sail': 2.0}
        i_date = i['Date']
        i_time = i['Time']
        monitor_time = f"{i_date} {i_time}"

        data = {
            "ranchId": ranch_id,
            "reportTime": i_date,
            "aquafarmName": aquafarm_name,
            "itemList": [
                {"key": "waterDepth", "value": i["Depth"], "monitorTime": monitor_time},
                {"key": "oxygen", "value": i["DO"], "monitorTime": monitor_time},
                {"key": "waterTemperature", "value": i["Temp"], "monitorTime": monitor_time},
                {"key": "chlorophyll", "value": i["Chl"], "monitorTime": monitor_time},
                {"key": "salinity", "value": i["Sail"], "monitorTime": monitor_time}
            ]
        }
        # print(data)

        try:
            ret = requests.post(url=url, json=data, verify=False)
            if ret.status_code == 200:
                print(now_time, ret.text, ret.status_code)
                res_result.append(True)
            else:
                print(now_time, ret.text, ret.status_code)
                res_result.append(False)
        except Exception as e:
            print(f"[{now_time}]-ERROR [post_data] {e},url={url}, data={data}")
            res_result.append(False)

    return res_result


def change_data_status(data):
    """更新已入网的数据的状态"""
    # data = {'Date': '2022-08-30', 'Time': '13:48:54', 'Depth': 2.0, 'DO': 56.0, 'Temp': 3.5, 'Chl': 5.0, 'Sail': 2.0}
    date = data["Date"]
    times = data["Time"]
    upload_status_sql = f"UPDATE `178wx` SET is_upload=1 WHERE Date=\"{date}\" AND Time LIKE \"{times[:5]}%\";"
    update_sql(upload_status_sql)


def get_unupload_data():
    """读取还未上传的数据"""
    # get_date = "SELECT DATE_FORMAT(concat(date(Date), ' ', HOUR (Time), ':', floor(MINUTE(Time)/10 )*10), '%Y-%m-%d %H:%i') AS t FROM `178wx` WHERE is_upload=0 GROUP BY t;"
    get_date = "SELECT DATE_FORMAT(concat(date(Date), ' ', HOUR (Time), ':', MINUTE(Time)), '%Y-%m-%d %H:%i') AS t " \
               "FROM `178wx` WHERE DATE_FORMAT(concat(Date, ' ', Time), '%Y-%m-%d %H:%i:%s')<=NOW() AND is_upload=0 GROUP BY t;"

    date_group = select_sql(get_date)
    if date_group is not False:
        if len(date_group) > 0:
            res_data = []
            for i in date_group:
                # print(i)
                date, times = i["t"].split(" ")     # 2022-07-06 12:00:00
                get_data_sql = f"SELECT CAST(Date AS CHAR) AS Date,CAST(Time AS CHAR) AS Time,Depth,DO,Temp,Chl,Sail FROM `178wx` WHERE is_upload=0 AND Date=\"{date}\" AND Time LIKE \"{times}%\" ORDER BY `Date` DESC,`Time` DESC LIMIT 1;"
                real_data = select_sql(get_data_sql)
                data = real_data[0]
                res_data.append(data)
            return res_data
        else:
            print(f"[{now_time}]-INFO [deal_data] 读取数据量为0！")
            return None
    else:
        print(f"[{now_time}]-ERROR [deal_data] 读取数据失败")
        return False


def main():
    data = get_unupload_data()      # 获取未上传的数据
    if data:
        try:
            send_res = post_data(data)  # post接口，上传数据
            # print(send_res)
            for i in range(len(send_res)):
                if send_res[i]:
                    change_data_status(data[i])     # 本地数据的状态改为已上传
        except Exception as e:
            print(e)


if __name__ == '__main__':

    now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

    config = configparser.ConfigParser()
    config.read("/home/sencott/jiayi/config.ini", encoding="utf-8")

    # 本地数据库的相关信息
    local_db_ip = config.get("localDataBase", 'ip')
    local_db_port = int(config.get("localDataBase", 'port'))
    local_db_user = config.get("localDataBase", 'user')
    local_db_pw = config.get("localDataBase", 'password')
    local_db_dbName = config.get("localDataBase", 'dbName')

    main()
