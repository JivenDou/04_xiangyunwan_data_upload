#!/usr/bin/env python
# encoding: utf-8
"""
@CreateTime: 2022/07/07 09:41
@Author: DJW
@LastEditTime: 2022/10/19 16:59
@Desctiption:祥云湾观测数据上报接口
"""

import datetime
import time
import pymysql
import socket
import configparser
import requests
import os


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


def send_socket(ip, port, data):
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
    report_time = datetime.datetime.now().strftime('%Y-%m-%d')
    # aquafarm_name = config.get("observePostData", 'aquafarmName')
    for i in data:
        # {'Date': '2021-07-07', 'Time': '17:00:28', 'windspeed': 2.35344, 'winddirection': 0.0, 'airtemp': 0.0,
        # 'humdity': 0.0, 'airpressure': 0.0, 'rain': 0.0, 'flowV1': 0.0, 'flowV2': 0.0, 'flowV3': 0.0, 'flowV4': 0.0,
        # 'flowV5': 0.0, 'Depth': 0.0, 'DO': 0.0, 'Temp': 0.0, 'Chl': 0.0, 'Sali': 0.0, 'PH': 0.0}
        monitor_time = f"{i['Date']} {i['Time']}"
        data = {
            "ranchId": ranch_id,
            "reportTime": report_time,
            "itemList": [
                {"key": "temp", "value": i["airtemp"], "monitorTime": monitor_time},
                {"key": "humidity", "value": i["humdity"], "monitorTime": monitor_time},
                {"key": "windSpeed", "value": i["windspeed"], "monitorTime": monitor_time},
                {"key": "windDirection", "value": i["winddirection"], "monitorTime": monitor_time},
                {"key": "rainfall", "value": i["rain"], "monitorTime": monitor_time},
                {"key": "pressure", "value": i["airpressure"], "monitorTime": monitor_time},
                {"key": "waterTemperature", "value": i["Temp"], "monitorTime": monitor_time},
                {"key": "waterDepth", "value": i["Depth"], "monitorTime": monitor_time},
                {"key": "chlorophyll", "value": i["Chl"], "monitorTime": monitor_time},
                {"key": "salinity", "value": i["Sali"], "monitorTime": monitor_time},
                {"key": "oxygen", "value": i["DO"], "monitorTime": monitor_time},
                {"key": "ph", "value": i["PH"], "monitorTime": monitor_time},
                {"key": "firstVelocity", "value": i["flowV1"], "monitorTime": monitor_time},
                {"key": "secondVelocity ", "value": i["flowV2"], "monitorTime": monitor_time},
                {"key": "thirdVelocity", "value": i["flowV3"], "monitorTime": monitor_time},
                {"key": "fourthVelocity", "value": i["flowV4"], "monitorTime": monitor_time},
                {"key": "fifthVelocity", "value": i["flowV5"], "monitorTime": monitor_time},
            ]
        }
        print(data)
        # try:
        #     ret = requests.post(url=url, json=data, verify=False)
        #     if ret.status_code == 200:
        #         print(now_time, ret.text, ret.status_code)
        #         res_result.append(True)
        #     else:
        #         print(now_time, ret.text, ret.status_code)
        #         res_result.append(False)
        # except Exception as e:
        #     print(f"[{now_time}]-ERROR [post_data] {e},url={url}, data={data}")
        #     res_result.append(False)

    return res_result


def change_data_status(data):
    """更新已入网的数据的状态"""
    # {'Date': '2021-07-07', 'Time': '17:00:28', 'windspeed': 2.35344, 'winddirection': 0.0, 'airtemp': 0.0,
    # 'humdity': 0.0, 'airpressure': 0.0, 'rain': 0.0, 'flowV1': 0.0, 'flowV2': 0.0, 'flowV3': 0.0, 'flowV4': 0.0,
    # 'flowV5': 0.0, 'Depth': 0.0, 'DO': 0.0, 'Temp': 0.0, 'Chl': 0.0, 'Sali': 0.0, 'PH': 0.0}
    date = data["Date"]
    times = data["Time"]
    upload_status_sql = f"UPDATE `xiangyunwan_qx_tbl` SET is_upload=1 WHERE Date=\"{date}\" AND Time LIKE \"{times[:5]}%\";"
    update_sql(upload_status_sql)
    upload_status_sql = f"UPDATE `xiangyunwan_sw_tbl` SET is_upload=1 WHERE Date=\"{date}\" AND Time LIKE \"{times[:5]}%\";"
    update_sql(upload_status_sql)
    upload_status_sql = f"UPDATE `xiangyunwan_sz_tbl` SET is_upload=1 WHERE Date=\"{date}\" AND Time LIKE \"{times[:5]}%\";"
    update_sql(upload_status_sql)


def get_unupload_data():
    """读取还未上传的数据"""
    qx = "xiangyunwan_qx_tbl"
    sw = "xiangyunwan_sw_tbl"
    sz = "xiangyunwan_sz_tbl"
    get_date = "SELECT DATE_FORMAT(concat(date(qx.Date), ' ', HOUR(qx.Time), ':', MINUTE(qx.Time)), '%Y-%m-%d %H:%i') AS t " \
               f"FROM {qx} qx LEFT JOIN {sw} sw ON qx.Date=sw.Date AND qx.Time=sw.Time " \
               f"LEFT JOIN {sz} sz ON sw.Date=sz.Date AND sw.Time=sz.Time " \
               f"WHERE DATE_FORMAT(concat(qx.Date, ' ', qx.Time), '%Y-%m-%d %H:%i:%s')<=NOW() AND qx.is_upload=0 GROUP BY t LIMIT 2;"
    date_group = select_sql(get_date)
    print("get_date is gone")
    if date_group is not False:
        if len(date_group) > 0:
            res_data = []
            num = 0
            try:
                for i in date_group:
                    date, times = i["t"].split(" ")     # 2021-07-07 17:01
                    get_data_sql = "SELECT CAST(qx.Date AS CHAR) Date, CAST(qx.Time AS CHAR) Time, windspeed, winddirection, airtemp, humdity, " \
                                   "airpressure, rain, flowV1, flowV2, flowV3, flowV4, flowV5, Depth, `DO`, Temp, Chl, Sali, PH " \
                                   f"FROM {qx} qx LEFT JOIN {sw} sw ON qx.Date=sw.Date AND qx.Time=sw.Time " \
                                   f"LEFT JOIN {sz} sz ON sw.Date=sz.Date AND sw.Time=sz.Time " \
                                   "WHERE qx.is_upload = 0 AND sw.is_upload = 0 AND sz.is_upload = 0 " \
                                   f"AND qx.Date=\"{date}\" AND qx.Time LIKE \"{times}:%\" " \
                                   "ORDER BY qx.Date DESC,qx.Time DESC LIMIT 1;"
                    real_data = select_sql(get_data_sql)
                    data = real_data[0]
                    res_data.append(data)
                    num += 1
                print(num)
                return res_data
            except Exception as e:
                print(f"[{now_time}]-ERROR [get_unupload_data] {e} ")
                print(date, times)
                print(real_data)
                return False
        else:
            print(f"[{now_time}]-INFO [get_unupload_data] 读取数据量为0！")
            return None
    else:
        print(f"[{now_time}]-ERROR [get_unupload_data] 读取数据失败")
        return False


def main():
    data = get_unupload_data()      # 获取未上传的数据
    # print(data)
    if data:
        try:
            send_res = post_data(data)  # post接口，上传数据
            # print(send_res)
            # for i in range(len(send_res)):
            #     if send_res[i]:
            #         change_data_status(data[i])     # 本地数据的状态改为已上传
        except Exception as e:
            print(e)


if __name__ == '__main__':

    now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    # 读取配置文件信息
    path = os.path.abspath(os.path.dirname(__file__))
    config = configparser.ConfigParser()
    config.read(path + "/config.ini", encoding="utf-8")

    # 本地数据库的相关信息
    local_db_ip = config.get("localDataBase", 'ip')
    local_db_port = int(config.get("localDataBase", 'port'))
    local_db_user = config.get("localDataBase", 'user')
    local_db_pw = config.get("localDataBase", 'password')
    local_db_dbName = config.get("localDataBase", 'dbName')

    main()
