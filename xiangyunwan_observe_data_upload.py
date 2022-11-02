#!/usr/bin/env python
# encoding: utf-8
"""
@CreateTime: 2022/07/07 09:41
@Author: DJW
@LastEditTime: 2022/10/20 16:59
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
        conn = pymysql.connect(host=local_db_ip, user=local_db_user, password=local_db_pw, database=local_db_dbName,
                               port=local_db_port, autocommit=True)
        return conn
    except Exception as e:
        print(f"[{now_time}]-ERROR [conn_mysql] {e}")
        return False


def select_sql(conn, sql):
    """查询MySQL中的数据"""
    try:
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        print(f"[{now_time}]-ERROR [select_sql] {e}")
        return False
    # try:
    #     conn = conn_mysql()
    #     if conn:
    #         cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    #         cursor.execute(sql)
    #         results = cursor.fetchall()
    #         cursor.close()
    #         return results
    # except Exception as e:
    #     print(f"[{now_time}]-ERROR [select_sql] {e}")
    #     return False


def update_sql(conn, sql):
    """更新MySQL中的数据"""
    try:
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        print(f"[{now_time}]-ERROR [select_sql] {e}")
        return False
    # try:
    #     conn = conn_mysql()
    #     if conn:
    #         cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    #         cursor.execute(sql)
    #         conn.commit()
    #         cursor.close()
    #         return True
    # except Exception as e:
    #     print(f"[{now_time}]-ERROR [update_sql] {e}")
    #     return False


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
                    sock.send(each.encode("utf-8"))  # 发送数据，将数据以字节流形式发出
                    # sock.send(each)     # 发送数据，将数据以字节流形式发出
                    time.sleep(1)
                    res_result.append(True)
                    print(f"[{now_time}]-SUCCESS [send_socket] \"{each}\"")
                except Exception as e:
                    print(f"[{now_time}]-ERROR [send_socket] \"{data}\" {e}")
                    res_result.append(False)
    sock.close()  # 关闭套接字
    return res_result


def field_change(field):
    """字段名转换"""
    names = {
        "airtemp": "temp",
        "humdity": "humidity",
        "windspeed": "windSpeed",
        "winddirection": "windDirection",
        "rain": "rainfall",
        "airpressure": "pressure",
        "Temp": "waterTemperature",
        "Depth": "waterDepth",
        "Chl": "chlorophyll",
        "Sali": "salinity",
        "DO": "oxygen",
        "PH": "ph",
        "flowV1": "firstVelocity",
        "flowV2": "secondVelocity",
        "flowV3": "thirdVelocity",
        "flowV4": "fourthVelocity",
        "flowV5": "fifthVelocity",
    }
    return names.get(field, None)


def post_data(data):
    """将数据上传到指定的接口"""
    res_result = []
    url = config.get("url", 'observeUrl')  # 观测数据上报接口需要请求的URL
    ranch_id = config.get("observePostData", 'ranchId')
    # aquafarm_name = config.get("observePostData", 'aquafarmName')
    num = 1
    for i in data:
        # {'existFlag': '111', 'Date': '2021-07-07', 'Time': '17:00:28', 'windspeed': 2.35344, 'winddirection': 0.0, 'airtemp': 0.0,
        # 'humdity': 0.0, 'airpressure': 0.0, 'rain': 0.0, 'flowV1': 0.0, 'flowV2': 0.0, 'flowV3': 0.0, 'flowV4': 0.0,
        # 'flowV5': 0.0, 'Depth': 0.0, 'DO': 0.0, 'Temp': 0.0, 'Chl': 0.0, 'Sali': 0.0, 'PH': 0.0}
        monitor_time = f"{i['Date']} {i['Time']}"
        report_time = datetime.datetime.now().strftime('%Y-%m-%d')

        # itemList组包
        item_list = []
        for k, v in i.items():
            if k != "Date" and k != "Time" and k != "existFlag":
                # print(k, v)
                item_dict = {"key": field_change(k), "value": v, "monitorTime": monitor_time}
                item_list.append(item_dict)
                # print(item_dict)
        data = {
            "ranchId": ranch_id,
            "reportTime": report_time,
            "itemList": item_list
        }
        # print(data)
        try:
            ret = requests.post(url=url, json=data, verify=False)
            if ret.status_code == 200:
                # print(now_time, ret.text, ret.status_code)
                print(f"{num}. “{now_time}”|“{monitor_time}”", ret.text, ret.status_code)
                num += 1
                res_result.append(True)
            else:
                # print(now_time, ret.text, ret.status_code)
                print(f"{num}.", now_time, monitor_time, ret.text, ret.status_code)
                num += 1
                res_result.append(True)
        except Exception as e:
            print(f"[{now_time}]-ERROR [post_data] {e},url={url}, data={data}")
            res_result.append(False)

    return res_result


def change_data_status(data):
    """更新已入网的数据的状态"""
    # 连接数据库
    try:
        conn = conn_mysql()
    except Exception as e:
        print(f"[{now_time}]-ERROR [change_data_status] {e}")
        return False

    # {'existFlag': '111', 'Date': '2021-07-07', 'Time': '17:00:28', 'windspeed': 2.35344, 'winddirection': 0.0, 'airtemp': 0.0,
    # 'humdity': 0.0, 'airpressure': 0.0, 'rain': 0.0, 'flowV1': 0.0, 'flowV2': 0.0, 'flowV3': 0.0, 'flowV4': 0.0,
    # 'flowV5': 0.0, 'Depth': 0.0, 'DO': 0.0, 'Temp': 0.0, 'Chl': 0.0, 'Sali': 0.0, 'PH': 0.0}
    date = data["Date"]
    times = data["Time"]
    # 获取表数据存在标志
    exist_flag = []
    for c in data["existFlag"]:
        exist_flag.append(int(c))

    try:
        if exist_flag[0]:
            upload_status_sql = f"UPDATE `xiangyunwan_qx_tbl` SET is_upload=1 WHERE Date=\"{date}\" AND Time = \"{times}\";"
            update_sql(conn, upload_status_sql)
        if exist_flag[1]:
            upload_status_sql = f"UPDATE `xiangyunwan_sw_tbl` SET is_upload=1 WHERE Date=\"{date}\" AND Time = \"{times}\";"
            update_sql(conn, upload_status_sql)
        if exist_flag[2]:
            upload_status_sql = f"UPDATE `xiangyunwan_sz_tbl` SET is_upload=1 WHERE Date=\"{date}\" AND Time = \"{times}\";"
            update_sql(conn, upload_status_sql)
        print(f"更新完成：{date} {times}  exist_flag：{data['existFlag']}")
    except Exception as e:
        print(f"[{now_time}]-ERROR [change_data_status] {e}")

    # 关闭数据库
    conn.close()


def get_unupload_data():
    """读取还未上传的数据"""
    # 连接数据库
    try:
        conn = conn_mysql()
    except Exception as e:
        print(f"[{now_time}]-ERROR [get_unupload_data] {e}")
        return False
    # 获取各表时间
    qx = "xiangyunwan_qx_tbl"
    sw = "xiangyunwan_sw_tbl"
    sz = "xiangyunwan_sz_tbl"
    get_qx_date = "SELECT DATE_FORMAT(concat(Date, ' ', Time), '%Y-%m-%d %H:%i:%s') AS t " \
                  f"FROM {qx} WHERE DATE_FORMAT(concat(Date, ' ', Time), '%Y-%m-%d %H:%i:%s')<=NOW() " \
                  f"AND is_upload=0;"
    get_sw_date = "SELECT DATE_FORMAT(concat(Date, ' ', Time), '%Y-%m-%d %H:%i:%s') AS t " \
                  f"FROM {sw} WHERE DATE_FORMAT(concat(Date, ' ', Time), '%Y-%m-%d %H:%i:%s')<=NOW() " \
                  f"AND is_upload=0;"
    get_sz_date = "SELECT DATE_FORMAT(concat(Date, ' ', Time), '%Y-%m-%d %H:%i:%s') AS t " \
                  f"FROM {sz} WHERE DATE_FORMAT(concat(Date, ' ', Time), '%Y-%m-%d %H:%i:%s')<=NOW() " \
                  f"AND is_upload=0;"
    qx_date_group = select_sql(conn, get_qx_date)
    sw_date_group = select_sql(conn, get_sw_date)
    sz_date_group = select_sql(conn, get_sz_date)
    # 查数据进行组包
    res_data = []
    number_qx = 0
    all_sum = 0
    # 遍历气象时间
    if qx_date_group is not False:
        if len(qx_date_group) > 0:
            for i in qx_date_group:
                date, times = i["t"].split(" ")  # 2021-07-07 17:00:28
                data_sw = None
                data_sz = None
                get_qx = "SELECT CAST(Date AS CHAR) Date,CAST(Time AS CHAR) Time, windspeed, winddirection, airtemp, humdity, airpressure, rain " \
                         f"FROM {qx} WHERE is_upload=0 AND Date=\"{date}\" AND Time=\"{times}\";"
                data_qx = select_sql(conn, get_qx)

                if i in sw_date_group:
                    get_sw = "SELECT CAST(Date AS CHAR) Date,CAST(Time AS CHAR) Time, flowV1, flowV2, flowV3, flowV4, flowV5 " \
                             f"FROM {sw} WHERE is_upload=0 AND Date=\"{date}\" AND Time=\"{times}\";"
                    data_sw = select_sql(conn, get_sw)
                    sw_date_group.remove(i)
                if i in sz_date_group:
                    get_sz = "SELECT CAST(Date AS CHAR) Date,CAST(Time AS CHAR) Time, Depth, `DO`, Temp, Chl, Sali, PH " \
                             f"FROM {sz} WHERE is_upload=0 AND Date=\"{date}\" AND Time=\"{times}\";"
                    data_sz = select_sql(conn, get_sz)
                    sz_date_group.remove(i)
                # 组合数据
                if data_sw is not None and data_sz is not None:
                    data = {"existFlag": "111", **data_qx[0], **data_sw[0], **data_sz[0]}
                elif data_sw is not None:
                    data = {"existFlag": "110", **data_qx[0], **data_sw[0]}
                elif data_sz is not None:
                    data = {"existFlag": "101", **data_qx[0], **data_sz[0]}
                else:
                    data = {"existFlag": "100", **data_qx[0]}

                res_data.append(data)

                number_qx += 1
                all_sum += 1
                print(f"{number_qx}. {i['t']}")
        # else:
        #     print(f"[{now_time}]-INFO [get_unupload_data][qx] 读取数据量为0！")
    else:
        print(f"[{now_time}]-ERROR [get_unupload_data][qx] 读取数据失败")
        # 关闭数据库
        conn.close()
        return False


    # 遍历水文时间
    number_sw = 0
    if sw_date_group is not False:
        if len(sw_date_group) > 0:
            for i in sw_date_group:
                date, times = i["t"].split(" ")  # 2021-07-07 17:00:28
                data_sz = None
                get_sw = "SELECT CAST(Date AS CHAR) Date,CAST(Time AS CHAR) Time, flowV1, flowV2, flowV3, flowV4, flowV5 " \
                         f"FROM {sw} WHERE is_upload=0 AND Date=\"{date}\" AND Time=\"{times}\";"
                data_sw = select_sql(conn, get_sw)
                if i in sz_date_group:
                    get_sz = "SELECT CAST(Date AS CHAR) Date,CAST(Time AS CHAR) Time, Depth, `DO`, Temp, Chl, Sali, PH " \
                             f"FROM {sz} WHERE is_upload=0 AND Date=\"{date}\" AND Time=\"{times}\";"
                    data_sz = select_sql(conn, get_sz)
                    sz_date_group.remove(i)
                if data_sz is not None:
                    data = {"existFlag": "011", **data_sw[0], **data_sz[0]}
                else:
                    data = {"existFlag": "010", **data_sw[0]}
                # print(data)
                res_data.append(data)
                number_sw += 1
                all_sum += 1
                print(f"{number_sw}. {i['t']}")
        # else:
        #     print(f"[{now_time}]-INFO [get_unupload_data][sw] 读取数据量为0！")
    else:
        print(f"[{now_time}]-ERROR [get_unupload_data][sw] 读取数据失败")
        # 关闭数据库
        conn.close()
        return False


    # 遍历水质时间
    number_sz = 0
    if sz_date_group is not False:
        if len(sz_date_group) > 0:
            for i in sz_date_group:
                date, times = i["t"].split(" ")  # 2021-07-07 17:00:28
                get_sz = "SELECT CAST(Date AS CHAR) Date,CAST(Time AS CHAR) Time, Depth, `DO`, Temp, Chl, Sali, PH " \
                         f"FROM {sz} WHERE is_upload=0 AND Date=\"{date}\" AND Time=\"{times}\";"
                data_sz = select_sql(conn, get_sz)

                data = {"existFlag": "001", **data_sz[0]}
                # print(data)
                res_data.append(data)
                number_sz += 1
                all_sum += 1
                print(f"{number_sz}. {i['t']}")
        # else:
        #     print(f"[{now_time}]-INFO [get_unupload_data][sz] 读取数据量为0！")
    else:
        print(f"[{now_time}]-ERROR [get_unupload_data][sz] 读取数据失败")
        # 关闭数据库
        conn.close()
        return False
    print(f"{now_time} 总条数：{all_sum} 气象遍历条数：{number_qx} 水文遍历条数：{number_sw} 水质遍历条数：{number_sz}")
    # 关闭数据库
    conn.close()
    return res_data


def main():
    data = get_unupload_data()  # 获取未上传的数据
    # print(data)
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
