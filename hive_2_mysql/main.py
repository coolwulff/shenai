#!/usr/bin/python
# -*- coding:utf8 -*-
"""
一、安装环境：
    python3
    pip install pymysql,pyhive
二、实现功能：
    将hive的增量数据，同步到mysql数据库
三、运行方法：
    a)定时任务
        配置:crontab -e
        每天23点(绝对路径):0 23 * * * /usr/local/python3/bin/python3 -W ignore /usr/local/kidney_tumor/main.py  >> /var/log/main-20220105.log 2>&1
        重启服务:service crond restart
        查看运行状态:cat /var/log/cron
        列出所有任务:crontab -l
        列出某一个用户任务:crontab -l -u username
        删除全部任务:crontab -r
        删除某一个用户任务:crontab -r -u username
    b) 日志位置(循环读取)
        tail /var/log/main-20220105.log
"""
import regex

from pyhive import hive
import pymysql as mysqldb
import pandas as pd

from config import *
from dict import data_process_dict
from dict import extract_rule_dict
from dict import extract_rule_dict_operate
from dict import computer_rule_dict
from Struct import Struct_data


class HiveClient:
    def __init__(self, host, port, database, username, password, auth):
        """
        create connection to hive server
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.auth = auth

    def get_connection(self):
        """
        get the connection of the hive
        :return:
        """
        return hive.Connection(host=self.host, port=self.port, database=self.database, username=self.username,
                               password=self.password, auth=self.auth)

    def close(self, conn=None, cursor=None):
        """
        close connection
        """
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


class HiveOperator(object):
    def __init__(self, cursor, sql):
        self.cursor = cursor
        self.sql = sql

    def execute(self):
        """
        执行sql
        :return:
        """
        self.cursor.execute(self.sql)

    def get_data(self):
        """
        执行sql
        :return:
        """
        self.cursor.execute(self.sql)
        res = self.cursor.fetchall()
        return res


class MysqlClient:
    def __init__(self, host, user, passwd, port, database, charset):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port
        self.database = database
        self.charset = charset

    def get_connection(self):
        """
        get the connection of the mysql
        :return:
        """
        return mysqldb.connect(host=self.host, user=self.user, passwd=self.passwd, port=self.port,
                               database=self.database, charset=self.charset)

    def close(self, conn=None, cursor=None):
        """
        close connection
        """
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


class MysqlOperator(object):
    def __init__(self, cursor, sql):
        self.cursor = cursor
        self.sql = sql

    def execute(self):
        """
        执行sql
        :return:
        """
        self.cursor.execute(self.sql)

    def get_address(self):
        """
        执行sql
        :return:
        """
        self.cursor.execute(self.sql)
        rows = self.cursor.fetchall()
        column = self.cursor.description
        column_name = [column[i][0] for i in range(len(column))]
        res = pd.DataFrame(list(rows), columns=column_name)
        return res


def extract(res_string, body_args, jcbw_args):
    """
    对form_ultrasound_info中的jcbw字段进行操作
    :param res_string:
    :param body_args:检查部位字段位置
    :param jcbw_args:
    :return:
    """
    for i in range(len(res_string)):
        if res_string[i][body_args] is None:
            res_string[i] = res_string[i][:jcbw_args] + ('NULL', 'NULL',) + res_string[i][jcbw_args:]
        elif any(e in res_string[i][body_args] for e in ['肾', '输尿管', '肾上腺', '肾动脉', '肾静脉', '后腹膜淋巴结']):
            body_site = regex.findall(r"\L<words>", res_string[i][body_args],
                                      words=['肾', '输尿管', '肾上腺', '肾动脉', '肾静脉', '后腹膜淋巴结'])
            body_site = ",".join(set(body_site))
            res_string[i] = res_string[i][:jcbw_args] + (body_site, 'NULL',) + res_string[i][jcbw_args:]
        else:
            body_site = res_string[i][body_args].replace('（需空腹）', '')
            body_site = body_site.replace('（需膀胱充盈）', '')
            body_site = body_site.replace('（请注明部位）', '')
            body_site = body_site.replace('（含肝静脉、脾静脉，需空腹）', '')
            body_site = body_site.replace('(腔内超声）', '')
            body_site = body_site.replace('（普通）', '')
            body_site = body_site.replace('（床旁）', '')
            body_site = body_site.replace('(胸部含胸腔、纵膈)', '')
            body_site = body_site.replace('(胸部含胸腔纵膈)', '')
            res_string[i] = res_string[i][:jcbw_args] + ('其他', body_site,) + res_string[i][jcbw_args:]
    return res_string

"""
这个函数的意义是对form_ultrasound_info中的jcbw(检查部位)字段进行操作，即对超声检查的检查部位进行分类和提取。函数的参数和返回值如下：

res_string: 一个列表，每个元素是一个元组，表示一条超声检查的记录，包含多个字段，如检查号，检查部位，检查结果等。
body_args: 一个整数，表示检查部位字段在元组中的位置。
jcbw_args: 一个整数，表示jcbw字段在元组中的位置。
return: 一个列表，每个元素是一个元组，表示经过处理后的超声检查的记录，其中jcbw字段被拆分为两个字段，分别表示检查部位的大类和小类。
函数的主要逻辑是：

遍历res_string中的每个元素，即每条超声检查的记录。
如果检查部位字段为空，就在jcbw字段的位置插入两个’NULL’，表示没有检查部位的信息。
如果检查部位字段包含以下任意一个词：[‘肾’, ‘输尿管’, ‘肾上腺’, ‘肾动脉’, ‘肾静脉’, ‘后腹膜淋巴结’]，就用正则表达式提取出这些词，去除重复，用逗号连接，作为检查部位的大类，然后在jcbw字段的位置插入这个大类和一个’NULL’，表示没有检查部位的小类。
如果检查部位字段不包含以上任意一个词，就去除检查部位字段中的一些无关的内容，如’（需空腹）‘，’（需膀胱充盈）'等，作为检查部位的小类，然后在jcbw字段的位置插入’其他’和这个小类，表示检查部位的大类是其他。
这个函数的目的是为了将超声检查的检查部位进行统一的格式化和分类，方便后续的分析和处理。
"""


def update_patient_num(res_string, patient_num):
    """
    对qs_drz_patient中的patient_num字段进行操作
    :param res_string:
    :param patient_num:
    :return:
    """
    # patient_num['patient'], patient_num['num'] = patient_num['patient_num'].str.split('-', 1).str
    patient_num['patient'] = patient_num['patient_num'].map(lambda x: x.split('-')[0])
    patient_num['num'] = patient_num['patient_num'].map(lambda x: x.split('-')[1])
    patient_num['num'] = patient_num['num'].astype(int)
    list_patient_num = list(set(patient_num['patient_num']))
    res_string = [list(i) for i in res_string]
    for i in range(len(res_string)):
        # 更新
        if res_string[i][0] in list_patient_num:
            patient_num_int = max(list(set(patient_num[patient_num['patient'] == res_string[i][0][0:4]]['num']))) + 1
            patient_num_new = str(patient_num_int).zfill(4)
            res_string[i][0] = res_string[i][0][0:5] + patient_num_new
            patient_num = patient_num.append({'patient_num': res_string[i][0], 'empi': '0',
                                              'patient': res_string[i][0][0:4], 'num': patient_num_int},
                                             ignore_index=True)
            list_patient_num.append(res_string[i][0])
        else:
            patient_num = patient_num.append({'patient_num': res_string[i][0], 'empi': '0',
                                              'patient': res_string[i][0][0:4], 'num': int(res_string[i][0][5:9])},
                                             ignore_index=True)
            list_patient_num.append(res_string[i][0])
    res_string = tuple(tuple(i) for i in res_string)
    return res_string
"""
这个函数的意义是对qs_drz_patient中的patient_num字段进行操作，即对患者的编号进行更新或添加。函数的参数和返回值如下：

res_string: 一个列表，每个元素是一个元组，表示一条患者的记录，包含多个字段，如患者编号，姓名，性别等。
patient_num: 一个数据框，包含四个字段，分别是patient_num（患者编号），empi（患者唯一标识），patient（患者前缀），num（患者后缀）。
return: 一个列表，每个元素是一个元组，表示经过处理后的患者的记录，其中患者编号可能被更新或保持不变。
函数的主要逻辑是：

首先，对patient_num数据框进行处理，将patient_num字段拆分为patient和num两个字段，分别表示患者编号的前四位和后四位，然后将num字段转换为整数类型。
然后，将res_string列表转换为列表的列表，方便修改元素。
接着，遍历res_string中的每个元素，即每条患者的记录。
如果患者编号已经存在于patient_num数据框中，就说明需要更新患者编号，避免重复。这时，就找出patient_num数据框中与当前患者编号前四位相同的所有记录，取出num字段的最大值，加一，作为新的患者编号的后四位，然后用0补齐，拼接成新的患者编号，替换原来的患者编号。同时，将新的患者编号和相关信息添加到patient_num数据框中，以及list_patient_num列表中，方便后续的判断。
如果患者编号不存在于patient_num数据框中，就说明不需要更新患者编号，直接将患者编号和相关信息添加到patient_num数据框中，以及list_patient_num列表中，方便后续的判断。
最后，将res_string列表转换回列表的元组，作为返回值。
这个函数的目的是为了保证患者编号的唯一性和连续性，方便后续的管理和查询。
"""

def update_address(res_string, provinces, cities, districts):
    """
    对form_basic_info中的省市区字段进行操作
    :param res_string:
    :param provinces:
    :param cities:
    :param districts:
    :return:
    """
    res_string = [list(i) for i in res_string]
    for i in range(len(res_string)):
        # 更新户口地址21-26
        # 户口地址：区代码为NULL或后两位=00，且市代码不为NULL且后两位!=00，则区代码=市代码
        if (res_string[i][24] is None or res_string[i][24][4:6] == '00') and \
                (res_string[i][22] is not None and res_string[i][22][4:6] != '00'):
            res_string[i][24] = res_string[i][22]
        # 户口地址：区代码为NULL或后两位=00，且市代码为NULL或后两位=00，则区代码、名称=NULL
        elif (res_string[i][24] is None or res_string[i][24][4:6] == '00') and \
                (res_string[i][22] is None or res_string[i][22][4:6] == '00'):
            res_string[i][24] = None
            res_string[i][25] = None
        else:
            pass

        # 户口地址：区代码不为NULL且和省代码一致或省代码为NULL和市代码一致或省、市代码为NULL，根据区代码去区字典表找到区名称并赋值
        if (res_string[i][24] is not None and res_string[i][20] is not None and
            res_string[i][24][0:2] == res_string[i][20][0:2]) or \
                (res_string[i][24] is not None and res_string[i][22] is not None and res_string[i][20] is None and
                 res_string[i][24][0:2] == res_string[i][22][0:2]) or \
                (res_string[i][24] is not None and res_string[i][22] is None and res_string[i][20] is None):
            districts_hks = districts[districts['code'] == res_string[i][24]]['name'].values
            districts_hk = districts_hks[0] if len(districts_hks) else None
            res_string[i][25] = districts_hk
        else:
            res_string[i][24] = None
            res_string[i][25] = None

        # 户口地址：省代码为NULL或市代码与省代码一致，市代码保留前4位，市字典表匹配市名称并赋值.市代码前两位与省代码不一致，则赋值NULL
        if (res_string[i][22] is not None and res_string[i][20] is None) or \
                (res_string[i][22] is not None and res_string[i][20] is not None and
                 res_string[i][22][0:2] == res_string[i][20][0:2]):
            res_string[i][22] = res_string[i][22][0:4]
            cities_hks = cities[cities['code'] == res_string[i][22]]['name'].values
            cities_hk = cities_hks[0] if len(cities_hks) else None
            res_string[i][23] = cities_hk
        else:
            res_string[i][22] = None
            res_string[i][23] = None

        # 户口地址：省代码不为NULL，省代码只保留前2位，根据省代码去省字典表找到省名称并赋值
        if res_string[i][20] is not None:
            res_string[i][20] = res_string[i][20][0:2]
            provinces_hks = provinces[provinces['code'] == res_string[i][20]]['name'].values
            provinces_hk = provinces_hks[0] if len(provinces_hks) else None
            res_string[i][21] = provinces_hk
        else:
            pass

        # 更新现住址地址32-37
        # 现住址地址：区代码为NULL或后两位=00，且市代码不为NULL且后两位!=00，则区代码=市代码
        if (res_string[i][35] is None or res_string[i][35][4:6] == '00') and \
                (res_string[i][33] is not None and res_string[i][33][4:6] != '00'):
            res_string[i][35] = res_string[i][33]
        # 现住址地址：区代码为NULL或后两位=00，且市代码为NULL或后两位=00，则区代码、名称=NULL
        elif (res_string[i][35] is None or res_string[i][35][4:6] == '00') and \
                (res_string[i][33] is None or res_string[i][33][4:6] == '00'):
            res_string[i][35] = None
            res_string[i][36] = None
        else:
            pass

        # 现住址地址：区代码不为NULL且和省代码一致或省代码为NULL和市代码一致或省、市代码为NULL，根据区代码去区字典表找到区名称并赋值
        if (res_string[i][35] is not None and res_string[i][31] is not None and
            res_string[i][35][0:2] == res_string[i][31][0:2]) or \
                (res_string[i][35] is not None and res_string[i][33] is not None and res_string[i][31] is None and
                 res_string[i][35][0:2] == res_string[i][33][0:2]) or \
                (res_string[i][35] is not None and res_string[i][33] is None and res_string[i][31] is None):
            districts_xzzs = districts[districts['code'] == res_string[i][35]]['name'].values
            districts_xzz = districts_xzzs[0] if len(districts_xzzs) else None
            res_string[i][36] = districts_xzz
        else:
            res_string[i][35] = None
            res_string[i][36] = None

        # 现住址地址：省代码为NULL或市代码与省代码一致，市代码保留前4位，市字典表匹配市名称并赋值.市代码前两位与省代码不一致，则赋值NULL
        if (res_string[i][33] is not None and res_string[i][31] is None) or \
                (res_string[i][33] is not None and res_string[i][31] is not None and
                 res_string[i][33][0:2] == res_string[i][31][0:2]):
            res_string[i][33] = res_string[i][33][0:4]
            cities_xzzs = cities[cities['code'] == res_string[i][33]]['name'].values
            cities_xzz = cities_xzzs[0] if len(cities_xzzs) else None
            res_string[i][34] = cities_xzz
        else:
            res_string[i][33] = None
            res_string[i][34] = None

        # 现住址地址：省代码不为NULL，省代码只保留前2位，根据省代码去省字典表找到省名称并赋值
        if res_string[i][31] is not None:
            res_string[i][31] = res_string[i][31][0:2]
            provinces_xzzs = provinces[provinces['code'] == res_string[i][31]]['name'].values
            provinces_xzz = provinces_xzzs[0] if len(provinces_xzzs) else None
            res_string[i][32] = provinces_xzz
        else:
            pass

        # 更新籍贯地址28-31
        # 籍贯地址：省代码不为NULL，省代码只保留前2位，根据省代码去省字典表找到省名称并赋值
        if res_string[i][27] is not None:
            res_string[i][27] = res_string[i][27][0:2]
            provinces_jgs = provinces[provinces['code'] == res_string[i][27]]['name'].values
            provinces_jg = provinces_jgs[0] if len(provinces_jgs) else None
            res_string[i][28] = provinces_jg
        else:
            pass

        # 籍贯地址：省代码为NULL市代码不为NULL且前两位有效或省市代码一致且有效，根据市六位代码匹配区字典表找到区名称并赋值
        if (res_string[i][29] is not None and res_string[i][27] is None and res_string[i][29][4:6] != '00') or \
                (res_string[i][29] is not None and res_string[i][27] is not None and res_string[i][29][4:6] != '00'
                 and res_string[i][29][0:2] == res_string[i][27][0:2]):
            # 籍贯地址：市代码为NULL或(不为NULL且后两位!=00)，则插入值作为区代码和区名称
            districts_jgs = districts[districts['code'] == res_string[i][29]]['name'].values
            districts_jg = districts_jgs[0] if len(districts_jgs) else None
            res_string[i].insert(31, res_string[i][29])
            res_string[i].insert(32, districts_jg)
        else:
            res_string[i].insert(31, None)
            res_string[i].insert(32, None)

        # 籍贯地址：省代码为NULL或市代码与省代码一致，市代码保留前4位，市字典表匹配市名称并赋值.市代码前两位与省代码不一致，则赋值NULL
        if (res_string[i][29] is not None and res_string[i][27] is None) or \
                (res_string[i][29] is not None and res_string[i][27] is not None and
                 res_string[i][29][0:2] == res_string[i][27][0:2]):
            res_string[i][29] = res_string[i][29][0:4]
            cities_jgs = cities[cities['code'] == res_string[i][29]]['name'].values
            cities_jg = cities_jgs[0] if len(cities_jgs) else None
            res_string[i][30] = cities_jg
        else:
            res_string[i][29] = None
            res_string[i][30] = None
    res_string = tuple(tuple(i) for i in res_string)
    return res_string


def struct_data(res_string):
    """
    对form_operate_pathology_info中的手术描述和检查结论字段进行后结构化
    :param res_string:
    :return:
    """
    res_string = [list(i) for i in res_string]
    rows = len(res_string)
    data_operate_col = []
    data_exam_col = []

    for i in range(rows):
        line_operate = '' if res_string[i][9] is None else res_string[i][9]  # 手术描述
        line_exam = '' if res_string[i][16] is None else res_string[i][16]  # 检查结论
        data_operate_col.append(line_operate)
        data_exam_col.append(line_exam)
    s_operate = Struct_data(data_operate_col, data_process_dict, extract_rule_dict_operate, computer_rule_dict)
    s_exam = Struct_data(data_exam_col, data_process_dict, extract_rule_dict, computer_rule_dict)
    result_operate = s_operate.main()
    result_exam = s_exam.main()
    r = 0
    for one in result_operate:
        if result_exam[r]['result'] != {}:
            merges = dict(one['result'], **result_exam[r]['result'])
        else:
            merges = one['result']
        res_string[r].insert(10, None)
        res_string[r].insert(11, None)
        res_string[r].insert(19, None)
        res_string[r].insert(20, None)
        res_string[r].insert(21, None)
        res_string[r].insert(22, None)
        res_string[r].insert(23, None)
        res_string[r].insert(24, None)
        res_string[r].insert(25, None)
        if len(merges) != 0:
            for key, value in merges.items():
                if 'block_way' in key:
                    res_string[r][10] = key.replace('block_way_', '')
                elif 'block_time' in key:
                    if '时' in value[0]['entity']:
                        hour_minute = value[0]['entity'].replace('小时', '时').split('时')
                        hours, minutes = map(float, [i if len(i) != 0 and i.isdigit() else '0' for i in hour_minute])
                        res_string[r][11] = hours * 60 + minutes
                    elif '分' in value[0]['entity']:
                        minute_second = value[0]['entity'].replace('分钟', '分').split('分')
                        minutes, sec = map(float, [i if len(i) != 0 and i.isdigit() else '0' for i in minute_second])
                        res_string[r][11] = minutes + round(sec / 60, 2)
                    elif value[0]['entity'].replace('.', '').isdigit():
                        res_string[r][11] = value[0]['entity']
                    else:
                        pass
                elif 'pathology_pattern' in key and 'pathology_pattern_other' not in key:
                    res_string[r][19] = key.replace('pathology_pattern_', '')
                elif 'pathology_pattern_other' in key:
                    res_string[r][20] = key.replace('pathology_pattern_other_', '')
                elif 'who_isup' in key:
                    res_string[r][21] = key.replace('who_isup_', '')
                elif 'tumor_status' in key:
                    res_string[r][22] = key.replace('tumor_status_', '')
                elif 'tumor_size1' in key:
                    if '-' in value[0]['entity']:
                        min_max = value[0]['entity'].split('-')
                        size1, size2 = \
                            map(float, [i if len(i) != 0 and i.replace('.', '').isdigit() else '0' for i in min_max])
                        res_string[r][23] = max(size1, size2) * 10
                    else:
                        try:
                            res_string[r][23] = float(value[0]['entity']) * 10
                        except:
                            res_string[r][23] = value[0]['entity']
                elif 'tumor_size2' in key:
                    try:
                        res_string[r][24] = float(value[0]['entity']) * 10
                    except:
                        res_string[r][24] = value[0]['entity']
                elif 'tumor_size3' in key:
                    try:
                        res_string[r][25] = float(value[0]['entity']) * 10
                    except:
                        res_string[r][25] = value[0]['entity']
                else:
                    pass
        else:
            pass
        r += 1
    res_string = tuple(tuple(i) for i in res_string)
    return res_string

"""
form_operate_pathology_info中的手术描述和检查结论字段进行后结构化，即将这两个字段中的文本信息提取出来，转换为结构化的数据，方便后续的分析和处理。脚本的参数和返回值如下：

res_string: 一个列表，每个元素是一个元组，表示一条手术病理信息的记录，包含多个字段，如患者编号，手术日期，手术描述，检查结论等。
return: 一个列表，每个元素是一个元组，表示经过处理后的手术病理信息的记录，其中手术描述和检查结论字段被拆分为多个字段，分别表示不同的信息，如阻断方式，阻断时间，病理类型，肿瘤大小等。
脚本的主要逻辑是：

首先，将res_string列表转换为列表的列表，方便修改元素。
然后，定义两个空列表，分别用于存储手术描述和检查结论字段的数据。
接着，遍历res_string中的每个元素，即每条手术病理信息的记录。
对于每个元素，分别提取出手术描述和检查结论字段的值，如果为空，就用空字符串代替，然后分别添加到对应的列表中。
然后，调用Struct_data这个类，传入手术描述和检查结论的列表，以及一些其他的参数，如data_process_dict, extract_rule_dict_operate, computer_rule_dict等，这些参数是用于对文本进行预处理，提取，计算的规则字典。
接着，调用Struct_data类的main方法，得到两个结果列表，分别表示手术描述和检查结论的后结构化结果，每个元素是一个字典，包含result和error两个键，result的值是一个字典，表示提取出来的信息，error的值是一个列表，表示提取过程中的错误信息。
然后，遍历两个结果列表，将提取出来的信息插入到原来的记录中，形成新的字段，如阻断方式，阻断时间，病理类型，肿瘤大小等，如果没有提取出来的信息，就用None代替。
在插入值的过程中，还需要对一些特殊的情况进行处理，如：
如果阻断时间的值包含时和分，就需要将其转换为分钟数，如1小时30分钟，就转换为90分钟。
如果阻断时间的值包含分和秒，就需要将其转换为分钟数，并保留两位小数，如2分钟30秒，就转换为2.5分钟。
如果阻断时间的值是一个数字，就直接使用它，如3.5，就表示3.5分钟。
如果阻断时间的值不是以上任何一种情况，就不做任何处理，如未知，就保留为未知。
如果肿瘤大小的值是一个范围，就需要取出最大值，如1-2，就取2。
如果肿瘤大小的值是一个数字，就直接使用它，如3，就表示3厘米。
如果肿瘤大小的值不是以上任何一种情况，就不做任何处理，如未见，就保留为未见。
最后，将res_string列表转换回列表的元组，作为返回值。
"""
"""
data_operate_col = []
data_exam_col = []

for i in range(rows):
    line_operate = '' if res_string[i][9] is None else res_string[i][9]  # 手术描述
    line_exam = '' if res_string[i][16] is None else res_string[i][16]  # 检查结论
    data_operate_col.append(line_operate)
    data_exam_col.append(line_exam)

替换为：

data_operate_col = ['' if res_string[i][9] is None else res_string[i][9] for i in range(rows)]
data_exam_col = ['' if res_string[i][16] is None else res_string[i][16] for i in range(rows)]

这样可以减少代码的行数，提高代码的可读性。

您可以使用enumerate函数来遍历列表，而不是使用索引，如将以下代码：
r = 0
for one in result_operate:
    ...
    r += 1

替换为：

for r, one in enumerate(result_operate):
    ...

这样可以避免手动维护索引，提高代码的简洁性。

您可以使用字典的get方法来获取值，而不是使用键，如将以下代码：
if 'block_way' in key:
    res_string[r][10] = key.replace('block_way_', '')
elif 'block_time' in key:
    if '时' in value[0]['entity']:
        ...

替换为：

block_way = key.get('block_way')
block_time = key.get('block_time')
if block_way:
    res_string[r][10] = block_way.replace('block_way_', '')
elif block_time:
    if '时' in value[0]['entity']:
        ...

这样可以避免重复访问字典，提高代码的效率。

您可以使用正则表达式来匹配和提取数字，而不是使用字符串的split和replace方法，如将以下代码：
if '时' in value[0]['entity']:
    hour_minute = value[0]['entity'].replace('小时', '时').split('时')
    hours, minutes = map(float, [i if len(i) != 0 and i.isdigit() else '0' for i in hour_minute])
    res_string[r][11] = hours * 60 + minutes
elif '分' in value[0]['entity']:
    minute_second = value[0]['entity'].replace('分钟', '分').split('分')
    minutes, sec = map(float, [i if len(i) != 0 and i.isdigit() else '0' for i in minute_second])
    res_string[r][11] = minutes + round(sec / 60, 2)
elif value[0]['entity'].replace('.', '').isdigit():
    res_string[r][11] = value[0]['entity']
else:
    pass

替换为：

import re
pattern = re.compile(r'(\d+\.?\d*)')
numbers = pattern.findall(value[0]['entity'])
if len(numbers) == 2:
    res_string[r][11] = float(numbers[0]) * 60 + float(numbers[1])
elif len(numbers) == 1:
    res_string[r][11] = float(numbers[0])
else:
    pass
"""




def update_order_name(res_string):
    """
    对form_medication_orders中的orders_name字段进行操作
    :param res_string:
    :return:
    """
    orders_name = ['索坦', '维全特', '英立达', 'Cabometyx', '乐卫玛', '多吉美', '飞尼妥', '福可维', 'Welireg', '拓益',
                   '百泽安', '艾瑞卡', '达伯舒', '可瑞达', '欧狄沃', '逸沃']
    for i in range(len(res_string)):
        for j in orders_name:
            orders_qt = orders_name.copy()
            orders_qt.remove(j)
            if j in res_string[i][17] and not any(e in res_string[i][17] for e in orders_qt):
                res_string[i] = res_string[i][:17] + (j,) + res_string[i][17:]
            elif j in res_string[i][17] and any(e in res_string[i][17] for e in orders_qt):
                res_string[i] = res_string[i][:17] + ('NULL',) + res_string[i][17:]
            else:
                pass
    return res_string

"""
这个函数的意义是对form_medication_orders中的orders_name字段进行操作，即对患者的用药名称进行提取和标准化。函数的参数和返回值如下：

res_string: 一个列表，每个元素是一个元组，表示一条用药记录，包含多个字段，如患者编号，用药日期，用药名称等。
return: 一个列表，每个元素是一个元组，表示经过处理后的用药记录，其中orders_name字段被添加到原来的用药名称字段之前，表示用药名称的标准化结果。
函数的主要逻辑是：

首先，定义一个orders_name列表，包含16个常见的用药名称，如索坦，维全特，英立达等。
然后，遍历res_string中的每个元素，即每条用药记录。
对于每个元素，再遍历orders_name列表中的每个用药名称。
如果用药名称在用药记录的用药名称字段中出现，并且没有其他的用药名称出现，就将用药名称作为orders_name字段，添加到用药名称字段之前，表示用药名称的标准化结果。
如果用药名称在用药记录的用药名称字段中出现，但是有其他的用药名称也出现，就将’NULL’作为orders_name字段，添加到用药名称字段之前，表示用药名称无法标准化。
如果用药名称在用药记录的用药名称字段中没有出现，就不做任何操作。
这个函数的目的是为了将用药记录的用药名称进行统一的格式化和标准化，方便后续的分析和处理。
"""

def hive_to_mysql():
    try:
        hive_client = HiveClient(host=hive_args['server'], port=hive_args['port'], database=hive_args['database'],
                                 username=hive_args['username'], password=hive_args['password'], auth=hive_args['auth'])
        hive_conn = hive_client.get_connection()
        hive_cursor = hive_conn.cursor()

        mysql_client = MysqlClient(host=mysql_args['server'], user=mysql_args['user'], passwd=mysql_args['password'],
                                   port=mysql_args['port'], database=mysql_args['database'],
                                   charset=mysql_args['charset'])
        mysql_conn = mysql_client.get_connection()
        mysql_cursor = mysql_conn.cursor()

        m_operator = MysqlOperator(cursor=mysql_cursor, sql=select_patient_num)
        patient_num = m_operator.get_address()
        m_operator = MysqlOperator(cursor=mysql_cursor, sql=select_province)
        province = m_operator.get_address()
        m_operator = MysqlOperator(cursor=mysql_cursor, sql=select_city)
        city = m_operator.get_address()
        m_operator = MysqlOperator(cursor=mysql_cursor, sql=select_district)
        district = m_operator.get_address()

        for key in entry_tables:
            print(key)
            fields = entry_tables[key]

            if key == 'cdr_patient_info':
                select_sql = "select distinct {} from {}".format(",".join(str(x) for x in fields), 'skzbk.'+key)
            elif key == 'form_medication_orders':
                fields_copy = fields.copy()
                fields_copy.remove('orders_name')
                select_sql = "select distinct {} from {}"\
                    .format(",".join(str(x) for x in fields_copy), 'skzbk.'+key+'_kidney')
            elif key == 'form_ris_follow':
                select_sql = "select distinct {} from {} where question_id is null or question_id != 8"\
                    .format(",".join(str(x) for x in fields), 'skzbk.'+key+'_kidney')
            elif key == 'form_ris_follow_ultra':
                fields_copy = fields.copy()
                fields_copy.remove('jcbw')
                fields_copy.remove('jcbw_qt')
                select_sql = "select distinct {} from {} where question_id = 8"\
                    .format(",".join(str(x) for x in fields_copy), 'skzbk.'+'form_ris_follow'+'_kidney')
            elif key == 'form_ultrasound_info':
                fields_copy = fields.copy()
                fields_copy.remove('jcbw')
                fields_copy.remove('jcbw_qt')
                select_sql = "select distinct {} from {}"\
                    .format(",".join(str(x) for x in fields_copy), 'skzbk.'+key+'_kidney')
            else:
                select_sql = "select distinct {} from {}"\
                    .format(",".join(str(x) for x in fields), 'skzbk.'+key+'_kidney')

            h_operator = HiveOperator(cursor=hive_cursor, sql="set hive.execution.engine=spark")
            h_operator.execute()
            h_operator = HiveOperator(cursor=hive_cursor, sql=select_sql)
            res = h_operator.get_data()

            if key == 'qs_drz_patient':
                # 插入待入组表的patient_num唯一
                res = update_patient_num(res, patient_num)
            elif key == 'form_medication_orders':
                # 医嘱用药、门诊处方名称处理
                res = update_order_name(res)
            elif key == 'form_operate_pathology_info':
                # 手术描述和检查结论中匹配阻断方式、阻断时间、病理类型、WHO/ISUP核分级、肿瘤合并情况、肿瘤大小
                res = struct_data(res)
                fields.insert(10, 'block_way')
                fields.insert(11, 'block_time')
                fields.insert(19, 'pathology_pattern')
                fields.insert(20, 'pathology_pattern_other')
                fields.insert(21, 'who_isup')
                fields.insert(22, 'tumor_status')
                fields.insert(23, 'tumor_size1')
                fields.insert(24, 'tumor_size2')
                fields.insert(25, 'tumor_size3')
            elif key in ['form_ris_follow_ultra']:
                # 门诊超声
                key = 'form_ris_follow'
                res = extract(res, 12, 21)
            elif key == 'form_ultrasound_info':
                # 检查部位中匹配肾、输尿管、肾上腺、肾动脉、肾静脉、后腹膜淋巴结,将结果拼接
                res = extract(res, 9, 14)
            elif key == 'form_basic_info':
                # 地址修改，省代码2位、市代码4位、区代码6位，且省市区需保持一致
                res = update_address(res, province, city, district)
                fields.insert(31, 'native_county_code')
                fields.insert(32, 'native_county_name')
            else:
                pass

            for item in res:
                item = \
                    ['NULL' if x is None or x == 'None' or x == 'NoneNone' or x == '' or x == ' ' else x for x in item]

                insert_sql = "insert into {} ({}) values ({})".format(key, ",".join(str(x) for x in fields),
                                                                      ",".join("'{}'".format(mysqldb.escape_string(x))
                                                                      if x != 'NULL' and type(x) is str else
                                                                               "{}".format(x) if x == 'NULL' else
                                                                               "'{}'".format(x) for x in item))
                m_operator = MysqlOperator(cursor=mysql_cursor, sql=insert_sql)
                m_operator.execute()
        mysql_conn.commit()
    except Exception as e:
        print(e)
    finally:
        hive_client.close(conn=hive_conn, cursor=hive_cursor)
        mysql_client.close(conn=mysql_conn, cursor=mysql_cursor)


if __name__ == "__main__":
    hive_to_mysql()

