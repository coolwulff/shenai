#!/usr/bin/python
# -*- coding:utf8 -*-
"""
一、安装环境：
    python3
    pip install pymysql
二、实现功能：
    将mysql的entry增量数据，同步到数据库的format中
三、运行方法：
    a)定时任务
        配置:crontab -e
        每天23点(绝对路径):0 23 * * * /usr/local/python3/bin/python3 -W ignore /usr/local/kidney/main.py  >> /var/log/main-20220112.log 2>&1
        重启服务:service crond restart
        查看运行状态:cat /var/log/cron
        列出所有任务:crontab -l
        列出某一个用户任务:crontab -l -u username
        删除全部任务:crontab -r
        删除某一个用户任务:crontab -r -u username
    b) 日志位置(循环读取)
        tail /var/log/main-20220112.log
"""
import time

import pandas as pd
from contextlib import contextmanager
import regex
import pymysql as mysqldb

from config import *
from dict import data_process_dict
from dict import extract_rule_dict
from dict import extract_rule_dict_operate
from dict import computer_rule_dict
from Struct import Struct_data


@contextmanager
def get_mysql_conn(**kwargs):
    """
    建立MySQL数据库连接
    :param kwargs:
    :return:
    """
    conn = mysqldb.connect(host=kwargs.get('host', 'localhost'), user=kwargs.get('user'),
                           passwd=kwargs.get('password'), port=kwargs.get('port', 3306),
                           database=kwargs.get('database'), charset=kwargs.get('charset'))
    try:
        yield conn
    finally:
        if conn:
            conn.close()


def execute_mysql_select_sql(conn, sql):
    """
    执行mysql的select类型语句
    :param conn:
    :param sql:
    :param table_name:
    :return:
    """
    with conn as cur:
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            column = cur.description
            column_name = [column[i][0] for i in range(len(column))]
            frame = pd.DataFrame(list(rows), columns=column_name)
        except Exception as e:
            frame = pd.DataFrame()
            print(e)
    return frame


def execute_mysql_sql(conn, sql):
    """
    执行mysql的dml和ddl语句，不包括select语句
    :param conn:
    :param sql:
    :return:
    """
    if sql == 'NULL':
        pass
    else:
        with conn as cur:  # cursor will now auto-commit
            try:
                cur.execute(sql)
            except Exception as e:
                conn.rollback()
                print(e)


def get_mysql_entry_data(conn, table_name, field='NULL'):
    """
    获取mysql的entry数据
    :param conn:
    :param table_name:
    :param field:
    :return:
    """
    if table_name == 'cdr_patient_info':  # 插入全部就诊信息表
        sql = "SELECT distinct concat_ws(';', a.kh, a.yljgdm, a.klx) as empi, a.kh as patient_no, a.xm as " \
              "patient_name, CASE WHEN a.xb = '1' THEN '男' WHEN a.xb = '2' THEN '女' ELSE '未知' END as sex, NULL as " \
              "age, a.zjhm as id_number, a.kh as card_no, a.klx as card_type, d.cisid as inpatient_number, d.rysj as " \
              "in_hospital_date_time, b.inpatient_primary_diagnose, c.operate_date, e.mzh as outpatient_number, " \
              "e.jzksrq as visit_date_time, e.jzzdsm as outpatient_primary_diagnose, a.yljgdm as hospital_code, " \
              "g.name as hospital_name, FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date " \
              "FROM entry_data_patient_info a " \
              "left join (SELECT yljgdm, kh, klx, max(sskssj) as operate_date FROM entry_data_operation_detail " \
              "WHERE mzzybz = '2' GROUP BY yljgdm, kh, klx) c " \
              "on a.yljgdm = c.yljgdm and a.kh = c.kh and a.klx = c.klx " \
              "left join (SELECT * FROM (SELECT @rn:= CASE WHEN @yljgdm = yljgdm AND @kh = kh AND @klx = klx " \
              "THEN @rn+1 ELSE 1 END AS rn, @yljgdm:= yljgdm AS yljgdm, @kh:= kh AS kh, @klx:= klx AS klx, jzlsh, " \
              "cisid, rysj FROM (SELECT * FROM entry_data_zy_medical_record ORDER BY yljgdm, kh, klx, rysj DESC) aa, " \
              "(SELECT @rn:=0, @yljgdm:=0, @kh:=0, @klx:=0) bb) cc WHERE cc.rn = 1) d " \
              "on a.yljgdm = d.yljgdm and a.kh = d.kh and a.klx = d.klx " \
              "left join (SELECT yljgdm, jzlsh, GROUP_CONCAT(zdsm SEPARATOR '；') as inpatient_primary_diagnose " \
              "FROM entry_data_ih_diagnosis_detail WHERE cyzdbz = '1' GROUP BY yljgdm, jzlsh) b " \
              "on d.yljgdm = b.yljgdm and d.jzlsh = b.jzlsh " \
              "left join (SELECT * FROM (SELECT @rn:= CASE WHEN @yljgdm = yljgdm AND @kh = kh AND @klx = klx " \
              "THEN @rn+1 ELSE 1 END AS rn, @yljgdm:= yljgdm AS yljgdm, @kh:= kh AS kh, @klx:= klx AS klx, mzh, " \
              "jzksrq, jzzdsm FROM (SELECT * FROM entry_data_mz_medical_record ORDER BY yljgdm, kh, klx, jzksrq " \
              "DESC) aa, (SELECT @rn:=0, @yljgdm:=0, @kh:=0, @klx:=0) bb) cc WHERE cc.rn = 1) e " \
              "on a.yljgdm = e.yljgdm and a.kh = e.kh and a.klx = e.klx " \
              "left join dic_hospital g on a.yljgdm = g.code " \
              "WHERE concat_ws(';', a.kh, a.yljgdm, a.klx) not in (select n.empi from cdr_patient_info n)"
    elif table_name == 'form_electrocardio_info':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, c.klx) as empi, a.kh as patient_no, a.jzlsh as " \
              "encounter_id, c.cisid as inpatient_number, NULL AS outpatient_number, CASE WHEN a.examtype = '11' " \
              "THEN '心电检查' ELSE NULL end as report_name, a.patientid as report_id, a.jcsj as ts_exam, a.jcmc as " \
              "exam_name, a.bt2nr as exam_find, a.bt1nr as exam_conclusion, b.zjhm as id_number, a.yljgdm as " \
              "hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date FROM " \
              "(SELECT yljgdm, kh, jzlsh, patientid, jcsj, examtype, jcmc, bt1nr, bt2nr from " \
              "entry_data_ris_report2 where id in (select t.max_id from (select max(id) as max_id from " \
              "entry_data_ris_report2 where process_status = 's' group by studyuid, instanceuid, yljgdm, jzlsh) as " \
              "t) and examtype = '11') a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh " \
              "JOIN entry_data_zy_medical_record c ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    elif table_name in list(lab_info.keys()):
        sql = lis_sql % (lab_info[table_name][1], ("','".join(str(x) for x in lab_info[table_name][0])))
    elif table_name == 'form_lis_follow':
        sql = lis_sql_follow
    elif table_name == 'form_medication_orders':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, c.klx) as empi, a.kh as patient_no, a.jzlsh as " \
              "encounter_id, c.cisid AS inpatient_number, NULL AS outpatient_number, c.rysj AS in_hospital_time, " \
              "c.jzksmc AS inhospital_dept_name, 6 AS question_id, '1' AS medication_type, a.yzid as " \
              "master_orders_id, a.yzlb as orders_attribute_id, CASE WHEN a.yzlb = '1' THEN '长期（在院）' WHEN a.yzlb " \
              "= '2' THEN '临时（在院）' WHEN a.yzlb = '3' THEN '出院带药' ELSE '其他' END as orders_attribute_name, " \
              "NULL AS orders_type_code, NULL AS orders_type_name, NULL AS project_type_code, NULL AS " \
              "project_type_name, a.yzmxbm as orders_code, a.yzmxmc as origin_name, a.ypgg as specifications, a.jl " \
              "as dosage, a.dw as dosage_unit, a.mcsl as project_number, a.mcdw as project_number_unit, a.yypddm " \
              "as frequency, a.yypd as pc, a.yf as pathway, NULL as write_recipe_time, a.yzzxsj AS " \
              "orders_start_time, a.yzzzsj AS stop_time, b.zjhm as id_number, a.yljgdm as hospital_code, " \
              "FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date FROM " \
              "(SELECT * from entry_data_cis_dradvice_detail where (yzmxmc LIKE '%索坦%' OR yzmxmc LIKE '%维全特%' OR " \
              "yzmxmc LIKE '%英立达%' OR yzmxmc LIKE '%Cabometyx%' OR yzmxmc LIKE '%乐卫玛%' OR yzmxmc LIKE '%多吉美%' " \
              "OR yzmxmc LIKE '%飞尼妥%' OR yzmxmc LIKE '%福可维%' OR yzmxmc LIKE '%Welireg%' OR yzmxmc LIKE '%拓益%' " \
              "OR yzmxmc LIKE '%百泽安%' OR yzmxmc LIKE '%艾瑞卡%' OR yzmxmc LIKE '%达伯舒%' OR yzmxmc LIKE '%可瑞达%' " \
              "OR yzmxmc LIKE '%欧狄沃%' OR yzmxmc LIKE '%逸沃%') and yzzxsj >= '2019-12-01' and id in (select " \
              "t.max_id from (select max(id) as max_id from entry_data_cis_dradvice_detail where process_status = " \
              "'s' group by yljgdm, yzid, jzlsh) as t)) a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh " \
              "JOIN entry_data_zy_medical_record c " \
              "ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    elif table_name == 'form_medication_orders1':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, a.klx) as empi, a.kh as patient_no, a.jzlsh as " \
              "encounter_id, NULL AS inpatient_number, NULL AS outpatient_number, c.jzksrq AS in_hospital_time, " \
              "c.jzksmc AS inhospital_dept_name, 6 AS question_id, '2' AS medication_type, a.cfmxh as " \
              "master_orders_id, NULL as orders_attribute_id, NULL as orders_attribute_name, NULL AS " \
              "orders_type_code, NULL AS orders_type_name, NULL AS project_type_code, NULL AS project_type_name, " \
              "a.xmbm as orders_code, a.xmmc as origin_name, a.ypgg as specifications, a.jl as dosage, a.dw as " \
              "dosage_unit, a.mcsl as project_number, a.mcdw as project_number_unit, a.sypcdm as frequency, a.sypc " \
              "as pc, a.yf as pathway, a.kfrq as write_recipe_time, NULL AS orders_start_time, NULL AS stop_time, " \
              "b.zjhm as id_number, a.yljgdm as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') " \
              "as create_date FROM " \
              "(SELECT * from entry_data_cis_prescription_detail where (xmmc LIKE '%索坦%' OR xmmc LIKE '%维全特%' OR " \
              "xmmc LIKE '%英立达%' OR xmmc LIKE '%Cabometyx%' OR xmmc LIKE '%乐卫玛%' OR xmmc LIKE '%多吉美%' OR xmmc " \
              "LIKE '%飞尼妥%' OR xmmc LIKE '%福可维%' OR xmmc LIKE '%Welireg%' OR xmmc LIKE '%拓益%' OR xmmc LIKE " \
              "'%百泽安%' OR xmmc LIKE '%艾瑞卡%' OR xmmc LIKE '%达伯舒%' OR xmmc LIKE '%可瑞达%' OR xmmc LIKE '%欧狄沃%' " \
              "OR xmmc LIKE '%逸沃%') and kfrq >= '2019-12-01' and id in (select t.max_id from " \
              "(select max(id) as max_id from entry_data_cis_prescription_detail where process_status = 's' group by " \
              "cyh, cfmxh, yljgdm) as t)) a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh AND a.klx = b.klx " \
              "JOIN entry_data_mz_medical_record c " \
              "ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    elif table_name == 'form_operate_pathology_info':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, a.klx) as empi, NULL AS empi_old, ssmxlsh, a.kh as " \
              "patient_no, a.jzlsh AS encounter_id, d.cisid AS inpatient_number, NULL AS outpatient_number, sskssj " \
              "as operate_date, ssczbm as operate_code, ssczmc as operate_name, NULL AS operate_process, CASE WHEN " \
              "a.ssczmc LIKE '%腹腔镜%' THEN '腹腔镜手术' WHEN a.ssczmc LIKE '%达芬奇下%' THEN '机器人辅助手术' ELSE " \
              "'开放手术' end as sslx, CASE WHEN a.ssczmc LIKE '%肾病损切除术%' or a.ssczmc LIKE '%肾部分切除术%' or " \
              "a.ssczmc LIKE '%部分肾切除术%' THEN '肾部分切除术' WHEN a.ssczmc LIKE '%肾根治切除术%' or a.ssczmc LIKE " \
              "'%单侧肾切除术%' THEN '根治性肾切除术' WHEN a.ssczmc LIKE '%肾病损射频消融术%' THEN '微波辅助肿瘤剜除术' WHEN " \
              "a.ssczmc LIKE '%下腔静脉取栓术%' or a.ssczmc LIKE '%上腔静脉取栓术%' or a.ssczmc LIKE '%肾静脉切开取栓术%' " \
              "or a.ssczmc LIKE '%肾动脉切开取栓术%' THEN '瘤栓取出术' ELSE NULL end as ssfs, b.patientid as report_id, " \
              "b.jcsj as ts_exam, CASE WHEN b.examtype = '07' THEN '病理检查' ELSE NULL end as report_name, b.bt2nr " \
              "as exam_find, b.bt1nr as exam_conclusion, c.zjhm as id_number, a.yljgdm as hospital_code, " \
              "FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date FROM " \
              "(SELECT yljgdm, kh, klx, jzlsh, ssmxlsh, sskssj, ssczbm, ssczmc from entry_data_operation_detail " \
              "where id in (select t.max_id from (select max(id) as max_id from entry_data_operation_detail where " \
              "process_status = 's' group by yljgdm, ssmxlsh) as t) and ssczmc in ('腹腔镜下肾部分切除术', " \
              "'腹腔镜下肾部分切除术', '腹腔镜下肾癌根治术', '肾部分切除术', '腹腔镜下肾切除术', '腹腔镜下肾肿瘤射频消融', " \
              "'肾癌根治术', '腹腔镜下单侧肾切除术', '腹腔镜下肾肿瘤射频消融术', '单侧肾切除术', '腹腔镜下肾癌根治术', " \
              "'腹腔镜下肾根治切除术', '达芬奇下肾部分切除术', '腹腔镜下肾脏肿瘤切除术', '达芬奇下肾部分切除术', " \
              "'腹腔镜下肾病损射频消融术', '腹腔镜下肾肿瘤射频消融', '达芬奇下肾部分切除术', '肾肿瘤射频消融术', '肾切除术单侧', " \
              "'肾癌根治术', '腹腔镜下肾根治切除术术', '肾部分切除术', '肾病损切除术', '腹腔镜下肾周粘连松解术', '部分肾切除术', " \
              "'肾根治切除术', '腹腔镜下肾(肿瘤)消融术', '肾肿瘤切除术', '达芬奇下肾癌根治术', '腹腔镜下肾脏肿瘤切除术', " \
              "'肾静脉取栓术', '腹腔镜下肾病损切除术', '达芬奇下单侧肾切除术', '腹腔镜下肾根治性切除术', '肾肿瘤射频消融术', " \
              "'下腔静脉取栓术', '肾病损射频消融术', 'B超引导下经皮肾肿瘤微波消融术', '肾病损切除术', '双侧肾切除术', " \
              "'肾周围粘连松解术', '超声造影下肾肿瘤穿刺术', '达芬奇下肾癌根治术', '腹腔镜下肾病损切除术', '肾静脉切开取栓术', " \
              "'肾根治切除术', '肾门淋巴结清扫术', '开放性肾活组织检查', '肾周围活检术', '腹腔镜下肾周或输尿管周围粘连的松解术', " \
              "'肾探查术', '超声引导下肾病损射频消融术', '肾肿瘤切除术', '肾周围粘连松解', '腹腔镜下肾肿瘤冷冻消融术', " \
              "'腹腔镜下肾探查术', '达芬奇下肾病损切除术', '肾病损或组织的其他和未特指切除', '腹腔镜下肾切除术', '下腔静脉病损切除术', " \
              "'残留肾切除术', '肾周活组织检查', '肾周病损切除术', '腹腔镜中转开放肾癌根治术', '肾病损或组织的经皮消融术', " \
              "'双侧肾切除术')) a LEFT JOIN " \
              "(SELECT yljgdm, kh, jzlsh, patientid, jcsj, examtype, jcmc, bt1nr, bt2nr from " \
              "entry_data_ris_report2 where id in (select t.max_id from (select max(id) as max_id from " \
              "entry_data_ris_report2 group by studyuid, instanceuid, yljgdm, jzlsh) as t) and examtype = '07') b " \
              "ON a.yljgdm = b.yljgdm AND a.kh = b.kh AND a.jzlsh = b.jzlsh " \
              "LEFT JOIN entry_data_patient_info c ON a.yljgdm = c.yljgdm AND a.kh = c.kh AND a.klx = c.klx " \
              "JOIN entry_data_zy_medical_record d " \
              "ON a.yljgdm = d.yljgdm AND a.jzlsh = d.jzlsh " \
              "WHERE b.jcsj is NULL OR a.sskssj < b.jcsj"
    elif table_name == 'form_outpatient_visit':
        sql = outpatient_sql
    elif table_name == 'form_outpatient_visit1':
        sql = outpatient_sql1
    elif table_name == 'form_pathology_info':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, c.klx) as empi, a.kh as patient_no, a.patientid as " \
              "patient_image_id, a.jcsj as ts_exam, CASE WHEN a.examtype = '07' THEN '病理检查' ELSE NULL end as " \
              "report_name, a.jcbw as body_site, a.bt2nr as exam_find, a.bt1nr as exam_conclusion, b.zjhm as " \
              "id_number, a.yljgdm as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as " \
              "create_date FROM " \
              "(SELECT yljgdm, kh, jzlsh, patientid, jcsj, examtype, jcmc, jcbw, bt1nr, bt2nr from " \
              "entry_data_ris_report2 where id in (select t.max_id from (select max(id) as max_id from " \
              "entry_data_ris_report2 where process_status = 'sy' group by studyuid, instanceuid, yljgdm, jzlsh) as " \
              "t) and examtype = '07') a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh " \
              "JOIN entry_data_zy_medical_record c ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    elif table_name == 'form_radiology_info':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, c.klx) as empi, a.kh as patient_no, a.jzlsh as " \
              "encounter_id, c.cisid AS inpatient_number, NULL AS outpatient_number, CASE WHEN a.examtype = 11 THEN " \
              "'心电检查' ELSE NULL end as report_name, NULL as jclx, a.jcsj as ts_exam, a.jcmc as exam_name, a.jcbw " \
              "as body_site, CASE WHEN a.jcbw LIKE '%肾脏%' THEN '肾脏' WHEN a.jcbw LIKE '%输尿管%' THEN '输尿管' WHEN " \
              "a.jcbw LIKE '%上腹部%' THEN '上腹部' WHEN a.jcbw LIKE '%CTU%' THEN 'CTU' WHEN a.jcbw LIKE '%肾CTA%' " \
              "THEN '肾CTA' WHEN a.jcbw LIKE '%肾MR%' THEN '肾MR' WHEN a.jcbw LIKE '%MRU%' THEN 'MRU' WHEN a.jcbw " \
              "LIKE '%胸部%' THEN '胸部' ELSE '其他' end as jcbw, a.jcff as exam_method, NULL AS exam_tech, a.yxbx as " \
              "exam_find, a.yxzd as exam_conclusion, a.patientid as report_iD, b.zjhm as id_number, a.yljgdm as " \
              "hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date FROM " \
              "(SELECT yljgdm, kh, jzlsh, patientid, jcsj, examtype, jcmc, jcbw, jcff, yxbx, yxzd from " \
              "entry_data_ris_report where id in (select t.max_id from (select max(id) as max_id from " \
              "entry_data_ris_report where process_status = 's' group by studyuid, instanceuid, yljgdm, jzlsh) as " \
              "t) and examtype = 11) a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh " \
              "JOIN entry_data_zy_medical_record c ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    elif table_name == 'form_radiotherapy_info':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, c.klx) as empi, a.kh as patient_no, a.jzlsh as " \
              "encounter_id, c.cisid AS inpatient_number, NULL AS outpatient_number, a.patientid as report_id, CASE " \
              "WHEN a.examtype = '09' THEN '核医学检查' ELSE NULL end as report_name, a.jcmc as exam_name, a.jcsj as " \
              "ts_exam, a.yxzd as exam_conclusion, NULL AS report_class_name, CASE WHEN a.jcmc = 'PET/CT全身检查' " \
              "THEN 'PET/CT' WHEN a.jcmc in ('SPECT显像检查', 'SPECT显像检查(核医学)', '骨全身断层显像', '骨全身显像') THEN " \
              "'ECT' WHEN a.jcmc in ('肾图+肾小球滤过率（GFR）显像', '肾图+肾有效血浆流量（ERPF）显像') THEN 'GFR' ELSE NULL " \
              "end as jcxm, b.zjhm as id_number, a.yljgdm as hospital_code, " \
              "FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date FROM " \
              "(SELECT yljgdm, kh, jzlsh, patientid, jcsj, examtype, jcmc, yxzd from " \
              "entry_data_ris_report where id in (select t.max_id from (select max(id) as max_id from " \
              "entry_data_ris_report where process_status = 'sy' group by studyuid, instanceuid, yljgdm, jzlsh) as " \
              "t) and examtype = '09') a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh " \
              "JOIN entry_data_zy_medical_record c ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    elif table_name == 'form_ris_follow':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, c.klx) as empi, a.kh as patient_no, a.jzlsh as " \
              "encounter_id, NULL AS inpatient_number, NULL AS outpatient_number, a.mzzybz as encounter_type, CASE " \
              "WHEN a.examtype = '06' THEN 8 WHEN a.examtype in ('01', '02', '03', '04', '05') THEN 9 WHEN " \
              "a.examtype = '09' THEN 10 WHEN a.examtype = '11' THEN 11 ELSE NULL END as question_id, NULL as " \
              "report_id, a.bglczd as diagnosis, a.patientid as patient_imageid, CASE WHEN a.examtype = '01' THEN " \
              "'计算机X线断层摄影' WHEN a.examtype = '02' THEN '核磁共振成像' WHEN a.examtype = '03' THEN '数字减影血管造影' " \
              "WHEN a.examtype = '04' THEN '普通X光摄影' WHEN a.examtype = '05' THEN '特殊X光摄影' WHEN a.examtype = " \
              "'09' THEN '核医学检查' WHEN a.examtype = '10' THEN '其他检查' ELSE NULL end as report_name, NULL as " \
              "exam_operator, a.jcsj as ts_exam, a.jcbw as body_site, a.jcmc as exam_name, NULL AS exam_tech, a.jcff " \
              "AS exam_method, a.yxbx as exam_find, a.yxzd as exam_conclusion, NULL AS report_class_name, CASE " \
              "WHEN a.examtype IN ('01', '02', '03', '04', '05') AND a.jcmc LIKE '%CT%' AND a.jcmc not LIKE '%CTU%' " \
              "THEN 'CT' WHEN a.examtype IN ('01', '02', '03', '04', '05') AND a.jcmc LIKE '%MR%' THEN 'MR' WHEN " \
              "a.examtype NOT IN ('01', '02', '03', '04', '05') THEN NULL ELSE '其他' end as jclx, CASE WHEN " \
              "a.examtype = '09' AND a.jcmc = 'PET/CT全身检查' THEN 'PET/CT' WHEN a.examtype = '09' AND a.jcmc in " \
              "('SPECT显像检查', 'SPECT显像检查(核医学)', '骨全身断层显像', '骨全身显像') THEN 'ECT' WHEN a.examtype = '09' " \
              "AND a.jcmc in ('肾图+肾小球滤过率（GFR）显像', '肾图+肾有效血浆流量（ERPF）显像') THEN 'GFR' ELSE NULL end as " \
              "jcxm, CASE WHEN a.examtype IN ('01', '02', '03', '04', '05') AND a.jcbw LIKE '%肾脏%' THEN '肾脏' WHEN " \
              "a.examtype IN ('01', '02', '03', '04', '05') AND a.jcbw LIKE '%输尿管%' THEN '输尿管' WHEN a.examtype " \
              "IN ('01', '02', '03', '04', '05') AND a.jcbw LIKE '%上腹部%' THEN '上腹部' WHEN a.examtype IN " \
              "('01', '02', '03', '04', '05') AND a.jcbw LIKE '%CTU%' THEN 'CTU' WHEN a.examtype IN " \
              "('01', '02', '03', '04', '05') AND a.jcbw LIKE '%肾CTA%' THEN '肾CTA' WHEN a.examtype IN " \
              "('01', '02', '03', '04', '05') AND a.jcbw LIKE '%肾MR%' THEN '肾MR' WHEN a.examtype IN " \
              "('01', '02', '03', '04', '05') AND a.jcbw LIKE '%MRU%' THEN 'MRU' WHEN a.examtype IN " \
              "('01', '02', '03', '04', '05') AND a.jcbw LIKE '%胸部%' THEN '胸部' WHEN a.examtype NOT IN " \
              "('01', '02', '03', '04', '05') THEN NULL ELSE '其他' end as jcbw, NULL as jcbw_qt, b.zjhm as " \
              "id_number, a.yljgdm as hospital_code, " \
              "FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date FROM " \
              "(SELECT * from entry_data_ris_report where jcsj >= '2019-12-01' and id in (select t.max_id from " \
              "(select max(id) as max_id from entry_data_ris_report where process_status = 'sy1' group by studyuid, " \
              "instanceuid, yljgdm, jzlsh) as t)) a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh " \
              "JOIN entry_data_mz_medical_record c ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    elif table_name == 'form_ris_follow1':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, c.klx) as empi, a.kh as patient_no, a.jzlsh as " \
              "encounter_id, NULL AS inpatient_number, NULL AS outpatient_number, a.mzzybz as encounter_type, CASE " \
              "WHEN a.examtype = '06' THEN 8 WHEN a.examtype in ('01', '02', '03', '04', '05') THEN 9 WHEN " \
              "a.examtype = '09' THEN 10 WHEN a.examtype = '11' THEN 11 ELSE NULL END as question_id, NULL as " \
              "report_id, NULL as diagnosis, a.patientid as patient_imageid, CASE WHEN a.examtype = '06' THEN " \
              "'超声检查' WHEN a.examtype = '07' THEN '病理检查' WHEN a.examtype = '08' THEN '內窥镜检查' WHEN a.examtype " \
              "= '10' THEN '其他检查' WHEN a.examtype = '11' THEN '心电检查' ELSE NULL end as report_name, NULL as " \
              "exam_operator, a.jcsj as ts_exam, a.jcbw as body_site, a.jcmc as exam_name, NULL AS exam_tech, a.jcff " \
              "AS exam_method, a.bt2nr as exam_find, a.bt1nr as exam_conclusion, NULL AS report_class_name, NULL as " \
              "jclx, NULL as jcxm, NULL as jcbw, NULL as jcbw_qt, b.zjhm as id_number, a.yljgdm as hospital_code, " \
              "FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date FROM " \
              "(SELECT * from entry_data_ris_report2 where jcsj >= '2019-12-01' and examtype != '06' and id in " \
              "(select t.max_id from (select max(id) as max_id from entry_data_ris_report2 where process_status = " \
              "'sy1' group by studyuid, instanceuid, yljgdm, jzlsh) as t)) a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh " \
              "JOIN entry_data_mz_medical_record c ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    elif table_name == 'form_ris_follow_ultra':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, c.klx) as empi, a.kh as patient_no, a.jzlsh as " \
              "encounter_id, NULL AS inpatient_number, NULL AS outpatient_number, a.mzzybz as encounter_type, CASE " \
              "WHEN a.examtype = '06' THEN 8 WHEN a.examtype in ('01', '02', '03', '04', '05') THEN 9 WHEN " \
              "a.examtype = '09' THEN 10 WHEN a.examtype = '11' THEN 11 ELSE NULL END as question_id, NULL as " \
              "report_id, NULL as diagnosis, a.patientid as patient_imageid, '超声检查' as report_name, NULL as " \
              "exam_operator, a.jcsj as ts_exam, a.jcbw as body_site, a.jcmc as exam_name, NULL AS exam_tech, a.jcff " \
              "AS exam_method, a.bt2nr as exam_find, a.bt1nr as exam_conclusion, NULL AS report_class_name, NULL as " \
              "jclx, CASE WHEN a.examtype IN ('06') AND a.jcbw like '%输尿管%' THEN '输尿管' WHEN a.examtype IN ('06') " \
              "AND a.jcbw like '%后腹膜淋巴结%' THEN '后腹膜淋巴结' WHEN a.examtype IN ('06') AND a.jcbw like '%肾静脉%' " \
              "THEN '肾静脉' ELSE NULL end as jcxm, b.zjhm as id_number, a.yljgdm as " \
              "hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date FROM " \
              "(SELECT * from entry_data_ris_report2 where jcsj >= '2019-12-01' and examtype = '06' and id in " \
              "(select t.max_id from (select max(id) as max_id from entry_data_ris_report2 where process_status = " \
              "'sy2' group by studyuid, instanceuid, yljgdm, jzlsh) as t)) a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh " \
              "JOIN entry_data_mz_medical_record c ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    elif table_name == 'form_scale_follow':
        sql = "SELECT * FROM (SELECT @rn:= CASE WHEN @empi = empi AND @test_item_name = test_item_name THEN @rn+1 " \
              "ELSE 1 END AS rn, @empi:= empi AS empi, @test_item_name:= test_item_name AS test_item_name, " \
              "patient_no, encounter_id, inpatient_number, outpatient_number, encounter_type, lab_generic_id, " \
              "ts_test, ts_draw, reportname, test_item_code, print_value, result_value, result_unit, reference_text, " \
              "abnormal_flag, abnormal_flag_name, id_number, hospital_code, create_date FROM " \
              "(SELECT concat_ws(';', b.kh, b.yljgdm, d.klx) AS empi, b.kh AS patient_no, b.jzlsh AS encounter_id, " \
              "NULL AS inpatient_number, NULL AS outpatient_number, b.mzzybz AS encounter_type, a.jyzblsh AS " \
              "lab_generic_id, b.jyrq AS ts_test, b.cjrq AS ts_draw, b.bgdlb as reportname, a.jczbdm AS " \
              "test_item_code, CASE WHEN a.jczbmc IN ('钙', '钙（全血）', '钙(全血)', '钙离子') THEN '血清校正钙' WHEN " \
              "a.jczbmc IN ('嗜中性粒细胞绝对值', '嗜中性细胞绝对值') THEN '中性粒细胞计数绝对值' WHEN a.jczbmc = '血小板计数' " \
              "THEN '血小板计数绝对值' WHEN a.jczbmc = '乳酸脱氢酶' THEN '乳酸脱氢酶LDH' ELSE a.jczbmc END AS " \
              "test_item_name, a.jczbjg AS print_value, a.jczbjg AS result_value, a.jldw AS result_unit, a.ckz AS " \
              "reference_text, a.ycts AS abnormal_flag, IFNULL(a.ycts, 'NA') AS abnormal_flag_name, c.zjhm AS " \
              "id_number, a.yljgdm AS hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'%%Y-%%m-%%d %%H:%%i:%%s') AS " \
              "create_date FROM (SELECT * FROM entry_data_lis_indicator WHERE id IN (SELECT t.max_id FROM (SELECT " \
              "MAX(id) AS max_id FROM entry_data_lis_indicator GROUP BY jyzblsh, yljgdm, jczbdm) AS t)) a " \
              "JOIN (SELECT * FROM entry_data_lis_report WHERE id IN (SELECT t.max_id FROM (SELECT MAX(id) AS max_id " \
              "FROM entry_data_lis_report GROUP BY yljgdm, bgdh, bgrq, jzlsh) AS t)) b " \
              "ON a.yljgdm = b.yljgdm AND a.bgdh = b.bgdh AND a.bgrq = b.bgrq " \
              "LEFT JOIN entry_data_patient_info c ON b.yljgdm = c.yljgdm AND b.kh = c.kh " \
              "JOIN entry_data_mz_medical_record d ON b.yljgdm = d.yljgdm AND b.jzlsh = d.jzlsh " \
              "WHERE ((a.jczbmc = '血红蛋白' or a.jczbmc = '嗜中性粒细胞绝对值' or a.jczbmc = '嗜中性细胞绝对值' or a.jczbmc " \
              "= '血小板计数') and b.bbmc LIKE '%血%') or a.jczbmc = '血钙' or a.jczbmc = '钙' or a.jczbmc = '钙（全血）' " \
              "or a.jczbmc = '钙(全血)' or a.jczbmc = '钙离子' or a.jczbmc = '乳酸脱氢酶' ORDER BY b.kh, b.yljgdm, " \
              "d.klx, a.jczbmc, b.cjrq) aa, (SELECT @rn:=0, @empi:=0, @test_item_name:=0) bb) cc WHERE cc.rn = 1 AND " \
              "concat(cc.empi, cc.test_item_name) not in (SELECT concat(empi, test_item_name) from form_scale_follow)"
    elif table_name == 'form_ultrasound_info':
        sql = "SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, c.klx) as empi, a.kh as patient_no, a.jzlsh as " \
              "encounter_id, c.cisid AS inpatient_number, NULL AS outpatient_number, a.patientid as report_id, " \
              "a.jcmc as exam_name, CASE WHEN a.examtype = '06' THEN '超声检查' ELSE NULL end as report_name, a.jcbw " \
              "as body_site, a.jcsj as ts_exam, a.bt2nr as exam_find, a.bt1nr as exam_conclusion, CASE WHEN a.jcbw " \
              "like '%输尿管%' THEN '输尿管' WHEN a.jcbw like '%后腹膜淋巴结%' THEN '后腹膜淋巴结' WHEN a.jcbw like " \
              "'%肾静脉%' THEN '肾静脉' ELSE NULL end as jcxm, b.zjhm as id_number, a.yljgdm as hospital_code, " \
              "FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%m-%d %H:%i:%s') as create_date FROM " \
              "(SELECT yljgdm, kh, jzlsh, patientid, jcsj, examtype, jcmc, jcbw, bt1nr, bt2nr from " \
              "entry_data_ris_report2 where id in (select t.max_id from (select max(id) as max_id from " \
              "entry_data_ris_report2 where process_status = 'sy3' group by studyuid, instanceuid, yljgdm, jzlsh) as " \
              "t) and examtype = '06') a " \
              "LEFT JOIN entry_data_patient_info b ON a.yljgdm = b.yljgdm AND a.kh = b.kh " \
              "JOIN entry_data_zy_medical_record c ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh"
    else:
        pass

    mysql_entry_data_rows = execute_mysql_select_sql(conn, sql)
    return mysql_entry_data_rows


def table_to_dict(table_name, row_data):
    """
    生成fields,values字典
    :param table_name:
    :param row_data:
    :return:
    """
    dict_field_value = {}
    field_list = entry_tables[table_name]
    for i in field_list:
        dict_field_value[i] = row_data[i]
    return dict_field_value


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


def mysql_sync_to_mysql(mysql_conn, data, entry_name):
    """
    把mysql的entry数据同步到数据库format里面
    :param mysql_conn:
    :param data:
    :param entry_name:
    :return:
    """
    if entry_name == 'form_medication_orders1':
        entry_name = 'form_medication_orders'
        column_name = list(data.columns)
        column_name.insert(17, 'orders_name')
        data = [tuple(row) for row in data.values]
        data = update_order_name(data)
        data = pd.DataFrame(list(data), columns=column_name)
    elif entry_name == 'form_operate_pathology_info':    # 手术病理表取每次手术后最近的一次病理报告
        data.sort_values(['hospital_code', 'patient_no', 'encounter_id', 'ssmxlsh', 'ts_exam'],
                         ascending=[1, 1, 1, 1, 1], inplace=True)
        grouped = data.groupby(['hospital_code', 'patient_no', 'encounter_id', 'ssmxlsh']).head(1)
        data = grouped.reset_index(drop=True)
        data.drop('ssmxlsh', axis=1, inplace=True)
        column_name = list(data.columns)
        data = [tuple(row) for row in data.values]
        data = struct_data(data)
        data = pd.DataFrame(list(data), columns=column_name)
    elif entry_name == 'form_outpatient_visit1':
        entry_name = 'form_outpatient_visit'
    elif entry_name == 'form_ris_follow1':
        entry_name = 'form_ris_follow'
    elif entry_name == 'form_ris_follow_ultra':
        entry_name = 'form_ris_follow'
        column_name = list(data.columns)
        column_name.insert(22, 'jcbw')
        column_name.insert(23, 'jcbw_qt')
        data_tuple = [tuple(row) for row in data.values]
        data_tuple = extract(data_tuple, 13, 22)  # 处理检查部位
        data = pd.DataFrame(list(data_tuple), columns=column_name)
    elif entry_name == 'form_ultrasound_info':  # 检查部位
        column_name = list(data.columns)
        column_name.insert(13, 'jcbw')
        column_name.insert(14, 'jcbw_qt')
        data_tuple = [tuple(row) for row in data.values]
        data_tuple = extract(data_tuple, 8, 13)  # 处理检查部位
        data = pd.DataFrame(list(data_tuple), columns=column_name)
    else:
        pass

    for index, row in data.iterrows():

        # 向form插入数据
        data_dict = table_to_dict(entry_name, row)    # column、value字典
        fields, values = zip(*list(data_dict.items()))    # tuple
        values = ['NULL' if x is None or x == '' else x for x in values]    # 替换dataframe中None为NULL,插入数据库为NULL

        insert_data = \
            "insert into {}({}) values ({})".format(entry_name, ",".join(str(x) for x in fields),
                                                    ",".join("'{}'".format(mysqldb.escape_string(x))
                                                             if x != 'NULL' and type(x) is str
                                                             else "{}".format(x) if x == 'NULL' else "'{}'".format(x)
                                                             for x in values))
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('###############第{}条{}记录###############'.format(index+1, entry_name))
        try:
            execute_mysql_sql(mysql_conn, insert_data)    # 执行插入数据
        except Exception as e:
            mysql_conn.rollback()
            print(e)

    if entry_name in ['form_electrocardio_info']:
        marked_sql = "update {} set {} = 'sy' where {} = 's'".format('entry_data_ris_report2', status, status)
    elif entry_name in list(lab_info.keys()):
        marked_sql = "update {} set {} = '{}' where {} = '{}'".format('entry_data_lis_indicator', status,
                                                                      lab_info[entry_name][2], status,
                                                                      lab_info[entry_name][1])
    elif entry_name in ['form_lis_follow']:
        marked_sql = "update {} set {} = 'syed' where {} = 'sy10'".format('entry_data_lis_indicator', status, status)
    elif entry_name in ['form_medication_orders']:
        marked_sql = "update {} set {} = 'syed' where {} = 's'".format('entry_data_cis_dradvice_detail', status, status)
    elif entry_name in ['form_medication_orders1']:
        marked_sql = "update {} set {} = 'syed' where {} = 's'".format('entry_data_cis_prescription_detail', status, status)
    elif entry_name in ['form_operate_pathology_info']:
        marked_sql = "update {} set {} = 'sy' where {} = 's'".format('entry_data_operation_detail', status, status)
    elif entry_name in ['form_pathology_info']:
        marked_sql = "update {} set {} = 'sy1' where {} = 'sy'".format('entry_data_ris_report2', status, status)
    elif entry_name in ['form_radiology_info']:
        marked_sql = "update {} set {} = 'sy' where {} = 's'".format('entry_data_ris_report', status, status)
    elif entry_name in ['form_radiotherapy_info']:
        marked_sql = "update {} set {} = 'sy1' where {} = 'sy'".format('entry_data_ris_report', status, status)
    elif entry_name in ['form_ris_follow']:
        marked_sql = "update {} set {} = 'syed' where {} = 'sy1'".format('entry_data_ris_report', status, status)
    elif entry_name in ['form_ris_follow1']:
        marked_sql = "update {} set {} = 'sy2' where {} = 'sy1'".format('entry_data_ris_report2', status, status)
    elif entry_name in ['form_ris_follow_ultra']:
        marked_sql = "update {} set {} = 'sy3' where {} = 'sy2'".format('entry_data_ris_report2', status, status)
    elif entry_name in ['form_ultrasound_info']:
        marked_sql = "update {} set {} = 'syed' where {} = 'sy3'".format('entry_data_ris_report2', status, status)
    else:
        marked_sql = "NULL"

    try:
        execute_mysql_sql(mysql_conn, marked_sql)    # 执行同步标记
    except Exception as e:
        mysql_conn.rollback()
        print(e)


# 数据插入完成之后生成qs_drz_patient、syz_sysjc、syz_zyyzmx、syz_ris、syz_ris2
def mysql_update_to_mysql(mysql_conn, update_sql, entry_name):
    """
    把mysql的format数据更新
    :param mysql_conn:
    :param update_sql:
    :param entry_name:
    :return:
    """
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('###############更新{}表记录###############'.format(entry_name))
    try:
        execute_mysql_sql(mysql_conn, update_sql)
        if entry_name in ['entry_data_zy_medical_record']:
            marked_sql = "update {} set {} = 'sy' where {} = 's'".format(entry_name, status, status)
        elif entry_name in ['entry_data_operation_detail']:
            marked_sql = "update {} set {} = 'syed' where {} = 'sy'".format(entry_name, status, status)
        else:  # entry_data_patient_info
            marked_sql = "update {} set {} = 'syed' where {} = 'sy'".format('entry_data_zy_medical_record', status, status)
        execute_mysql_sql(mysql_conn, marked_sql)    # 标记
    except Exception as e:
        mysql_conn.rollback()
        print(e)


def main():
    mysql_conn_args = dict(user=args['user'], host=args['server'], password=args['password'],
                           database=args['database'], charset=args['charset'])
    with get_mysql_conn(**mysql_conn_args) as mysql_conn:  # ensure that the connection is closed
        for key in entry_tables:  # for key in ['entry_data_patientinfo']:
            print(key)
            if key == 'form_electrocardio_info':
                upd = "update {} set {} = 's' where {} is NULL".format('entry_data_ris_report2', status, status)
            elif key == 'form_labinfo_biochemistry':
                upd = "update {} set {} = 's' where {} is NULL".format('entry_data_lis_indicator', status, status)
            elif key == 'form_medication_orders':
                upd = "update {} set {} = 's' where {} is NULL".format('entry_data_cis_dradvice_detail', status, status)
            elif key == 'form_medication_orders1':
                upd = "update {} set {} = 's' where {} is NULL".format('entry_data_cis_prescription_detail', status, status)
            elif key == 'form_operate_pathology_info':
                upd = "update {} set {} = 's' where {} is NULL".format('entry_data_operation_detail', status, status)
            elif key == 'form_radiology_info':
                upd = "update {} set {} = 's' where {} is NULL".format('entry_data_ris_report', status, status)
            else:
                upd = "NULL"

            try:
                execute_mysql_sql(mysql_conn, upd)
                mysql_data = get_mysql_entry_data(mysql_conn, table_name=key)
                mysql_sync_to_mysql(mysql_conn, mysql_data, entry_name=key)
            except Exception as e:
                mysql_conn.rollback()
                print(e)

        execute_mysql_sql(mysql_conn, upd_drz)  # 更新entry_data_zy_medical_record新增数据process_status状态
        mysql_update_to_mysql(mysql_conn, insert_drz, 'entry_data_zy_medical_record')  # 待入组
        execute_mysql_sql(mysql_conn, update_drz)  # 更新待入组patient_num字段
        mysql_update_to_mysql(mysql_conn, insert_drz_ope, 'entry_data_operation_detail')  # 待入组手术
        mysql_update_to_mysql(mysql_conn, insert_basic_info, 'entry_data_patient_info')  # 基本信息


if __name__ == '__main__':
    main()

