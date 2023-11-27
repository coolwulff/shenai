# 需要根据服务器地址修改
# 生产环境
# args = {'server': "172.28.17.239", 'user': "root", 'password': "Abcde_12345",
#         'database': "kidney_tumor_origin", 'charset': 'utf8'}
# args = {'server': "172.28.17.239", 'user': "root", 'password': "Abcde_12345",
#         'database': "kidney_tumor_test", 'charset': 'utf8'}
# 公司环境
# args = {'server': "10.1.192.113", 'user': "root", 'password': "123456",
#         'database': "kidney_tumor_dev", 'charset': 'utf8'}
# args = {'server': "10.1.192.172", 'user': "root", 'password': "WondersGroup_Mysql3306",
#         'database': "kidney_tumor_pro", 'charset': 'utf8'}
# 本地环境
args = {'server': "127.0.0.1", 'user': "sjkdatazh", 'password': "0P$!EAPnXWDF",
        'database': "kidney_tumor_origin", 'charset': 'utf8'}

cdr_patient_info = ['empi', 'patient_no', 'patient_name', 'sex', 'age', 'id_number', 'card_no', 'card_type',
                    'inpatient_number', 'in_hospital_date_time', 'inpatient_primary_diagnose', 'operate_date',
                    'outpatient_number', 'visit_date_time', 'outpatient_primary_diagnose', 'hospital_code',
                    'hospital_name', 'create_date']

form_basic_info = ['empi', 'patient_no', 'encounter_id', 'patient_name', 'sex', 'age', 'hzly',
                   'marriage_id', 'marriage_name', 'marriage', 'id_number', 'height', 'weight', 'birth',
                   'in_hospital_time', 'hospital_name', 'hospital_code', 'inpatient_number', 'outpatient_number',
                   'hukou_province_code', 'hukou_province_name', 'hukou_city_code', 'hukou_city_name',
                   'hukou_county_code', 'hukou_county_name', 'hukou_addr', 'native_province_code',
                   'native_province_name', 'native_city_code', 'native_city_name', 'current_province_code',
                   'current_province_name', 'current_city_code', 'current_city_name', 'current_county_code',
                   'current_county_name', 'current_addr', 'current_tel', 'contact_tel', 'pay_way', 'create_date']

form_electrocardio_info = ['empi', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                           'report_name', 'report_id', 'ts_exam', 'exam_name', 'exam_find', 'exam_conclusion',
                           'id_number', 'hospital_code', 'create_date']

form_labinfo = ['empi', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number', 'lab_generic_id',
                'ts_test', 'ts_draw', 'test_item_code', 'test_item_name', 'print_value', 'result_value',
                'result_unit', 'reference_text', 'abnormal_flag', 'abnormal_flag_name', 'id_number',
                'hospital_code', 'create_date']

form_lis_follow = ['empi', 'patient_no', 'encounter_id', 'outpatient_number', 'encounter_type', 'question_id',
                   'lab_generic_id', 'ts_test', 'ts_draw', 'reportname', 'test_item_code', 'test_item_name',
                   'print_value', 'result_value', 'result_unit', 'reference_text', 'abnormal_flag',
                   'abnormal_flag_name', 'id_number', 'hospital_code', 'create_date', 'update_date',
                   'last_update_date', 'delete_flag']

form_medication_orders = ['empi', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                          'in_hospital_time', 'inhospital_dept_name', 'question_id', 'medication_type',
                          'master_orders_id', 'orders_attribute_id', 'orders_attribute_name', 'orders_type_code',
                          'orders_type_name', 'project_type_code', 'project_type_name', 'orders_code', 'orders_name',
                          'origin_name', 'specifications', 'dosage', 'dosage_unit', 'project_number',
                          'project_number_unit', 'frequency', 'pc', 'pathway', 'write_recipe_time',
                          'orders_start_time', 'stop_time', 'id_number', 'hospital_code', 'create_date', 'update_date',
                          'last_update_date', 'delete_flag']

form_operate_pathology_info = ['empi', 'empi_old', 'patient_no', 'encounter_id', 'inpatient_number',
                               'outpatient_number', 'operate_date', 'operate_code', 'operate_name', 'operate_process',
                               'sslx', 'ssfs', 'report_id', 'ts_exam', 'report_name', 'exam_find', 'exam_conclusion',
                               'id_number', 'hospital_code', 'create_date']

form_outpatient_visit = ['empi', 'patient_no', 'encounter_id', 'cardno', 'patient_name', 'age', 'patient_sex',
                         'outpatient_code', 'outpatient_name', 'category_code', 'category_name', 'visit_date',
                         'visit_time', 'visit_state_code', 'visit_state_name', 'visit_dept_code', 'visit_dept_name',
                         'patid', 'visit_state_mapid', 'outpatient_class_code', 'outpatient_class_name',
                         'visit_date_time', 'primary_diagnosis', 'primary_diagnosis_code', 'visit_deptid',
                         'hospital_code', 'create_date']

form_pathology_info = ['empi', 'patient_no', 'patient_image_id', 'ts_exam', 'report_name', 'body_site', 'exam_find',
                       'exam_conclusion', 'id_number', 'hospital_code', 'create_date']

form_radiology_info = ['empi', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                       'report_name', 'jclx', 'ts_exam', 'exam_name', 'body_site', 'jcbw', 'exam_method', 'exam_tech',
                       'exam_find', 'exam_conclusion', 'report_iD', 'id_number', 'hospital_code', 'create_date']

form_radiotherapy_info = ['empi', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                          'report_id', 'report_name', 'exam_name', 'ts_exam', 'exam_conclusion', 'id_number',
                          'hospital_code', 'create_date', 'report_class_name', 'jcxm']

form_ris_follow = ['id', 'empi', 'patient_no', 'encounter_id', 'outpatient_number', 'encounter_type', 'question_id',
                   'report_id', 'diagnosis', 'patient_imageid', 'report_name', 'exam_operator', 'ts_exam', 'body_site',
                   'exam_name', 'exam_tech', 'exam_method', 'exam_find', 'exam_conclusion', 'report_class_name',
                   'jclx', 'jcxm', 'jcbw', 'jcbw_qt', 'id_number', 'hospital_code', 'create_date', 'update_date',
                   'last_update_date', 'delete_flag']

form_scale_follow = ['id', 'empi', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                     'encounter_type', 'lab_generic_id', 'ts_test', 'ts_draw', 'reportname', 'test_item_code',
                     'test_item_name', 'print_value', 'result_value', 'result_unit', 'reference_text', 'abnormal_flag',
                     'abnormal_flag_name', 'hospital_code', 'create_date', 'update_date', 'last_update_date',
                     'delete_flag']

form_ultrasound_info = ['empi', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                        'report_id', 'exam_name', 'report_name', 'body_site', 'ts_exam', 'exam_find',
                        'exam_conclusion', 'jcxm', 'jcbw', 'jcbw_qt', 'id_number', 'hospital_code', 'create_date']

qs_drz_patient = ['patient_num', 'empi', 'inpatient_number', 'name', 'id_card', 'card_no', 'card_type', 'sex', 'age',
                  'hospital_id', 'group_id', 'in_hospital_datetime', 'in_hospital_dept_code', 'in_hospital_dept_name',
                  'primary_diagnosis_code', 'primary_diagnosis', 'outpatient_number', 'visit_date_time',
                  'outpatient_primary_diagnose', 'operate_date', 'operate_code', 'operate_name', 'delete_flag',
                  'source_flag', 'operate_idx', 'create_date', 'hospital_code']

qs_drz_patient_operation = ['operation_num', 'empi', 'inpatient_number', 'in_hospital_datetime',
                            'in_hospital_dept_code', 'in_hospital_dept_name', 'primary_diagnosis_code',
                            'primary_diagnosis', 'operate_date', 'operate_code', 'operate_name', 'sslx', 'ssfs',
                            'operate_doctor_name', 'operate_first_name', 'source_flag', 'operate_idx', 'create_date',
                            'last_update_date', 'id_number', 'hospital_code', 'update_date']

entry_tables = {'cdr_patient_info': cdr_patient_info,
                # 'qs_drz_patient': qs_drz_patient,
                # 'qs_drz_patient_operation': qs_drz_patient_operation,
                # 'form_basic_info': form_basic_info,
                'form_electrocardio_info': form_electrocardio_info,
                'form_labinfo_biochemistry': form_labinfo,
                'form_labinfo_creactiveprotein': form_labinfo,
                'form_labinfo_dungroutine': form_labinfo,
                'form_labinfo_electrolyte': form_labinfo,
                'form_labinfo_esr': form_labinfo,
                'form_labinfo_liverfunction': form_labinfo,
                'form_labinfo_myocardialinfarctionmarkers': form_labinfo,
                'form_labinfo_renalfunction': form_labinfo,
                'form_labinfo_thyroidparathyroids': form_labinfo,
                'form_labinfo_urineanalysis': form_labinfo,
                'form_labinfo_wholeblood': form_labinfo,
                'form_lis_follow': form_lis_follow,
                'form_medication_orders': form_medication_orders,
                'form_medication_orders1': form_medication_orders,
                'form_operate_pathology_info': form_operate_pathology_info,
                'form_outpatient_visit': form_outpatient_visit,
                'form_outpatient_visit1': form_outpatient_visit,
                'form_pathology_info': form_pathology_info,
                'form_radiology_info': form_radiology_info,
                'form_radiotherapy_info': form_radiotherapy_info,
                'form_ris_follow': form_ris_follow,
                'form_ris_follow1': form_ris_follow,
                'form_ris_follow_ultra': form_ris_follow,
                'form_scale_follow': form_scale_follow,
                'form_ultrasound_info': form_ultrasound_info,
                }

status = 'process_status'

# 插入待入组
upd_drz = "update {} set {} = 's' where {} is NULL".format('entry_data_zy_medical_record', status, status)

insert_drz = "INSERT INTO qs_drz_patient(patient_num, empi, inpatient_number, name, id_card, card_no, card_type, " \
             "sex, age, hospital_id, group_id, in_hospital_datetime, in_hospital_dept_code, in_hospital_dept_name, " \
             "primary_diagnosis_code, primary_diagnosis, outpatient_number, visit_date_time, " \
             "outpatient_primary_diagnose, operate_date, operate_code, operate_name, delete_flag, source_flag, " \
             "operate_idx, create_date, hospital_code) " \
             "SELECT DISTINCT date_format(now(), '%Y%m%d') AS patient_num, concat_ws(';', a.kh, a.yljgdm, a.klx) AS " \
             "empi, a.cisid AS inpatient_number, b.xm AS name, b.zjhm AS id_card, a.kh AS card_no, a.klx AS " \
             "card_type, CASE WHEN b.xb = '1' THEN '男' WHEN b.xb = '2' THEN '女' ELSE '未知' END as sex, " \
             "LEFT(a.rysj, 4) - LEFT(b.csrq, 4) AS age, c.id as hospital_id, 1 AS group_id, a.rysj AS " \
             "in_hospital_datetime, a.jzksbm AS in_hospital_dept_code, a.jzksmc AS in_hospital_dept_name, " \
             "GROUP_CONCAT(distinct d.zdbm SEPARATOR '；') AS primary_diagnosis_code, " \
             "GROUP_CONCAT(distinct d.zdsm SEPARATOR '；') AS primary_diagnosis, e.jzlsh AS outpatient_number, " \
             "e.jzksrq AS visit_date_time, e.jzzdsm AS outpatient_primary_diagnose, left(f.sskssj, 10) AS " \
             "operate_date, f.ssczbm AS operate_code, f.ssczmc AS operate_name, '0' AS delete_flag, '0' AS " \
             "source_flag, f.ssxh AS operate_idx, from_unixtime(unix_timestamp(),'%Y-%m-%d') AS create_date, " \
             "a.yljgdm AS hospital_code FROM (SELECT * FROM (SELECT @rn:= CASE WHEN @yljgdm = yljgdm AND @kh = kh " \
             "AND @klx = klx THEN @rn+1 ELSE 1 END AS rn, @yljgdm:= yljgdm AS yljgdm, @kh:= kh AS kh, @klx:= klx AS " \
             "klx, jzlsh, cisid, rysj, jzksbm, jzksmc FROM (SELECT * FROM entry_data_zy_medical_record WHERE " \
             "process_status = 's' ORDER BY yljgdm, kh, klx, rysj) aa, (SELECT @rn:=0, @yljgdm:=0, @kh:=0, @klx:=0) " \
             "bb) cc WHERE cc.rn = 1) a " \
             "LEFT JOIN (SELECT DISTINCT yljgdm, kh, klx, zjhm, xb, xm, csrq FROM entry_data_patient_info) b " \
             "ON a.yljgdm = b.yljgdm AND a.kh = b.kh AND a.klx = b.klx " \
             "LEFT JOIN dic_hospital c ON a.yljgdm = c.code " \
             "JOIN (SELECT * FROM entry_data_ih_diagnosis_detail WHERE TRIM(zdsm) IN ('肾恶性肿瘤', " \
             "'肾(除外肾盂)恶性肿瘤', '肾恶性肿瘤个人史', '肾恶性肿瘤', '肾肿瘤', '肾(肾盂)恶性肿瘤免疫治疗', '肾恶性肿瘤', " \
             "'肾良性肿瘤', '肾恶性肿瘤(肾细胞癌)', '肾继发恶性肿瘤', '肾恶性肿瘤(肾细胞癌)', '肾(盂)恶性肿瘤术后随诊检查', " \
             "'肾(盂)恶性肿瘤靶向治疗', '肾(盂)肿瘤免疫治疗', '肾恶性肿瘤术后化疗', '肾(盂)恶性肿瘤术后免疫治疗', '肾继发恶性肿瘤', " \
             "'肾和肾盂继发性恶性肿瘤', '肾(盂)恶性肿瘤化疗后随诊', '肾(盂)恶性肿瘤术后靶向治疗', '肾(盂)恶性肿瘤术前新辅助化疗', " \
             "'肾(盂)恶性肿瘤(术后)对症治疗', '肾占位(肿瘤)', '肾继发恶性肿瘤', '肾恶性肿瘤术后复发', '肾(盂)恶性肿瘤放疗', " \
             "'肾、输尿管恶性肿瘤靶向治疗', '肾(肾盂)恶性肿瘤术后对症治疗', '肾(肾盂)恶性肿瘤介入治疗', '肾(盂)恶性肿瘤免疫治疗', " \
             "'肾交界性肿瘤', '肾(肾盂)恶性肿瘤对症支持治疗', '肾多处恶性肿瘤', '肾恶性肿瘤介入治疗后随诊', '双侧肾恶性肿瘤', " \
             "'肾(盂)恶性肿瘤(术后)免疫治疗', '肾、输尿管恶性肿瘤术后靶向治疗', '肾(盂)恶性肿瘤术前放疗', '肾恶性肿瘤(肾母细胞瘤)', " \
             "'肾动态未定或动态未知的肿瘤', '肾混合性上皮间质肿瘤', '肾和肾盂继发性恶性肿瘤', '肾（肾盂）恶性肿瘤术前化疗', " \
             "'肾(盂)恶性肿瘤术后放疗', '肾恶性肿瘤射频治疗', '肾恶性肿瘤术后随诊检查', '肾占位性病变待查', '肾占位性病变', '肾肿物', " \
             "'肾透明细胞癌', '肾透明细胞癌', '肾乳头状肾细胞癌', '肾乳头状肾细胞癌', '肾嫌色细胞癌', '肾母细胞癌', '肾透明细胞腺癌', " \
             "'肾(盂)恶性肿瘤术后化疗', '肾(肾盂)恶性肿瘤术后化疗', '肾输尿管恶性肿瘤', '肾(肾盂)恶性肿瘤化疗', '肾(盂)恶性肿瘤化疗', " \
             "'肾肿瘤', '中肾样型透明细胞癌', '肾癌术后随诊检查', '肾细胞癌', '肾盏恶性肿瘤', '肾周恶性肿瘤', '肾恶性肿瘤', " \
             "'左肾肿瘤', '乳头状肾细胞癌', '肾恶性肿瘤术后随诊检查', '转移性肾细胞癌', '左肾恶性肿瘤', '肾原位癌', '右侧肾肿瘤', " \
             "'左侧肾肿瘤', '左肾盂肿瘤', '肾(盂)恶性肿瘤(术后)免疫治疗')) d " \
             "ON a.yljgdm = d.yljgdm AND a.kh = d.kh AND a.klx = d.klx AND a.jzlsh = d.jzlsh " \
             "LEFT JOIN (SELECT * FROM (SELECT @rns:= CASE WHEN @yljgdm = yljgdm AND @kh = kh AND @klx = klx THEN " \
             "@rns+1 ELSE 1 END AS rns, @yljgdm:= yljgdm AS yljgdm, @kh:= kh AS kh, @klx:= klx AS klx, jzlsh, " \
             "jzksrq, jzzdsm FROM (SELECT * FROM entry_data_mz_medical_record ORDER BY yljgdm, kh, klx, jzksrq DESC) " \
             "dd, (SELECT @rns:=0, @yljgdm:=0, @kh:=0, @klx:=0) ee) ff WHERE ff.rns = 1) e " \
             "ON a.yljgdm = e.yljgdm AND a.kh = e.kh AND a.klx = e.klx " \
             "JOIN (SELECT * FROM (SELECT @ranks:= CASE WHEN @yljgdm = yljgdm AND @kh = kh AND @klx = klx AND @jzlsh " \
             "= jzlsh THEN @ranks+1 ELSE 1 END AS ranks, @yljgdm:= yljgdm AS yljgdm, @kh:= kh AS kh, @klx:= klx AS " \
             "klx, @jzlsh:= jzlsh AS jzlsh, sskssj, ssczbm, ssczmc, ssxh FROM (SELECT * FROM " \
             "entry_data_operation_detail WHERE TRIM(ssczmc) IN ('腹腔镜下肾部分切除术', '腹腔镜下肾部分切除术', " \
             "'腹腔镜下肾癌根治术', '肾部分切除术', '腹腔镜下肾切除术', '腹腔镜下肾肿瘤射频消融', '肾癌根治术', '腹腔镜下单侧肾切除术', " \
             "'腹腔镜下肾肿瘤射频消融术', '单侧肾切除术', '腹腔镜下肾癌根治术', '腹腔镜下肾根治切除术', '达芬奇下肾部分切除术', " \
             "'腹腔镜下肾脏肿瘤切除术', '达芬奇下肾部分切除术', '腹腔镜下肾病损射频消融术', '腹腔镜下肾肿瘤射频消融', " \
             "'达芬奇下肾部分切除术', '肾肿瘤射频消融术', '肾切除术单侧', '肾癌根治术', '腹腔镜下肾根治切除术术', '肾部分切除术', " \
             "'肾病损切除术', '腹腔镜下肾周粘连松解术', '部分肾切除术', '肾根治切除术', '腹腔镜下肾(肿瘤)消融术', '肾肿瘤切除术', " \
             "'达芬奇下肾癌根治术', '腹腔镜下肾脏肿瘤切除术', '肾静脉取栓术', '腹腔镜下肾病损切除术', '达芬奇下单侧肾切除术', " \
             "'腹腔镜下肾根治性切除术', '肾肿瘤射频消融术', '下腔静脉取栓术', '肾病损射频消融术', 'B超引导下经皮肾肿瘤微波消融术', " \
             "'肾病损切除术', '双侧肾切除术', '肾周围粘连松解术', '超声造影下肾肿瘤穿刺术', '达芬奇下肾癌根治术', " \
             "'腹腔镜下肾病损切除术', '肾静脉切开取栓术', '肾根治切除术', '肾门淋巴结清扫术', '开放性肾活组织检查', '肾周围活检术', " \
             "'腹腔镜下肾周或输尿管周围粘连的松解术', '肾探查术', '超声引导下肾病损射频消融术', '肾肿瘤切除术', '肾周围粘连松解', " \
             "'腹腔镜下肾肿瘤冷冻消融术', '腹腔镜下肾探查术', '达芬奇下肾病损切除术', '肾病损或组织的其他和未特指切除', " \
             "'腹腔镜下肾切除术', '下腔静脉病损切除术', '残留肾切除术', '肾周活组织检查', '肾周病损切除术', '腹腔镜中转开放肾癌根治术', " \
             "'肾病损或组织的经皮消融术', '双侧肾切除术') ORDER BY yljgdm, kh, klx, jzlsh, sskssj) aaa, (SELECT @ranks:=0, " \
             "@yljgdm:=0, @kh:=0, @klx:=0, @jzlsh:=0) bbb) ccc WHERE ccc.ranks = 1) f " \
             "ON a.yljgdm = f.yljgdm AND a.kh = f.kh AND a.klx = f.klx AND a.jzlsh = f.jzlsh " \
             "WHERE concat_ws(';', a.kh, a.yljgdm, a.klx) NOT IN (SELECT empi FROM qs_drz_patient) " \
             "GROUP BY a.kh, a.yljgdm, a.klx, a.cisid, b.xm, b.zjhm, b.xb, a.rysj, b.csrq, c.id, a.jzksbm, a.jzksmc, " \
             "e.jzlsh, e.jzksrq, e.jzzdsm, f.sskssj, f.ssczbm, f.ssczmc, f.ssxh"

# 更新patient_num
update_drz = "UPDATE qs_drz_patient a SET a.patient_num = CONCAT(a.patient_num, '-', " \
             "unix_timestamp(STR_TO_DATE(a.patient_num, '%Y%m%d')) + a.id) WHERE a.hospital_code != '42502657200' " \
             "AND a.patient_num NOT LIKE '%-%' AND LENGTH(patient_num) = 8"

# 待入组手术
insert_drz_ope = "INSERT INTO qs_drz_patient_operation(operation_num, empi, inpatient_number, in_hospital_datetime, " \
                 "in_hospital_dept_code, in_hospital_dept_name, primary_diagnosis_code, primary_diagnosis, " \
                 "operate_date, operate_code, operate_name, sslx, ssfs, operate_doctor_name, operate_first_name, " \
                 "source_flag, operate_idx, create_date, last_update_date, id_number, hospital_code, update_date) " \
                 "SELECT concat_ws('#', concat(a.yljgdm, a.ssmxlsh), a.ssxh) AS operation_num, " \
                 "concat_ws(';', a.kh, a.yljgdm, a.klx) AS empi, b.cisid AS inpatient_number, left(b.rysj, 10) AS " \
                 "in_hospital_datetime, b.jzksbm AS in_hospital_dept_code, b.jzksmc AS in_hospital_dept_name, " \
                 "GROUP_CONCAT(distinct c.zdbm SEPARATOR '；') AS primary_diagnosis_code, " \
                 "GROUP_CONCAT(distinct c.zdsm SEPARATOR '；') AS primary_diagnosis, left(a.sskssj, 10) AS " \
                 "operate_date, a.ssczbm AS operate_code, a.ssczmc AS operate_name, CASE WHEN a.ssczmc LIKE " \
                 "'%腹腔镜%' THEN '腹腔镜手术' WHEN a.ssczmc LIKE '%达芬奇下%' THEN '机器人辅助手术' ELSE '开放手术' END AS " \
                 "sslx, CASE WHEN a.ssczmc LIKE '%肾病损切除术%' OR a.ssczmc LIKE '%肾部分切除术%' OR a.ssczmc LIKE " \
                 "'%部分肾切除术%' THEN '肾部分切除术' WHEN a.ssczmc LIKE '%肾根治切除术%' OR a.ssczmc LIKE '%单侧肾切除术%' " \
                 "THEN '根治性肾切除术' WHEN a.ssczmc LIKE '%肾病损射频消融术%' THEN '微波辅助肿瘤剜除术' WHEN a.ssczmc LIKE " \
                 "'%下腔静脉取栓术%' or a.ssczmc LIKE '%上腔静脉取栓术%' OR a.ssczmc LIKE '%肾静脉切开取栓术%' OR a.ssczmc " \
                 "LIKE '%肾动脉切开取栓术%' THEN '瘤栓取出术' ELSE NULL END AS ssfs, NULL AS operate_doctor_name, NULL AS " \
                 "operate_first_name, '0' AS source_flag, a.ssxh AS operate_idx, " \
                 "from_unixtime(unix_timestamp(),'%Y-%m-%d') AS create_date, " \
                 "from_unixtime(unix_timestamp(),'%Y-%m-%d %H:%i:%s') AS last_update_date, d.zjhm AS id_number, " \
                 "a.yljgdm AS hospital_code, from_unixtime(unix_timestamp(),'%Y-%m-%d %H:%i:%s') AS update_date " \
                 "FROM (SELECT * FROM entry_data_operation_detail WHERE process_status = 'sy' AND TRIM(ssczmc) IN " \
                 "('腹腔镜下肾部分切除术', '腹腔镜下肾部分切除术', '腹腔镜下肾癌根治术', '肾部分切除术', '腹腔镜下肾切除术', " \
                 "'腹腔镜下肾肿瘤射频消融', '肾癌根治术', '腹腔镜下单侧肾切除术', '腹腔镜下肾肿瘤射频消融术', '单侧肾切除术', " \
                 "'腹腔镜下肾癌根治术', '腹腔镜下肾根治切除术', '达芬奇下肾部分切除术', '腹腔镜下肾脏肿瘤切除术', " \
                 "'达芬奇下肾部分切除术', '腹腔镜下肾病损射频消融术', '腹腔镜下肾肿瘤射频消融', '达芬奇下肾部分切除术', " \
                 "'肾肿瘤射频消融术', '肾切除术单侧', '肾癌根治术', '腹腔镜下肾根治切除术术', '肾部分切除术', '肾病损切除术', " \
                 "'腹腔镜下肾周粘连松解术', '部分肾切除术', '肾根治切除术', '腹腔镜下肾(肿瘤)消融术', '肾肿瘤切除术', " \
                 "'达芬奇下肾癌根治术', '腹腔镜下肾脏肿瘤切除术', '肾静脉取栓术', '腹腔镜下肾病损切除术', '达芬奇下单侧肾切除术', " \
                 "'腹腔镜下肾根治性切除术', '肾肿瘤射频消融术', '下腔静脉取栓术', '肾病损射频消融术', 'B超引导下经皮肾肿瘤微波消融术', " \
                 "'肾病损切除术', '双侧肾切除术', '肾周围粘连松解术', '超声造影下肾肿瘤穿刺术', '达芬奇下肾癌根治术', " \
                 "'腹腔镜下肾病损切除术', '肾静脉切开取栓术', '肾根治切除术', '肾门淋巴结清扫术', '开放性肾活组织检查', " \
                 "'肾周围活检术', '腹腔镜下肾周或输尿管周围粘连的松解术', '肾探查术', '超声引导下肾病损射频消融术', '肾肿瘤切除术', " \
                 "'肾周围粘连松解', '腹腔镜下肾肿瘤冷冻消融术', '腹腔镜下肾探查术', '达芬奇下肾病损切除术', " \
                 "'肾病损或组织的其他和未特指切除', '腹腔镜下肾切除术', '下腔静脉病损切除术', '残留肾切除术', '肾周活组织检查', " \
                 "'肾周病损切除术', '腹腔镜中转开放肾癌根治术', '肾病损或组织的经皮消融术', '双侧肾切除术')) a LEFT JOIN " \
                 "(SELECT DISTINCT cisid, rysj, jzksbm, jzksmc, yljgdm, kh, klx, jzlsh FROM " \
                 "entry_data_zy_medical_record) b " \
                 "ON a.yljgdm = b.yljgdm AND a.kh = b.kh AND a.klx = b.klx AND a.jzlsh = b.jzlsh " \
                 "LEFT JOIN entry_data_ih_diagnosis_detail c ON a.yljgdm = c.yljgdm AND a.jzlsh = c.jzlsh " \
                 "LEFT JOIN (SELECT DISTINCT zjhm, yljgdm, kh, klx FROM entry_data_patient_info) d " \
                 "ON a.yljgdm = d.yljgdm AND a.kh = d.kh AND a.klx = d.klx " \
                 "GROUP BY a.kh, a.klx, a.yljgdm, a.ssmxlsh, b.cisid, b.rysj, b.jzksbm, b.jzksmc, a.sskssj, " \
                 "a.ssczbm, a.ssczmc, a.ssxh, d.zjhm"

# 基本信息表
insert_basic_info = "INSERT INTO form_basic_info(empi, patient_no, encounter_id, patient_name, sex, age, hzly," \
                    "marriage_id, marriage_name, marriage, id_number, height, weight, birth, in_hospital_time, " \
                    "hospital_name, hospital_code, inpatient_number, outpatient_number, hukou_province_code, " \
                    "hukou_province_name, hukou_city_code, hukou_city_name, hukou_county_code, hukou_county_name, " \
                    "hukou_addr, native_province_code, native_province_name, native_city_code, native_city_name, " \
                    "current_province_code, current_province_name, current_city_code, current_city_name, " \
                    "current_county_code, current_county_name, current_addr, current_tel, contact_tel, pay_way, " \
                    "create_date) SELECT DISTINCT concat_ws(';', a.kh, a.yljgdm, a.klx) AS empi, a.kh AS patient_no, " \
                    "a.jzlsh AS encounter_id, b.xm AS patient_name, b.xb as sex, LEFT(a.rysj, 4) - LEFT(b.csrq, 4) " \
                    "AS age, b.hzlx AS hzly, NULL AS marriage_id, NULL AS marriage_name, b.hyzk AS marriage, b.zjhm " \
                    "AS id_number, NULL AS height, NULL AS weight, b.csrq AS birth, a.rysj AS in_hospital_time, " \
                    "c.name AS hospital_name, a.yljgdm AS hospital_code, a.cisid AS inpatient_number, d.jzlsh AS " \
                    "outpatient_number, NULL AS hukou_province_code, NULL AS hukou_province_name, NULL AS " \
                    "hukou_city_code, NULL AS hukou_city_name, NULL AS hukou_county_code, NULL AS hukou_county_name, " \
                    "b.hkdz AS hukou_addr, NULL AS native_province_code, b.csd AS native_province_name, NULL AS " \
                    "native_city_code, NULL AS native_city_name, NULL AS current_province_code, NULL AS " \
                    "current_province_name, NULL AS current_city_code, NULL AS current_city_name, NULL AS " \
                    "current_county_code, NULL AS current_county_name, b.jzdz AS current_addr, b.dhhm AS " \
                    "current_tel, NULL AS contact_tel, NULL AS pay_way, " \
                    "from_unixtime(unix_timestamp(),'%Y-%m-%d %H:%i:%s') AS create_date FROM " \
                    "(SELECT * FROM (SELECT @rn:= CASE WHEN @yljgdm = yljgdm AND @kh = kh AND @klx = klx THEN @rn+1 " \
                    "ELSE 1 END AS rn, @yljgdm:= yljgdm AS yljgdm, @kh:= kh AS kh, @klx:= klx AS klx, jzlsh, cisid, " \
                    "jzlx, rysj, jzksbm, jzksmc FROM (SELECT * FROM entry_data_zy_medical_record WHERE " \
                    "process_status = 'sy' ORDER BY yljgdm, kh, klx, rysj DESC) aa, (SELECT @rn:=0, @yljgdm:=0, " \
                    "@kh:=0, @klx:=0) bb) cc WHERE cc.rn = 1) a " \
                    "LEFT JOIN (SELECT DISTINCT yljgdm, kh, klx, zjhm, xb, xm,hzlx, hyzk, csrq, dhhm, hkdz, csd, " \
                    "jzdz FROM entry_data_patient_info) b ON a.yljgdm = b.yljgdm AND a.kh = b.kh AND a.klx = b.klx " \
                    "LEFT JOIN dic_hospital c ON a.yljgdm = c.code " \
                    "LEFT JOIN (SELECT * FROM (SELECT @ranks:= CASE WHEN @yljgdm = yljgdm AND @kh = kh AND @klx = " \
                    "klx THEN @ranks+1 ELSE 1 END AS ranks, @yljgdm:= yljgdm AS yljgdm, @kh:= kh AS kh, @klx:= klx " \
                    "AS klx, jzlsh FROM (SELECT * FROM entry_data_mz_medical_record ORDER BY yljgdm, kh, klx, jzksrq " \
                    "DESC) dd, (SELECT @ranks:=0, @yljgdm:=0, @kh:=0, @klx:=0) ee) ff WHERE ff.ranks = 1) d " \
                    "ON a.yljgdm = d.yljgdm AND a.kh = d.kh AND a.klx = d.klx"

biochemistry = ['血淀粉酶', '淀粉酶(血)', '淀粉酶']

c_reactive_protein = ['C反应蛋白', 'C-反应蛋白', 'C反应蛋白(CRP)']

dung_routine = ['颜色', '形状', '性状', '红细胞', '白细胞', '吞噬细胞', '酵母菌', '虫卵', '油滴', '粪隐血试验',
                '粪便隐血(免疫法)', '粪转铁蛋白', '粪便转铁蛋白测定', '粪便转铁蛋白']

electrolyte = ['血钾', '钾', '钾（全血）', '钾(全血)', '血钠', '钠', '钠（全血）', '钠(全血)', '血钙', '钙', '钙（全血）',
               '钙(全血)', '钙离子']

esr = ['血沉', '红细胞沉降率ESR']

liver_function = ['乳酸脱氢酶', '丙氨酸氨基转移酶', '丙氨酸氨基转移酶(ALT)', '天门冬氨酸氨基转移酶', '天门冬氨酸氨基转移酶(AST)',
                  '天门冬氨酸氨基转移酶（AST）', '总胆红素', '直接胆红素', '碱性磷酸酶']

myocardial_infarction_markers = ['肌酸激酶同工酶', '肌红蛋白', '肌钙蛋白I']

renal_function = ['肌酐', '尿素氮', '尿素', '胱抑素C']

thyroid_parathyroids = ['TSH', '促甲状腺激素(TSH)', 'T3', '总T3', 'T4', '总T4', 'FT3', '游离T3', 'FT4', '游离T4']

urine_analysis = ['尿蛋白', '尿蛋白质', '尿蛋白(PRO)', '尿RBC', '镜检红细胞', '红细胞(镜检)']

whole_blood = ['血红蛋白', '嗜中性粒细胞绝对值', '嗜中性细胞绝对值', '血小板计数', '红细胞计数', '白细胞计数']

lab_info = {'form_labinfo_biochemistry': [biochemistry, 's', 'sy'],
            'form_labinfo_creactiveprotein': [c_reactive_protein, 'sy', 'sy1'],
            'form_labinfo_dungroutine': [dung_routine, 'sy1', 'sy2'],
            'form_labinfo_electrolyte': [electrolyte, 'sy2', 'sy3'],
            'form_labinfo_esr': [esr, 'sy3', 'sy4'],
            'form_labinfo_liverfunction': [liver_function, 'sy4', 'sy5'],
            'form_labinfo_myocardialinfarctionmarkers': [myocardial_infarction_markers, 'sy5', 'sy6'],
            'form_labinfo_renalfunction': [renal_function, 'sy6', 'sy7'],
            'form_labinfo_thyroidparathyroids': [thyroid_parathyroids, 'sy7', 'sy8'],
            'form_labinfo_urineanalysis': [urine_analysis, 'sy8', 'sy9'],
            'form_labinfo_wholeblood': [whole_blood, 'sy9', 'sy10'],
            }

lis_sql = "SELECT DISTINCT concat_ws(';', b.kh, b.yljgdm, d.klx) AS empi, b.kh AS patient_no, b.jzlsh AS " \
          "encounter_id, d.cisid AS inpatient_number, NULL AS outpatient_number, a.jyzblsh AS lab_generic_id, b.jyrq " \
          "AS ts_test, b.cjrq AS ts_draw, a.jczbdm AS test_item_code, CASE WHEN a.jczbmc IN ('淀粉酶', '淀粉酶(血)') " \
          "THEN '血淀粉酶' WHEN a.jczbmc IN ('C-反应蛋白', 'C反应蛋白(CRP)') THEN 'C反应蛋白' WHEN a.jczbmc = '形状' THEN " \
          "'性状' WHEN a.jczbmc = '粪便隐血(免疫法)' THEN '粪隐血试验' WHEN a.jczbmc IN ('粪便转铁蛋白测定', '粪便转铁蛋白') " \
          "THEN '粪转铁蛋白' WHEN a.jczbmc IN ('钾', '钾（全血）', '钾(全血)') THEN '血钾' WHEN a.jczbmc IN " \
          "('钠', '钠（全血）', '钠(全血)') THEN '血钠' WHEN a.jczbmc IN ('钙', '钙（全血）', '钙(全血)', '钙离子') THEN " \
          "'血钙' WHEN a.jczbmc = '红细胞沉降率ESR' THEN '血沉' WHEN a.jczbmc = '丙氨酸氨基转移酶(ALT)' THEN " \
          "'丙氨酸氨基转移酶' WHEN a.jczbmc IN ('天门冬氨酸氨基转移酶(AST)', '天门冬氨酸氨基转移酶（AST）') THEN " \
          "'天门冬氨酸氨基转移酶' WHEN a.jczbmc = '尿素' THEN '尿素氮' WHEN a.jczbmc = '促甲状腺激素(TSH)' THEN 'TSH' WHEN " \
          "a.jczbmc = '总T3' THEN 'T3' WHEN a.jczbmc = '总T4' THEN 'T4' WHEN a.jczbmc = '游离T3' THEN 'FT3' WHEN " \
          "a.jczbmc = '游离T4' THEN 'FT4' WHEN a.jczbmc IN ('尿蛋白质', '尿蛋白(PRO)') THEN '尿蛋白' WHEN a.jczbmc IN " \
          "('红细胞(镜检)', '镜检红细胞') THEN '尿RBC' WHEN a.jczbmc IN ('嗜中性粒细胞绝对值', '嗜中性细胞绝对值') THEN " \
          "'中性粒细胞' WHEN a.jczbmc = '血小板计数' THEN '血小板' WHEN a.jczbmc = '红细胞计数' THEN '红细胞' WHEN a.jczbmc " \
          "= '白细胞计数' THEN '白细胞' ELSE a.jczbmc END AS test_item_name, a.jczbjg AS print_value, a.jczbjg AS " \
          "result_value, a.jldw AS result_unit, a.ckz AS reference_text, a.ycts AS abnormal_flag, a.ycts AS " \
          "abnormal_flag_name, c.zjhm AS id_number, a.yljgdm AS hospital_code, " \
          "FROM_UNIXTIME(UNIX_TIMESTAMP(),'%%Y-%%m-%%d %%H:%%i:%%s') AS create_date FROM " \
          "(SELECT jyzblsh, yljgdm, bgdh, bgrq, jczbmc, jczbjg, jldw, jczbdm, ckz, ycts FROM " \
          "entry_data_lis_indicator WHERE id IN (SELECT t.max_id FROM (SELECT MAX(id) AS max_id FROM " \
          "entry_data_lis_indicator WHERE process_status = '%s' GROUP BY jyzblsh, yljgdm, jczbdm) AS t) AND jczbmc " \
          "IN ('%s')) a JOIN " \
          "(SELECT yljgdm, kh, jzlsh, bgdh, bgrq, cjrq, jyrq, bbdm, bgdlbbm, bgdlb FROM entry_data_lis_report " \
          "WHERE id IN (SELECT t.max_id FROM (SELECT MAX(id) AS max_id FROM entry_data_lis_report GROUP BY yljgdm, " \
          "bgdh, bgrq, jzlsh) AS t)) b ON a.yljgdm = b.yljgdm AND a.bgdh = b.bgdh AND a.bgrq = b.bgrq " \
          "LEFT JOIN entry_data_patient_info c ON b.yljgdm = c.yljgdm AND b.kh = c.kh " \
          "JOIN entry_data_zy_medical_record d ON b.yljgdm = d.yljgdm AND b.jzlsh = d.jzlsh"

lis_sql_follow = "SELECT DISTINCT concat_ws(';', b.kh, b.yljgdm, d.klx) AS empi, b.kh AS patient_no, b.jzlsh AS " \
                 "encounter_id, NULL AS inpatient_number, NULL AS outpatient_number, b.mzzybz AS encounter_type, " \
                 "case when a.jczbmc in ('血红蛋白', '嗜中性粒细胞绝对值', '嗜中性细胞绝对值', '血小板计数', '红细胞计数', " \
                 "'白细胞计数') and b.bbmc LIKE '%血%' then 12 when a.jczbmc in ('乳酸脱氢酶', '丙氨酸氨基转移酶', " \
                 "'丙氨酸氨基转移酶(ALT)', '天门冬氨酸氨基转移酶', '天门冬氨酸氨基转移酶(AST)', '天门冬氨酸氨基转移酶（AST）', " \
                 "'总胆红素', '直接胆红素', '碱性磷酸酶') then 13 when a.jczbmc in ('肌酐', '尿素氮', '尿素', '胱抑素C') then " \
                 "14 when a.jczbmc in ('血钾', '钾', '钾（全血）', '钾(全血)', '血钠', '钠', '钠（全血）', '钠(全血)', " \
                 "'血钙', '钙', '钙（全血）', '钙(全血)', '钙离子') then 15 when a.jczbmc in ('C反应蛋白', 'C-反应蛋白', " \
                 "'C反应蛋白(CRP)') then 16 when a.jczbmc in ('尿蛋白', '尿蛋白质', '尿蛋白(PRO)', '尿RBC', '镜检红细胞', " \
                 "'红细胞(镜检)') and b.bbmc LIKE '%尿%' then 17 when a.jczbmc in ('TSH', '促甲状腺激素(TSH)', 'T3', " \
                 "'总T3', 'T4', '总T4', 'FT3', '游离T3', 'FT4', '游离T4') then 18 when a.jczbmc in ('肌酸激酶同工酶', " \
                 "'肌红蛋白', '肌钙蛋白I') then 19 when a.jczbmc in ('血淀粉酶', '淀粉酶(血)', '淀粉酶') then 20 when " \
                 "a.jczbmc in ('颜色', '形状', '性状', '红细胞', '白细胞', '吞噬细胞', '酵母菌', '虫卵', '油滴', '粪隐血试验', " \
                 "'粪便隐血(免疫法)', '粪转铁蛋白', '粪便转铁蛋白测定', '粪便转铁蛋白') and b.bbmc like '%粪%' then 21 when " \
                 "a.jczbmc in ('血沉', '红细胞沉降率ESR') then 22 else NULL end as question_id, a.jyzblsh AS " \
                 "lab_generic_id, b.jyrq AS ts_test, b.cjrq AS ts_draw, b.bgdlb as reportname, a.jczbdm AS " \
                 "test_item_code, CASE WHEN a.jczbmc IN ('淀粉酶', '淀粉酶(血)') THEN '血淀粉酶' WHEN a.jczbmc IN " \
                 "('C-反应蛋白', 'C反应蛋白(CRP)') THEN 'C反应蛋白' WHEN a.jczbmc = '形状' THEN '性状' WHEN a.jczbmc = " \
                 "'粪便隐血(免疫法)' THEN '粪隐血试验' WHEN a.jczbmc IN ('粪便转铁蛋白测定', '粪便转铁蛋白') THEN '粪转铁蛋白' " \
                 "WHEN a.jczbmc IN ('钾', '钾（全血）', '钾(全血)') THEN '血钾' WHEN a.jczbmc IN ('钠', '钠（全血）', " \
                 "'钠(全血)') THEN '血钠' WHEN a.jczbmc IN ('钙', '钙（全血）', '钙(全血)', '钙离子') THEN '血钙' WHEN " \
                 "a.jczbmc = '红细胞沉降率ESR' THEN '血沉' WHEN a.jczbmc = '丙氨酸氨基转移酶(ALT)' THEN '丙氨酸氨基转移酶' " \
                 "WHEN a.jczbmc IN ('天门冬氨酸氨基转移酶(AST)', '天门冬氨酸氨基转移酶（AST）') THEN '天门冬氨酸氨基转移酶' WHEN " \
                 "a.jczbmc = '尿素' THEN '尿素氮' WHEN a.jczbmc = '促甲状腺激素(TSH)' THEN 'TSH' WHEN a.jczbmc = '总T3' " \
                 "THEN 'T3' WHEN a.jczbmc = '总T4' THEN 'T4' WHEN a.jczbmc = '游离T3' THEN 'FT3' WHEN a.jczbmc = " \
                 "'游离T4' THEN 'FT4' WHEN a.jczbmc IN ('尿蛋白质', '尿蛋白(PRO)') THEN '尿蛋白' WHEN a.jczbmc IN " \
                 "('红细胞(镜检)', '镜检红细胞') THEN '尿RBC' WHEN a.jczbmc IN ('嗜中性粒细胞绝对值', '嗜中性细胞绝对值') THEN " \
                 "'中性粒细胞' WHEN a.jczbmc = '血小板计数' THEN '血小板' WHEN a.jczbmc = '红细胞计数' THEN '红细胞' WHEN " \
                 "a.jczbmc = '白细胞计数' THEN '白细胞' ELSE a.jczbmc END AS test_item_name, a.jczbjg AS print_value, " \
                 "a.jczbjg AS result_value, a.jldw AS result_unit, a.ckz AS reference_text, a.ycts AS abnormal_flag, " \
                 "IFNULL(a.ycts, 'NA') AS abnormal_flag_name, c.zjhm AS id_number, a.yljgdm AS hospital_code, " \
                 "FROM_UNIXTIME(UNIX_TIMESTAMP(),'%%Y-%%m-%%d %%H:%%i:%%s') AS create_date FROM (SELECT * FROM " \
                 "entry_data_lis_indicator WHERE id IN (SELECT t.max_id FROM (SELECT MAX(id) AS max_id FROM " \
                 "entry_data_lis_indicator WHERE process_status = 'sy10' GROUP BY jyzblsh, yljgdm, jczbdm) AS t)) a " \
                 "JOIN (SELECT * FROM entry_data_lis_report WHERE cjrq >= '2019-12-01' and id IN (SELECT t.max_id " \
                 "FROM (SELECT MAX(id) AS max_id FROM entry_data_lis_report GROUP BY yljgdm, bgdh, bgrq, jzlsh) " \
                 "AS t)) b ON a.yljgdm = b.yljgdm AND a.bgdh = b.bgdh AND a.bgrq = b.bgrq " \
                 "LEFT JOIN entry_data_patient_info c ON b.yljgdm = c.yljgdm AND b.kh = c.kh " \
                 "JOIN entry_data_mz_medical_record d ON b.yljgdm = d.yljgdm AND b.jzlsh = d.jzlsh"

outpatient_sql = "SELECT DISTINCT empi, patient_no, encounter_id, cardno, patient_name, age, patient_sex, " \
                 "outpatient_code, outpatient_name, category_code, category_name, visit_date, visit_time, " \
                 "visit_state_code, visit_state_name, visit_dept_code, visit_dept_name, patid, visit_state_mapid, " \
                 "outpatient_class_code, outpatient_class_name, visit_date_time, primary_diagnosis, " \
                 "primary_diagnosis_code, visit_deptid, hospital_code, create_date FROM (SELECT @ranks:= CASE WHEN " \
                 "@empi = empi AND @inpatientnumber = inpatientnumber AND @follow_time = follow_time THEN @ranks+1 " \
                 "ELSE 1 END AS ranks, @empi:= empi AS empi, @inpatientnumber:= inpatientnumber AS inpatientnumber, " \
                 "@follow_time:= follow_time AS follow_time, patient_no, encounter_id, cardno, patient_name, age, " \
                 "patient_sex, outpatient_code, outpatient_name, category_code, category_name, visit_date, " \
                 "visit_time, visit_state_code, visit_state_name, visit_dept_code, visit_dept_name, patid, " \
                 "visit_state_mapid, outpatient_class_code, outpatient_class_name, visit_date_time, " \
                 "primary_diagnosis, primary_diagnosis_code, visit_deptid, hospital_code, create_date from " \
                 "(SELECT * from (SELECT * FROM (select DISTINCT concat_ws(';', a.kh, a.yljgdm, a.klx) as empi, a.kh " \
                 "as patient_no, a.jzlsh as encounter_id, a.kh as cardno, c.xm as patient_name, LEFT(a.jzksrq, 4) - " \
                 "LEFT(c.csrq, 4) AS age, c.xb as patient_sex, NULL as outpatient_code, NULL as outpatient_name, " \
                 "NULL as category_code, NULL as category_name, NULL as visit_date, NULL as visit_time, NULL as " \
                 "visit_state_code, NULL as visit_state_name, a.jzksbm as visit_dept_code, a.jzksmc as " \
                 "visit_dept_name, a.id as patid, NULL as visit_state_mapid, NULL as outpatient_class_code, NULL as " \
                 "outpatient_class_name, a.jzksrq as visit_date_time, a.jzzdsm as primary_diagnosis, a.jzzdbm as " \
                 "primary_diagnosis_code, NULL as visit_deptid, a.yljgdm as hospital_code, " \
                 "FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, b.cisid as inpatientnumber, " \
                 "b.cysj as outhospitaltime, case when a.jzksrq BETWEEN " \
                 "subdate(adddate(b.cysj, interval 3 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 3 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 3 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 6 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 6 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 6 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 9 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 9 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 9 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 12 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 12 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 12 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 15 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 15 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 15 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 18 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 18 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 18 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 21 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 21 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 21 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 24 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 24 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 24 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 30 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 30 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 30 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 36 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 36 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 36 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 42 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 42 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 42 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 48 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 48 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 48 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 54 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 54 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 54 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 60 MONTH), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 60 MONTH), interval 2 WEEK) then adddate(b.cysj, interval 60 MONTH) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 6 YEAR), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 6 YEAR), interval 2 WEEK) then adddate(b.cysj, interval 6 YEAR) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 7 YEAR), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 7 YEAR), interval 2 WEEK) then adddate(b.cysj, interval 7 YEAR) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 8 YEAR), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 8 YEAR), interval 2 WEEK) then adddate(b.cysj, interval 8 YEAR) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 9 YEAR), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 9 YEAR), interval 2 WEEK) then adddate(b.cysj, interval 9 YEAR) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 10 YEAR), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 10 YEAR), interval 2 WEEK) then adddate(b.cysj, interval 10 YEAR) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 11 YEAR), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 11 YEAR), interval 2 WEEK) then adddate(b.cysj, interval 11 YEAR) " \
                 "when a.jzksrq BETWEEN subdate(adddate(b.cysj, interval 12 YEAR), interval 2 WEEK) AND " \
                 "adddate(adddate(b.cysj, interval 12 YEAR), interval 2 WEEK) then adddate(b.cysj, interval 12 YEAR) " \
                 "else null end as follow_time " \
                 "from entry_data_mz_medical_record a join entry_data_zy_medical_record b " \
                 "on a.kh = b.kh and a.yljgdm = b.yljgdm and a.klx = b.klx left join entry_data_patient_info c " \
                 "on a.kh = c.kh and a.yljgdm = c.yljgdm and a.klx = c.klx) d WHERE " \
                 "(visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 3 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 3 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 6 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 6 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 9 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 9 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 12 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 12 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 15 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 15 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 18 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 18 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 21 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 21 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 24 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 24 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 30 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 30 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 36 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 36 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 42 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 42 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 48 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 48 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 54 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 54 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 60 MONTH), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 60 MONTH), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 6 YEAR), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 6 YEAR), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 7 YEAR), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 7 YEAR), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 8 YEAR), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 8 YEAR), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 9 YEAR), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 9 YEAR), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 10 YEAR), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 10 YEAR), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 11 YEAR), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 11 YEAR), interval 2 WEEK)) " \
                 "or (visit_date_time BETWEEN subdate(adddate(outhospitaltime, interval 12 YEAR), interval 2 WEEK) " \
                 "AND adddate(adddate(outhospitaltime, interval 12 YEAR), interval 2 WEEK))) d ORDER BY d.empi, " \
                 "d.inpatientnumber, d.follow_time, abs(TIMESTAMPDIFF(HOUR, d.visit_date_time, d.follow_time))) aa, " \
                 "(SELECT @ranks:=0, @empi:=0, @inpatientnumber:=0, @follow_time:=0) bb) cc " \
                 "WHERE cc.ranks = 1 AND cc.encounter_id NOT IN (SELECT encounter_id FROM form_outpatient_visit)"

outpatient_sql1 = "SELECT DISTINCT empi, patient_no, encounter_id, cardno, patient_name, age, patient_sex, " \
                  "outpatient_code, outpatient_name, category_code, category_name, visit_date, visit_time, " \
                  "visit_state_code, visit_state_name, visit_dept_code, visit_dept_name, patid, visit_state_mapid, " \
                  "outpatient_class_code, outpatient_class_name, visit_date_time, primary_diagnosis, " \
                  "primary_diagnosis_code, visit_deptid, hospital_code, create_date FROM " \
                  "(select DISTINCT concat_ws(';', a.kh, a.yljgdm, a.klx) as empi, a.kh as patient_no, a.jzlsh as " \
                  "encounter_id, a.kh as cardno, c.xm as patient_name, LEFT(a.jzksrq, 4) - LEFT(c.csrq, 4) AS age, " \
                  "c.xb as patient_sex, NULL as outpatient_code, NULL as outpatient_name, NULL as category_code, " \
                  "NULL as category_name, NULL as visit_date, NULL as visit_time, NULL as visit_state_code, NULL as " \
                  "visit_state_name, a.jzksbm as visit_dept_code, a.jzksmc as visit_dept_name, a.id as patid, NULL " \
                  "as visit_state_mapid, NULL as outpatient_class_code, NULL as outpatient_class_name, a.jzksrq as " \
                  "visit_date_time, a.jzzdsm as primary_diagnosis, a.jzzdbm as primary_diagnosis_code, NULL as " \
                  "visit_deptid, a.yljgdm as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') " \
                  "as create_date, b.cisid as inpatientnumber, b.cysj as outhospitaltime " \
                  "from entry_data_mz_medical_record a join entry_data_zy_medical_record b " \
                  "on a.kh = b.kh and a.yljgdm = b.yljgdm and a.klx = b.klx " \
                  "left join entry_data_patient_info c on a.kh = c.kh and a.yljgdm = c.yljgdm and a.klx = c.klx) d " \
                  "WHERE (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 3 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 3 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 6 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 6 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 9 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 9 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 12 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 12 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 15 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 15 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 18 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 18 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 21 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 21 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 24 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 24 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 30 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 30 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 36 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 36 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 42 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 42 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 48 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 48 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 54 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 54 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 60 MONTH), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 60 MONTH), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 6 YEAR), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 6 YEAR), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 7 YEAR), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 7 YEAR), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 8 YEAR), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 8 YEAR), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 9 YEAR), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 9 YEAR), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 10 YEAR), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 10 YEAR), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 11 YEAR), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 11 YEAR), interval 2 WEEK)) " \
                  "AND (d.visit_date_time NOT BETWEEN subdate(adddate(d.outhospitaltime, interval 12 YEAR), " \
                  "interval 2 WEEK) AND adddate(adddate(d.outhospitaltime, interval 12 YEAR), interval 2 WEEK)) " \
                  "AND d.encounter_id NOT IN (SELECT encounter_id FROM form_outpatient_visit)"

