# 需要根据服务器地址修改
# 生产环境
hive_args = {'server': "172.28.18.124", 'port': 10000, 'auth': "CUSTOM", 'database': "skzbk",
             'username': "skzbk", 'password': "skzbk123"}

# 生产环境
mysql_args = {'server': "172.28.17.239", 'user': "sjkdatazh", 'password': "5dg=ycnngMVh", 'port': 3306,
              'database': "kidney_tumor_origin", 'charset': 'utf8'}

# 测试环境
# mysql_args = {'server': "172.28.17.239", 'user': "root", 'password': "Abcde_12345", 'port': 3306,
#               'database': "kidney_tumor_test", 'charset': "utf8"}

cdr_patient_info = ['empi', 'patient_no', 'patient_name', 'sex', 'age', 'id_number', 'card_no', 'inpatient_number',
                    'in_hospital_date_time', 'inpatient_primary_diagnose', 'operate_date', 'outpatient_number',
                    'visit_date_time', 'outpatient_primary_diagnose', 'hospital_code', 'hospital_name', 'create_date']

qs_drz_patient = ['patient_num', 'empi', 'inpatient_number', 'name', 'id_card', 'card_no', 'sex', 'age', 'hospital_id',
                  'group_id', 'in_hospital_datetime', 'in_hospital_dept_code', 'in_hospital_dept_name',
                  'in_hospital_ward_code', 'in_hospital_ward_name', 'primary_diagnosis_code', 'primary_diagnosis',
                  'outpatient_number', 'visit_date_time', 'outpatient_primary_diagnose', 'operate_date',
                  'operate_code', 'operate_name', 'delete_flag', 'source_flag', 'operate_idx', 'create_date',
                  'hospital_code']

qs_drz_patient_operation = ['operation_num', 'empi', 'inpatient_number', 'in_hospital_datetime',
                            'in_hospital_dept_code', 'in_hospital_dept_name', 'primary_diagnosis_code',
                            'primary_diagnosis', 'operate_date', 'operate_code', 'operate_name', 'operate_process',
                            'sslx', 'ssfs', 'operate_doctor_name', 'operate_first_name', 'source_flag',
                            'operate_idx', 'create_date', 'last_update_date', 'id_number', 'hospital_code',
                            'update_date']

form_basic_info = ['empi', 'empi_old', 'patient_no', 'encounter_id', 'patient_name', 'sex', 'age', 'hzly',
                   'marriage_id', 'marriage_name', 'marriage', 'id_number', 'height', 'weight', 'birth',
                   'in_hospital_time', 'hospital_name', 'hospital_code', 'inpatient_number', 'outpatient_number',
                   'hukou_province_code', 'hukou_province_name', 'hukou_city_code', 'hukou_city_name',
                   'hukou_county_code', 'hukou_county_name', 'hukou_addr', 'native_province_code',
                   'native_province_name', 'native_city_code', 'native_city_name', 'current_province_code',
                   'current_province_name', 'current_city_code', 'current_city_name', 'current_county_code',
                   'current_county_name', 'current_addr', 'current_tel', 'contact_tel', 'pay_way', 'create_date']

form_electrocardio_info = ['empi', 'empi_old', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                           'report_name', 'report_id', 'ts_exam', 'exam_name', 'exam_find', 'exam_conclusion',
                           'id_number', 'hospital_code', 'create_date']

form_labinfo = ['empi', 'empi_old', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                'lab_generic_id', 'ts_test', 'ts_draw', 'test_item_code', 'test_item_name', 'print_value',
                'result_value', 'result_unit', 'reference_text', 'abnormal_flag', 'abnormal_flag_name', 'id_number',
                'hospital_code', 'create_date']

form_labinfo2 = ['empi', 'empi_old', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                'lab_generic_id', 'ts_test', 'ts_draw', 'test_item_code', 'test_item_name','print_value',
                'result_value', 'result_unit', 'reference_text', 'abnormal_flag', 'abnormal_flag_name', 'id_number',
                'hospital_code', 'create_date','report_name', 'table_item_name', 'ts_draw_var']

form_lis_follow = ['empi', 'patient_no', 'encounter_id', 'outpatient_number', 'encounter_type', 'question_id',
                   'lab_generic_id', 'ts_test', 'ts_draw', 'reportname', 'test_item_code', 'test_item_name',
                   'print_value', 'result_value', 'result_unit', 'reference_text', 'abnormal_flag',
                   'abnormal_flag_name', 'id_number', 'hospital_code', 'create_date']

form_medication_orders = ['empi', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                          'in_hospital_time', 'inhospital_dept_name', 'question_id', 'medication_type',
                          'master_orders_id', 'orders_attribute_id', 'orders_attribute_name', 'orders_type_code',
                          'orders_type_name', 'project_type_code', 'project_type_name', 'orders_code', 'orders_name',
                          'origin_name', 'specifications', 'dosage', 'dosage_unit', 'project_number',
                          'project_number_unit', 'frequency', 'pc', 'pathway', 'write_recipe_time',
                          'orders_start_time', 'stop_time', 'id_number', 'hospital_code', 'create_date']

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

form_radiology_info = ['empi', 'empi_old', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                       'report_name', 'jclx', 'ts_exam', 'exam_name', 'body_site', 'jcbw', 'exam_method', 'exam_tech',
                       'exam_find', 'exam_conclusion', 'report_id', 'id_number', 'hospital_code', 'create_date']

form_radiotherapy_info = ['empi', 'empi_old', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                          'report_id', 'report_name', 'exam_name', 'ts_exam', 'exam_conclusion', 'id_number',
                          'hospital_code', 'create_date', 'report_class_name', 'jcxm']

form_ris_follow = ['empi', 'patient_no', 'encounter_id', 'outpatient_number', 'encounter_type', 'question_id',
                   'report_id', 'diagnosis', 'patient_imageid', 'report_name', 'exam_operator', 'ts_exam',
                   'body_site', 'exam_name', 'exam_tech', 'exam_method', 'exam_find', 'exam_conclusion',
                   'report_class_name', 'jclx', 'jcxm', 'jcbw', 'jcbw_qt', 'id_number', 'hospital_code', 'create_date']

form_scale_follow = ['empi', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number', 'encounter_type',
                     'lab_generic_id', 'ts_test', 'ts_draw', 'reportname', 'test_item_code', 'test_item_name',
                     'print_value', 'result_value', 'result_unit', 'reference_text', 'abnormal_flag',
                     'abnormal_flag_name', 'hospital_code', 'create_date']

form_ultrasound_info = ['empi', 'empi_old', 'patient_no', 'encounter_id', 'inpatient_number', 'outpatient_number',
                        'report_id', 'exam_name', 'report_name', 'body_site', 'ts_exam', 'exam_find',
                        'exam_conclusion', 'jcxm', 'jcbw', 'jcbw_qt', 'id_number', 'hospital_code', 'create_date']

entry_tables = {'cdr_patient_info': cdr_patient_info,
                'qs_drz_patient': qs_drz_patient,
                'qs_drz_patient_operation': qs_drz_patient_operation,
                'form_basic_info': form_basic_info,
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
                'form_operate_pathology_info': form_operate_pathology_info,
                'form_outpatient_visit': form_outpatient_visit,
                'form_pathology_info': form_pathology_info,
                'form_radiology_info': form_radiology_info,
                'form_radiotherapy_info': form_radiotherapy_info,
                'form_ris_follow': form_ris_follow,
                'form_ris_follow_ultra': form_ris_follow,
                'form_scale_follow': form_scale_follow,
                'form_ultrasound_info': form_ultrasound_info,
                }

entry_tables2 = {'form_labinfo_lymphocyte': form_labinfo2,
                'form_labinfo_lymphocyte_combination': form_labinfo2,
                'form_labinfo_subgroup_cytokine': form_labinfo2,
                'form_labinfo_urinalysis': form_labinfo2,
                'form_labinfo_urinary_protein': form_labinfo2,
                'form_labinfo_tumor_markers': form_labinfo2,
                'form_labinfo_coagulation': form_labinfo2,
                }

select_patient_num = "select distinct patient_num, empi from qs_drz_patient where hospital_code = '42502657200' " \
                     "and length(patient_num) = 9"
select_province = "select distinct code, name from dic_province"
select_city = "select distinct code, name from dic_city"
select_district = "select distinct code, name from dic_district"

