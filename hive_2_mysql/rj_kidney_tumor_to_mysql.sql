set hive.execution.engine=spark;
-- DROP TABLE IF EXISTS skzbk.lxy_sa_medicalrecordmain_ls_temp;
-- CREATE TABLE skzbk.lxy_sa_medicalrecordmain_ls_temp AS 
insert into table skzbk.lxy_sa_medicalrecordmain_ls_temp 
select c.MedicalRecordID, A.inhospitaldeptcode, a.InHospitalDeptName, A.OutHospitalDeptName, A.OutHospitalWardName, a.EncounterID, a.patientno, a.idnumber, a.patientname, a.inpatientnumber, to_date(a.inhospitaltime) AS inhospitaltime, to_date(a.OutHospitalTime) AS OutHospitalTime, to_date(c.OperateDate) as OperateDate, c.OperateCODE, c.OperateName, c.operateidx, c.operatedoctorname, c.operatefirstname, concat_ws('；', collect_set(replace(b.diagnosecode,'，','，'))) as diagnosecode, concat_ws('；', collect_set(replace(b.diagnosename,'，','，'))) as diagnosename 
from cdr_v_MedicalRecordMain a join 
(SELECT * FROM cdr_v_MedicalRecordDiagnose WHERE TRIM(DiagnoseName) IN ('肾恶性肿瘤', '肾(除外肾盂)恶性肿瘤', '肾恶性肿瘤个人史', '肾恶性肿瘤', '肾肿瘤', '肾(肾盂)恶性肿瘤免疫治疗', '肾恶性肿瘤', '肾良性肿瘤', '肾恶性肿瘤(肾细胞癌)', '肾继发恶性肿瘤', '肾恶性肿瘤(肾细胞癌)', '肾(盂)恶性肿瘤术后随诊检查', '肾(盂)恶性肿瘤靶向治疗', '肾(盂)肿瘤免疫治疗', '肾恶性肿瘤术后化疗', '肾(盂)恶性肿瘤术后免疫治疗', '肾继发恶性肿瘤', '肾和肾盂继发性恶性肿瘤', '肾(盂)恶性肿瘤化疗后随诊', '肾(盂)恶性肿瘤术后靶向治疗', '肾(盂)恶性肿瘤术前新辅助化疗', '肾(盂)恶性肿瘤(术后)对症治疗', '肾占位(肿瘤)', '肾继发恶性肿瘤', '肾恶性肿瘤术后复发', '肾(盂)恶性肿瘤放疗', '肾、输尿管恶性肿瘤靶向治疗', '肾(肾盂)恶性肿瘤术后对症治疗', '肾(肾盂)恶性肿瘤介入治疗', '肾(盂)恶性肿瘤免疫治疗', '肾交界性肿瘤', '肾(肾盂)恶性肿瘤对症支持治疗', '肾多处恶性肿瘤', '肾恶性肿瘤介入治疗后随诊', '双侧肾恶性肿瘤', '肾(盂)恶性肿瘤(术后)免疫治疗', '肾、输尿管恶性肿瘤术后靶向治疗', '肾(盂)恶性肿瘤术前放疗', '肾恶性肿瘤(肾母细胞瘤)', '肾动态未定或动态未知的肿瘤', '肾混合性上皮间质肿瘤', '肾和肾盂继发性恶性肿瘤', '肾（肾盂）恶性肿瘤术前化疗', '肾(盂)恶性肿瘤术后放疗', '肾恶性肿瘤射频治疗', '肾恶性肿瘤术后随诊检查', '肾占位性病变待查', '肾占位性病变', '肾肿物', '肾透明细胞癌', '肾透明细胞癌', '肾乳头状肾细胞癌', '肾乳头状肾细胞癌', '肾嫌色细胞癌', '肾母细胞癌', '肾透明细胞腺癌', '肾(盂)恶性肿瘤术后化疗', '肾(肾盂)恶性肿瘤术后化疗', '肾输尿管恶性肿瘤', '肾(肾盂)恶性肿瘤化疗', '肾(盂)恶性肿瘤化疗', '肾肿瘤', '中肾样型透明细胞癌', '肾癌术后随诊检查', '肾细胞癌', '肾盏恶性肿瘤', '肾周恶性肿瘤', '肾恶性肿瘤', '左肾肿瘤', '乳头状肾细胞癌', '肾恶性肿瘤术后随诊检查', '转移性肾细胞癌', '左肾恶性肿瘤', '肾原位癌', '右侧肾肿瘤', '左侧肾肿瘤', '左肾盂肿瘤', '肾(盂)恶性肿瘤(术后)免疫治疗') and bigdata_data_tag = 1) b 
on a.id = b.MedicalRecordID join 
(select MedicalRecordID, OperateDate, OperateCODE, OperateName, operateidx, operatedoctorname, operatefirstname
FROM cdr_v_MedicalRecordOperate WHERE TRIM(OperateName) IN ('腹腔镜下肾部分切除术', '腹腔镜下肾部分切除术', '腹腔镜下肾癌根治术', '肾部分切除术', '腹腔镜下肾切除术', '腹腔镜下肾肿瘤射频消融', '肾癌根治术', '腹腔镜下单侧肾切除术', '腹腔镜下肾肿瘤射频消融术', '单侧肾切除术', '腹腔镜下肾癌根治术', '腹腔镜下肾根治切除术', '达芬奇下肾部分切除术', '腹腔镜下肾脏肿瘤切除术', '达芬奇下肾部分切除术', '腹腔镜下肾病损射频消融术', '腹腔镜下肾肿瘤射频消融', '达芬奇下肾部分切除术', '肾肿瘤射频消融术', '肾切除术单侧', '肾癌根治术', '腹腔镜下肾根治切除术术', '肾部分切除术', '肾病损切除术', '腹腔镜下肾周粘连松解术', '部分肾切除术', '肾根治切除术', '腹腔镜下肾(肿瘤)消融术', '肾肿瘤切除术', '达芬奇下肾癌根治术', '腹腔镜下肾脏肿瘤切除术', '肾静脉取栓术', '腹腔镜下肾病损切除术', '达芬奇下单侧肾切除术', '腹腔镜下肾根治性切除术', '肾肿瘤射频消融术', '下腔静脉取栓术', '肾病损射频消融术', 'B超引导下经皮肾肿瘤微波消融术', '肾病损切除术', '双侧肾切除术', '肾周围粘连松解术', '超声造影下肾肿瘤穿刺术', '达芬奇下肾癌根治术', '腹腔镜下肾病损切除术', '肾静脉切开取栓术', '肾根治切除术', '肾门淋巴结清扫术', '开放性肾活组织检查', '肾周围活检术', '腹腔镜下肾周或输尿管周围粘连的松解术', '肾探查术', '超声引导下肾病损射频消融术', '肾肿瘤切除术', '肾周围粘连松解', '腹腔镜下肾肿瘤冷冻消融术', '腹腔镜下肾探查术', '达芬奇下肾病损切除术', '肾病损或组织的其他和未特指切除', '腹腔镜下肾切除术', '下腔静脉病损切除术', '残留肾切除术', '肾周活组织检查', '肾周病损切除术', '腹腔镜中转开放肾癌根治术', '肾病损或组织的经皮消融术', '双侧肾切除术' ) and bigdata_data_tag = 1) c 
on a.id = c.MedicalRecordID 
WHERE TO_DATE(a.inhospitaltime) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) and a.id not in (select d.MedicalRecordID from skzbk.lxy_sa_medicalrecordmain_ls_temp d) and a.bigdata_data_tag = 1 
group by c.MedicalRecordID, A.inhospitaldeptcode, a.InHospitalDeptName, A.OutHospitalDeptName, A.OutHospitalWardName, a.EncounterID, a.patientno, a.idnumber, a.patientname, a.inpatientnumber, to_date(a.inhospitaltime), to_date(a.outhospitaltime), to_date(c.OperateDate), c.OperateCODE, c.OperateName, c.operateidx, c.operatedoctorname, c.operatefirstname;

-- 关联患者信息 
DROP TABLE IF EXISTS skzbk.lxy_sa_INPATIENTNUMBER_LS_TEMP0;
CREATE TABLE skzbk.lxy_sa_INPATIENTNUMBER_LS_TEMP0 AS 
-- insert into table skzbk.lxy_sa_INPATIENTNUMBER_LS_TEMP0 
SELECT NVL(B.EMPI,A.patientno) AS EMPI, B.EMPI AS EMPI_OLD, A.medicalrecordid, A.inhospitaldeptcode, A.inhospitaldeptname, A.outhospitaldeptname, A.outhospitalwardname, A.encounterid, A.patientno, A.idnumber, A.patientname, A.inpatientnumber, A.inhospitaltime, A.outhospitaltime, A.operatedate, A.operatecode, A.operatename, A.operateidx, A.operatedoctorname, A.operatefirstname, A.diagnosecode, A.diagnosename 
FROM skzbk.lxy_sa_medicalrecordmain_ls_temp A 
join (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1) B 
ON A.patientno = B.patientno;

-- 加工唯一键，并对同一人手术按手术时间排序
DROP TABLE IF EXISTS skzbk.lxy_sa_INPATIENTNUMBER_LS_TEMP;
CREATE TABLE skzbk.lxy_sa_INPATIENTNUMBER_LS_TEMP AS 
-- insert into table skzbk.lxy_sa_INPATIENTNUMBER_LS_TEMP 
SELECT B.*, CONCAT(B.MedicalRecordID, '#', B.operateidx) AS KEY_ID, CONCAT(YEAR(B.operatedate), '-', Lpad(B.YEAR_RN, 4, '0')) AS operationnum FROM 
(select a.*, row_number() over(partition by a.empi order by a.inhospitaltime, a.operatedate, a.operateidx) as rn, row_number() over(partition by year(a.operatedate) order by a.operatedate) as year_rn FROM skzbk.lxy_sa_INPATIENTNUMBER_LS_TEMP0 a) B;

-- 病案首页数据处理
DROP TABLE IF EXISTS skzbk.lxy_sa_MEDICALRECORDMAIN_TEMP;
CREATE TABLE skzbk.lxy_sa_MEDICALRECORDMAIN_TEMP AS
-- insert into table skzbk.lxy_sa_MEDICALRECORDMAIN_TEMP 
SELECT A.ID, A.PatientNo, A.RecordNumber, A.InpatientNumber, A.InHospitalWardCode, A.InHospitalWardName, A.PatientName AS XM, A.AGE AS NL, A.ShowAge, CASE WHEN TRIM(A.SEX) IN ('1', '2') THEN TRIM(A.SEX) WHEN A.IDNumber rlike '(^[1-9]\\d{5}(18|19|([23]\\d))\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$)|(^[1-9]\\d{5}\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}$)' THEN CASE WHEN PMOD(SUBSTR(A.IDNumber, 15, 3), 2) = 1 THEN '1' WHEN PMOD(SUBSTR(A.IDNumber, 15, 3), 2) = 0 THEN '2' ELSE '0' END ELSE '0' END AS XB, TO_DATE(A.Birth) AS Birth, A.NationName, nvl(B.CODE, '99') as mz, CASE WHEN TRIM(A.Marriage) IN ('1', '2', '3', '4') THEN TRIM(A.Marriage) ELSE '9' END AS HYZK, A.IDNumber AS sfzhm, NVL(A.JobName, '') AS zy, C.wj_jobcode, A.ContactName AS LXRXM, A.ContactRelationName AS LXRGX, A.ContactTel AS LXRDH, A.ABO, CASE WHEN TRIM(A.ABO) IN ('1', '2') THEN TRIM(A.ABO) WHEN TRIM(A.ABO) = '3' THEN '4' WHEN TRIM(A.ABO) = '4' THEN '3' ELSE '5' END AS XX, A.Rh AS OLD_RH, CASE WHEN TRIM(A.Rh) = '1' THEN '2' WHEN TRIM(A.Rh) = '2' THEN '1' ELSE '3' END AS RH 
FROM cdr_V_MedicalRecordMain A 
LEFT JOIN ( SELECT CODE,NAME FROM qs_wexzl_all_dic WHERE dic_type = '民族字典表' ) B 
ON A.NationNAME = B.NAME 
LEFT JOIN WEXZL_JOB_MAPPING_DIC C 
ON TRIM(A.JobCODE) = C.cdr_jobcode 
WHERE A.bigdata_data_tag = 1;

-- 关联病案首页(身份证补全，用基本信息表) 9175
DROP TABLE IF EXISTS skzbk.lxy_sa_INPATIENTNUMBER_FIRST;
CREATE TABLE skzbk.lxy_sa_INPATIENTNUMBER_FIRST AS
-- insert into table skzbk.lxy_sa_INPATIENTNUMBER_FIRST 
SELECT DISTINCT A.EMPI, A.patientno, A.inpatientnumber, A.InHospitalTime, A.InHospitalDeptCode, A.InHospitalDeptName, C.InHospitalWardCode, C.InHospitalWardName, A.OutHospitalDeptName, A.OutHospitalWardName, A.diagnosecode, A.diagnosename, A.OperateDate, A.RN AS OperateIdx, A.OperateCode, A.OperateName, C.XM AS NAME, replace(C.NL, '岁', '') AS AGE, C.XB AS SEX, C.Birth AS BIRTHDAY, NVL(NVL(C.SFZHM, B.IDNo), '') AS IDCARD, B.cardno as card_no, NVL(C.LXRXM, '') AS CONTACTNAME, NVL(C.LXRGX, '') AS CONTACTRELATION, NVL(C.LXRDH, '') AS CONTACTTEL, C.MZ AS NATION, C.wj_jobcode AS JOB, C.HYZK AS MARRIAGE, C.XX, C.RH 
FROM (SELECT * FROM skzbk.lxy_sa_INPATIENTNUMBER_LS_TEMP WHERE RN=1) A 
LEFT JOIN cdr_v_PatientBasicInformation B 
ON A.patientno = B.patientno and B.bigdata_data_tag = 1
JOIN skzbk.lxy_sa_MEDICALRECORDMAIN_TEMP C 
ON A.MedicalRecordID = C.id;

-- 加工患者编号=四位年份+当年手术日期的排序（四位）
DROP TABLE IF EXISTS skzbk.lxy_sa_INPATIENTNUMBER_LS;
CREATE TABLE skzbk.lxy_sa_INPATIENTNUMBER_LS AS 
-- insert into table skzbk.lxy_sa_INPATIENTNUMBER_LS 
SELECT CONCAT(YEAR(B.operatedate), '-', Lpad(B.YEAR_RN, 4, '0')) AS patient_num, B.* 
FROM (SELECT A.*, ROW_NUMBER() OVER(PARTITION BY YEAR(A.operatedate) ORDER BY A.operatedate, A.InHospitalTime, A.inpatientnumber) AS YEAR_RN FROM skzbk.lxy_sa_INPATIENTNUMBER_FIRST A) B;

-- 患者信息表cdr_patient_info：patient_no
-- DROP TABLE IF EXISTS skzbk.cdr_patient_info_temp;
-- CREATE TABLE skzbk.cdr_patient_info_temp AS 
insert into table skzbk.cdr_patient_info_temp 
SELECT distinct m.empi as empi, a.patientno as patient_no, a.patientname as patient_name, CASE WHEN a.sexname='男性' THEN '男' WHEN a.sexname='女性' THEN '女' ELSE '其他' END as sex, c.age as age, a.idno as id_number, a.cardno as card_no, d.AdmissionNumber as inpatient_number, d.inhospitaldatetime as in_hospital_date_time, d.PrimaryDiagnosis as inpatient_primary_diagnose, c.OperateDate as operate_date, e.PatID as outpatient_number, e.VisitDateTime as visit_date_time, e.PrimaryDiagnosis as outpatient_primary_diagnose, '42502657200' as hospital_code, c.HospitalName as hospital_name, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date 
FROM cdr_V_PatientBasicInformation a 
join (SELECT * FROM (select f.*, row_number() over(partition by f.patientno order by f.OperateDate desc) as rn from (SELECT b.patientno, b.age, b.HospitalCode, b.HospitalName, l.* from cdr_V_MedicalRecordMain b join cdr_V_MedicalRecordOperate l on b.id = l.MedicalRecordID WHERE year(b.InHospitalTime) in (2022,2023) and b.bigdata_data_tag = 1 and l.bigdata_data_tag = 1) f) g WHERE g.rn = 1) c 
on a.patientno = c.patientno 
join (SELECT * FROM (select h.*, row_number() over(partition by h.patientno order by h.inhospitaldatetime desc) as rn from cdr_V_InpatientVisitRecord h WHERE year(h.inhospitaldatetime) in (2022,2023) and h.bigdata_data_tag = 1) i WHERE i.rn = 1) d 
on a.patientno = d.patientno 
join (SELECT * FROM (select j.*, row_number() over(partition by j.patientno order by j.VisitDateTime desc) as rn from cdr_V_OutpatientVisitRecord j WHERE year(j.VisitDateTime) in (2022,2023) and j.bigdata_data_tag = 1) k WHERE k.rn = 1) e 
on a.patientno = e.patientno 
join (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1) m 
on a.patientno = m.patientno 
where year(c.OperateDate) in (2022,2023) 
and a.bigdata_data_tag = 1 
and a.patientno not in (select n.patient_no from skzbk.cdr_patient_info_temp n);

-- 待入组数据(姓名、身份证号不脱敏)qs_drz_patient：empi
-- DROP TABLE skzbk.qs_drz_patient_kidney_temp;
-- CREATE TABLE skzbk.qs_drz_patient_kidney_temp AS 
insert into table skzbk.qs_drz_patient_kidney_temp 
SELECT distinct patient_num, empi, inpatient_number, name, id_card, card_no, sex, age, hospital_id, group_id, in_hospital_datetime, in_hospital_dept_code, in_hospital_dept_name, in_hospital_ward_code, in_hospital_ward_name, outhospitaldeptname, outhospitalwardname, primary_diagnosis_code, primary_diagnosis, outpatient_number, visit_date_time, outpatient_primary_diagnose, operate_date, operate_code, operate_name, delete_flag, source_flag, operate_idx, create_date, hospital_code, insert_date 
from (SELECT A.patient_num as patient_num, A.empi as empi, A.inpatientnumber as inpatient_number, A.name as name, A.idcard as id_card, A.card_no as card_no, CASE WHEN A.SEX='1' THEN '男' WHEN A.SEX='2' THEN '女' ELSE '未知' END as sex, A.age as age, 1 as hospital_id, 1 as group_id, A.inhospitaltime as in_hospital_datetime, A.InHospitalDeptCode as in_hospital_dept_code, A.InHospitalDeptName as in_hospital_dept_name, A.InHospitalWardCode as in_hospital_ward_code, A.InHospitalWardName as in_hospital_ward_name, A.OutHospitalDeptName as outhospitaldeptname, A.OutHospitalWardName as outhospitalwardname, A.diagnosecode as primary_diagnosis_code, A.diagnosename as primary_diagnosis, B.patid as outpatient_number, B.visitdatetime as visit_date_time, B.primarydiagnosis as outpatient_primary_diagnose, A.OperateDate as operate_date, A.OperateCode as operate_code, A.OperateName as operate_name, '0' AS delete_flag, '0' AS source_flag, A.operateidx as operate_idx, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') AS create_date, '42502657200' as hospital_code, substr(current_timestamp,1,19) AS insert_date 
FROM (SELECT * from skzbk.lxy_sa_INPATIENTNUMBER_LS where empi not in (select d.empi from skzbk.qs_drz_patient_kidney_temp d)) A 
LEFT JOIN (SELECT * from (SELECT patientno, patid, visitdatetime, primarydiagnosis, row_number() over (partition by patientno order by visitdatetime desc) rank FROM cdr_v_OUTPATIENTVISITRECORD WHERE bigdata_data_tag = 1) c WHERE c.rank = 1) B 
ON A.patientno = B.patientno) d;

-- 待入组手术qs_drz_patient_operation：operation_num
-- DROP TABLE skzbk.qs_drz_patient_operation_kidney_temp;
-- CREATE TABLE skzbk.qs_drz_patient_operation_kidney_temp AS 
insert into table skzbk.qs_drz_patient_operation_kidney_temp 
select a.key_id as operation_num, a.empi as empi, a.inpatientnumber as inpatient_number, a.inhospitaltime as in_hospital_datetime, a.InHospitalDeptCode as in_hospital_dept_code, a.InHospitalDeptName as in_hospital_dept_name, a.diagnosecode as primary_diagnosis_code, a.diagnosename as primary_diagnosis, a.operatedate as operate_date, a.operatecode as operate_code, a.operatename as operate_name, b.operate_process, CASE WHEN a.operatename LIKE '%腹腔镜%' THEN '腹腔镜手术' WHEN a.operatename LIKE '%达芬奇下%' THEN '机器人辅助手术' ELSE '开放手术' end as sslx, CASE WHEN a.operatename LIKE '%肾病损切除术%' or a.operatename LIKE '%肾部分切除术%' or a.operatename LIKE '%部分肾切除术%' THEN '肾部分切除术' WHEN a.operatename LIKE '%肾根治切除术%' or a.operatename LIKE '%单侧肾切除术%' THEN '根治性肾切除术' WHEN a.operatename LIKE '%肾病损射频消融术%' THEN '微波辅助肿瘤剜除术' WHEN a.operatename LIKE '%下腔静脉取栓术%' or a.operatename LIKE '%上腔静脉取栓术%' or a.operatename LIKE '%肾静脉切开取栓术%' or a.operatename LIKE '%肾动脉切开取栓术%' THEN '瘤栓取出术' ELSE NULL end as ssfs, a.operatedoctorname as operate_doctor_name, a.operatefirstname as operate_first_name, '0' AS source_flag, A.operateidx AS operate_idx, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') AS create_date, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') AS last_update_date, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') AS update_date, a.idnumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') AS insert_date 
FROM skzbk.lxy_sa_INPATIENTNUMBER_LS_TEMP a 
LEFT JOIN (SELECT DISTINCT aa.HDSD0006144 as inpatientnumber, aa.HDSD0006073 as operate_process, bb.HDSD0006078 as operate_time FROM cdr_tb_ybssjl aa join cdr_tb_ybssjl_ss bb on aa.id = bb.refid and aa.bigdata_data_tag = 1 and bb.bigdata_data_tag = 1) b 
on replace(a.operatedate, '-', '') = substr(b.operate_time, 1, 8) and a.inpatientnumber = b.inpatientnumber 
WHERE a.key_id NOT IN (SELECT B.operation_num FROM skzbk.qs_drz_patient_operation_kidney_temp B);

-- 表单一基础信息form_basic_info：empi
-- DROP TABLE skzbk.form_basic_info_kidney_temp;
-- CREATE TABLE skzbk.form_basic_info_kidney_temp AS 
insert into table skzbk.form_basic_info_kidney_temp 
SELECT * FROM 
(SELECT g.*, row_number() over (partition by g.empi order by g.in_hospital_time desc) num FROM 
(SELECT distinct 
NVL(i.EMPI, i.PatientNo) AS empi, i.EMPI AS empi_old, a.PatientNo as patient_no, a.EncounterID as encounter_id, a.PatientName as patient_name, a.Sex as sex, REPLACE(a.Age, '岁', '') as age, b.patienttype as hzly, b.MarriageID as marriage_id, b.MarriageName as marriage_name, a.Marriage as marriage, a.IDNumber as id_number, d.value as height, f.value as weight, a.Birth as birth, a.InHospitalTime as in_hospital_time, a.HospitalName as hospital_name, '42502657200' as hospital_code, a.InpatientNumber as inpatient_number, a.OutpatientNumber as outpatient_number, a.HukouProvinceCode as hukou_province_code, a.HukouProvinceName as hukou_province_name, a.HukouCityCode as hukou_city_code, a.HukouCityName as hukou_city_name, a.HukouCountyCode as hukou_county_code, a.HukouCountyName as hukou_county_name, a.HukouAddr as hukou_addr, a.NativeProvinceCode as native_province_code, a.NativeProvinceName as native_province_name, a.NativeCityCode as native_city_code, a.NativeCityName as native_city_name, a.CurrentProvinceCode as current_province_code, a.CurrentProvinceName as current_province_name, a.CurrentCityCode as current_city_code, a.CurrentCityName as current_city_name, a.CurrentCountyCode as current_county_code, a.CurrentCountyName as current_county_name, a.CurrentAddr as current_addr, a.CurrentTel as current_tel, a.ContactTel as contact_tel, a.PayWay as pay_way, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date 
FROM (SELECT * FROM cdr_v_MedicalRecordMain where inpatientnumber in (SELECT inpatient_number FROM skzbk.qs_drz_patient_kidney_temp) and bigdata_data_tag = 1) a 
LEFT JOIN (select * from (SELECT patientno, patienttype, MarriageID, MarriageName, row_number() over (partition by patientno order by datacreatetime desc) rank FROM cdr_V_PatientBasicInformation WHERE bigdata_data_tag = 1) cc WHERE cc.rank = 1) b 
ON a.PatientNo=b.PatientNo 
join (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1) i 
ON A.patientno = i.patientno
LEFT JOIN 
(select * from (SELECT patientno, encounterid, operatedate, Value, row_number() over (partition by patientno, encounterid order by operatedate desc) rank FROM cdr_v_PATIENTVITALSIGN WHERE VitalSignId='3024' AND Value>=50 AND Value<=250 and bigdata_data_tag = 1) c WHERE c.rank = 1) d -- 身高
ON a.PatientNo=d.PatientNo and a.encounterid = d.encounterid 
LEFT JOIN 
(select * from (SELECT patientno, encounterid, operatedate, Value, row_number() over (partition by patientno, encounterid order by operatedate desc) rank FROM cdr_v_PATIENTVITALSIGN WHERE VitalSignId='1014' AND Value>=20 AND Value<=200 and bigdata_data_tag = 1) e WHERE e.rank = 1) f -- 体重
ON a.PatientNo=f.PatientNo and a.encounterid = f.encounterid 
WHERE TO_DATE(a.InHospitalTime) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120)) g) h 
WHERE h.num = 1 AND h.empi NOT in (SELECT k.empi from skzbk.form_basic_info_kidney_temp k);

-- 表单十一：心电图检查form_electrocardio_info：p_id
-- DROP TABLE skzbk.form_electrocardio_info_kidney_temp;
-- CREATE TABLE skzbk.form_electrocardio_info_kidney_temp AS 
insert into table skzbk.form_electrocardio_info_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, a.ReportName as report_name, b.id as p_id, b.ReportID as report_id, a.TSExam as ts_exam, b.ExamName as exam_name, b.ExamFind as exam_find, b.ExamConclusion as exam_conclusion, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.reportclasscode as report_class_code, a.reportclassname as report_class_name 
FROM cdr_V_OTHERREPORT a -- 心电图报告（ElectrocardioReport）
JOIN cdr_V_OTHERRESULT b -- 心电图报告项目结果（ElectrocardioResult）
ON a.id = b.ReportID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (a.reportclasscode = 'Electrocardio' or a.reportclasscode = 'ECG') AND TO_DATE(a.TSExam) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_electrocardio_info_kidney_temp e);

-- 表单十二：实验室检查 1生化form_labinfo_biochemistry：p_id
-- DROP TABLE skzbk.form_labinfo_biochemistry_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_biochemistry_kidney_temp AS 
insert into table skzbk.form_labinfo_biochemistry_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName in ('淀粉酶', '淀粉酶(血)') THEN '血淀粉酶' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = '血淀粉酶' or b.TestItemName = '淀粉酶(血)' or b.TestItemName = '淀粉酶') and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_biochemistry_kidney_temp e);

-- 表单十二：实验室检查 2C反应蛋白form_labinfo_creactiveprotein：p_id
-- DROP TABLE skzbk.form_labinfo_creactiveprotein_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_creactiveprotein_kidney_temp AS 
insert into table skzbk.form_labinfo_creactiveprotein_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName in ('C-反应蛋白', 'C反应蛋白(CRP)') THEN 'C反应蛋白' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = 'C反应蛋白' or b.TestItemName = 'C-反应蛋白' or b.TestItemName = 'C反应蛋白(CRP)') and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_creactiveprotein_kidney_temp e);

-- 表单十二：实验室检查 3粪常规form_labinfo_dungroutine：p_id
-- DROP TABLE skzbk.form_labinfo_dungroutine_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_dungroutine_kidney_temp AS 
insert into table skzbk.form_labinfo_dungroutine_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName = '形状' THEN '性状' WHEN b.TestItemName = '粪便隐血(免疫法)' THEN '粪隐血试验' WHEN b.TestItemName in ('粪便转铁蛋白测定', '粪便转铁蛋白') THEN '粪转铁蛋白' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = '颜色' or b.TestItemName = '形状' or b.TestItemName = '性状' or b.TestItemName = '红细胞' or b.TestItemName = '白细胞' or b.TestItemName = '吞噬细胞' or b.TestItemName = '酵母菌' or b.TestItemName = '虫卵' or b.TestItemName = '油滴' or b.TestItemName = '粪隐血试验' or b.TestItemName = '粪便隐血(免疫法)' or b.TestItemName = '粪转铁蛋白' or b.TestItemName = '粪便转铁蛋白测定' or b.TestItemName = '粪便转铁蛋白') 
and a.SpecimenClassName like '%粪%' and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_dungroutine_kidney_temp e);

-- 表单十二：实验室检查 4电解质form_labinfo_electrolyte：p_id
-- DROP TABLE skzbk.form_labinfo_electrolyte_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_electrolyte_kidney_temp AS 
insert into table skzbk.form_labinfo_electrolyte_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName in ('钾', '钾（全血）', '钾(全血)') THEN '血钾' WHEN b.TestItemName in ('钠', '钠（全血）', '钠(全血)') THEN '血钠' WHEN b.TestItemName in ('钙', '钙（全血）', '钙(全血)', '钙离子') THEN '血钙' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = '血钾' or b.TestItemName = '钾' or b.TestItemName = '钾（全血）' or b.TestItemName = '钾(全血)' or b.TestItemName = '血钠' or b.TestItemName = '钠' or b.TestItemName = '钠（全血）' or b.TestItemName = '钠(全血)' or b.TestItemName = '血钙' or b.TestItemName = '钙' or b.TestItemName = '钙（全血）' or b.TestItemName = '钙(全血)' or b.TestItemName = '钙离子') and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_electrolyte_kidney_temp e);

-- 表单十二：实验室检查 5血沉form_labinfo_esr：p_id
-- DROP TABLE skzbk.form_labinfo_esr_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_esr_kidney_temp AS 
insert into table skzbk.form_labinfo_esr_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName = '红细胞沉降率ESR' THEN '血沉' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = '血沉' or b.TestItemName = '红细胞沉降率ESR') and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_esr_kidney_temp e);

-- 表单十二：实验室检查 6肝功能form_labinfo_liverfunction：p_id
-- DROP TABLE skzbk.form_labinfo_liverfunction_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_liverfunction_kidney_temp AS 
insert into table skzbk.form_labinfo_liverfunction_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName = '丙氨酸氨基转移酶(ALT)' THEN '丙氨酸氨基转移酶' WHEN b.TestItemName in ('天门冬氨酸氨基转移酶(AST)', '天门冬氨酸氨基转移酶（AST）') THEN '天门冬氨酸氨基转移酶' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = '乳酸脱氢酶' or b.TestItemName = '丙氨酸氨基转移酶' or b.TestItemName = '丙氨酸氨基转移酶(ALT)' or b.TestItemName = '天门冬氨酸氨基转移酶' or b.TestItemName = '天门冬氨酸氨基转移酶(AST)' or b.TestItemName = '天门冬氨酸氨基转移酶（AST）' or b.TestItemName = '总胆红素' or b.TestItemName = '直接胆红素' or b.TestItemName = '碱性磷酸酶') and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_liverfunction_kidney_temp e);

-- 表单十二：实验室检查 7心梗标志物form_labinfo_myocardialinfarctionmarkers：p_id
-- DROP TABLE skzbk.form_labinfo_myocardialinfarctionmarkers_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_myocardialinfarctionmarkers_kidney_temp AS 
insert into table skzbk.form_labinfo_myocardialinfarctionmarkers_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, b.TestItemName as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = '肌酸激酶同工酶' or b.TestItemName = '肌红蛋白' or b.TestItemName = '肌钙蛋白I') and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_myocardialinfarctionmarkers_kidney_temp e);

-- 表单十二：实验室检查 8肾功能+胱抑素Cform_labinfo_renalfunction：p_id
-- DROP TABLE skzbk.form_labinfo_renalfunction_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_renalfunction_kidney_temp AS 
insert into table skzbk.form_labinfo_renalfunction_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName = '尿素' THEN '尿素氮' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = '肌酐' or b.TestItemName = '尿素氮' or b.TestItemName = '尿素' or b.TestItemName = '胱抑素C') and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_renalfunction_kidney_temp e);

-- 表单十二：实验室检查 9甲状腺与旁腺类form_labinfo_thyroidparathyroids：p_id
-- DROP TABLE skzbk.form_labinfo_thyroidparathyroids_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_thyroidparathyroids_kidney_temp AS 
insert into table skzbk.form_labinfo_thyroidparathyroids_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName = '促甲状腺激素(TSH)' THEN 'TSH' WHEN b.TestItemName = '总T3' THEN 'T3' WHEN b.TestItemName = '总T4' THEN 'T4' WHEN b.TestItemName = '游离T3' THEN 'FT3' WHEN b.TestItemName = '游离T4' THEN 'FT4' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = 'TSH' or b.TestItemName = '促甲状腺激素(TSH)' or b.TestItemName = 'T3' or b.TestItemName = '总T3' or b.TestItemName = 'T4' or b.TestItemName = '总T4' or b.TestItemName = 'FT3' or b.TestItemName = '游离T3' or b.TestItemName = 'FT4' or b.TestItemName = '游离T4') and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_thyroidparathyroids_kidney_temp e);

-- 表单十二：实验室检查 10尿液分析组合form_labinfo_urineanalysis：p_id
-- DROP TABLE skzbk.form_labinfo_urineanalysis_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_urineanalysis_kidney_temp AS 
insert into table skzbk.form_labinfo_urineanalysis_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName IN ('尿蛋白质', '尿蛋白(PRO)') THEN '尿蛋白' WHEN b.TestItemName in ('红细胞(镜检)', '镜检红细胞') THEN '尿RBC' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = '尿蛋白' or b.TestItemName = '尿蛋白质' or b.TestItemName = '尿蛋白(PRO)' or b.TestItemName = '尿RBC' or b.TestItemName = '镜检红细胞' or b.TestItemName = '红细胞(镜检)') and a.SpecimenClassName LIKE '%尿%' and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_urineanalysis_kidney_temp e);

-- 表单十二：实验室检查 11全血细胞分析form_labinfo_wholeblood：p_id
-- DROP TABLE skzbk.form_labinfo_wholeblood_kidney_temp;
-- CREATE TABLE skzbk.form_labinfo_wholeblood_kidney_temp AS 
insert into table skzbk.form_labinfo_wholeblood_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName IN ('嗜中性粒细胞绝对值', '嗜中性细胞绝对值') THEN '中性粒细胞' WHEN b.TestItemName = '血小板计数' THEN '血小板' WHEN b.TestItemName = '红细胞计数' THEN '红细胞' WHEN b.TestItemName = '白细胞计数' THEN '白细胞' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.Tsdraw as ts_draw 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (b.TestItemName = '血红蛋白' or b.TestItemName = '嗜中性粒细胞绝对值' or b.TestItemName = '嗜中性细胞绝对值' or b.TestItemName = '血小板计数' or b.TestItemName = '红细胞计数' or b.TestItemName = '白细胞计数') and a.SpecimenClassName LIKE '%血%' and (a.TSdraw is not null or a.TSTest is not null) 
AND TO_DATE(a.TSTest) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_labinfo_wholeblood_kidney_temp e);

-- 随访实验室检查 form_lis_follow：p_id
-- DROP TABLE skzbk.form_lis_follow_kidney_temp;
-- CREATE TABLE skzbk.form_lis_follow_kidney_temp AS 
insert into table skzbk.form_lis_follow_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, c.PatientNo as patient_no, c.id as encounter_id, c.patid as outpatient_number, a.EncounterType as encounter_type, case when (b.TestItemName = '血红蛋白' or b.TestItemName = '嗜中性粒细胞绝对值' or b.TestItemName = '嗜中性细胞绝对值' or b.TestItemName = '血小板计数' or b.TestItemName = '红细胞计数' or b.TestItemName = '白细胞计数') and a.SpecimenClassName LIKE '%血%' then 12 when b.TestItemName = '乳酸脱氢酶' or b.TestItemName = '丙氨酸氨基转移酶' or b.TestItemName = '丙氨酸氨基转移酶(ALT)' or b.TestItemName = '天门冬氨酸氨基转移酶' or b.TestItemName = '天门冬氨酸氨基转移酶(AST)' or b.TestItemName = '天门冬氨酸氨基转移酶（AST）' or b.TestItemName = '总胆红素' or b.TestItemName = '直接胆红素' or b.TestItemName = '碱性磷酸酶' then 13 when b.TestItemName = '肌酐' or b.TestItemName = '尿素氮' or b.TestItemName = '尿素' or b.TestItemName = '胱抑素C' then 14 when b.TestItemName = '血钾' or b.TestItemName = '钾' or b.TestItemName = '钾（全血）' or b.TestItemName = '钾(全血)' or b.TestItemName = '血钠' or b.TestItemName = '钠' or b.TestItemName = '钠（全血）' or b.TestItemName = '钠(全血)' or b.TestItemName = '血钙' or b.TestItemName = '钙' or b.TestItemName = '钙（全血）' or b.TestItemName = '钙(全血)' or b.TestItemName = '钙离子' then 15 when b.TestItemName = 'C反应蛋白' or b.TestItemName = 'C-反应蛋白' or b.TestItemName = 'C反应蛋白(CRP)' then 16 when (b.TestItemName = '尿蛋白' or b.TestItemName = '尿蛋白质' or b.TestItemName = '尿蛋白(PRO)' or b.TestItemName = '尿RBC' or b.TestItemName = '镜检红细胞' or b.TestItemName = '红细胞(镜检)') and a.SpecimenClassName LIKE '%尿%' then 17 when b.TestItemName = 'TSH' or b.TestItemName = '促甲状腺激素(TSH)' or b.TestItemName = 'T3' or b.TestItemName = '总T3' or b.TestItemName = 'T4' or b.TestItemName = '总T4' or b.TestItemName = 'FT3' or b.TestItemName = '游离T3' or b.TestItemName = 'FT4' or b.TestItemName = '游离T4' then 18 when b.TestItemName = '肌酸激酶同工酶' or b.TestItemName = '肌红蛋白' or b.TestItemName = '肌钙蛋白I' then 19 when b.TestItemName = '血淀粉酶' or b.TestItemName = '淀粉酶(血)' or b.TestItemName = '淀粉酶' then 20 when (b.TestItemName = '颜色' or b.TestItemName = '形状' or b.TestItemName = '性状' or b.TestItemName = '红细胞' or b.TestItemName = '白细胞' or b.TestItemName = '吞噬细胞' or b.TestItemName = '酵母菌' or b.TestItemName = '虫卵' or b.TestItemName = '油滴' or b.TestItemName = '粪隐血试验' or b.TestItemName = '粪便隐血(免疫法)' or b.TestItemName = '粪转铁蛋白' or b.TestItemName = '粪便转铁蛋白测定' or b.TestItemName = '粪便转铁蛋白') 
and a.SpecimenClassName like '%粪%' then 21 when b.TestItemName = '血沉' or b.TestItemName = '红细胞沉降率ESR' then 22 else NULL end as question_id, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.Tsdraw as ts_draw, a.ReportName, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName IN ('嗜中性粒细胞绝对值', '嗜中性细胞绝对值') THEN '中性粒细胞' WHEN b.TestItemName = '血小板计数' THEN '血小板' WHEN b.TestItemName = '红细胞计数' THEN '红细胞' WHEN b.TestItemName = '白细胞计数' THEN '白细胞' WHEN b.TestItemName = '丙氨酸氨基转移酶(ALT)' THEN '丙氨酸氨基转移酶' WHEN b.TestItemName in ('天门冬氨酸氨基转移酶(AST)', '天门冬氨酸氨基转移酶（AST）') THEN '天门冬氨酸氨基转移酶' WHEN b.TestItemName = '尿素' THEN '尿素氮' WHEN b.TestItemName in ('钾', '钾（全血）', '钾(全血)') THEN '血钾' WHEN b.TestItemName in ('钠', '钠（全血）', '钠(全血)') THEN '血钠' WHEN b.TestItemName in ('钙', '钙（全血）', '钙(全血)', '钙离子') THEN '血钙' WHEN b.TestItemName in ('C-反应蛋白', 'C反应蛋白(CRP)') THEN 'C反应蛋白' WHEN b.TestItemName IN ('尿蛋白质', '尿蛋白(PRO)') THEN '尿蛋白' WHEN b.TestItemName in ('红细胞(镜检)', '镜检红细胞') THEN '尿RBC' WHEN b.TestItemName = '促甲状腺激素(TSH)' THEN 'TSH' WHEN b.TestItemName = '总T3' THEN 'T3' WHEN b.TestItemName = '总T4' THEN 'T4' WHEN b.TestItemName = '游离T3' THEN 'FT3' WHEN b.TestItemName = '游离T4' THEN 'FT4' WHEN b.TestItemName in ('淀粉酶', '淀粉酶(血)') THEN '血淀粉酶' WHEN b.TestItemName = '形状' THEN '性状' WHEN b.TestItemName = '粪便隐血(免疫法)' THEN '粪隐血试验' WHEN b.TestItemName in ('粪便转铁蛋白测定', '粪便转铁蛋白') THEN '粪转铁蛋白' WHEN b.TestItemName = '红细胞沉降率ESR' THEN '血沉' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, d.idno as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_OUTPATIENTVISITRECORD c -- 门诊就诊记录
ON a.PatientNo = c.PatientNo and a.encounterid = c.id and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno, idno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE TO_DATE(a.TSdraw) >= '2019-12-01' AND b.id NOT in (SELECT e.p_id from skzbk.form_lis_follow_kidney_temp e);

-- 用药医嘱form_medication_orders：p_id
-- DROP TABLE skzbk.form_medication_orders_kidney_temp;
-- CREATE TABLE skzbk.form_medication_orders_kidney_temp AS 
insert into table skzbk.form_medication_orders_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, c.InHospitalTime as in_hospital_time, c.InhospitalDeptName as inhospital_dept_name, 6 AS question_id, '1' AS medication_type, concat(a.id, '-' , 1) as p_id, a.MasterOrdersID as master_orders_id, a.OrdersAttributeId as orders_attribute_id, a.OrdersAttributeName as orders_attribute_name, a.OrdersTypeCode as orders_type_code, a.OrdersTypeName as orders_type_name, a.ProjectTypeCode as project_type_code, a.ProjectTypeName as project_type_name, a.OrdersCode as orders_code, a.OrdersName as origin_name, a.Specifications as specifications, a.Dosage as dosage, a.DosageUnit as dosage_unit, a.ProjectNumber as project_number, a.ProjectNumberUnit as project_number_unit, a.Frequency as frequency, e.frequency as pc, a.Pathway as pathway, a.WriteRecipeTime as write_recipe_time, a.OrdersStartTime AS orders_start_time, a.StopTime AS stop_time, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date 
FROM (SELECT * FROM cdr_V_MedicationOrders WHERE OrdersName LIKE '%索坦%' OR OrdersName LIKE '%维全特%' OR OrdersName LIKE '%英立达%' OR OrdersName LIKE '%Cabometyx%' OR OrdersName LIKE '%乐卫玛%' OR OrdersName LIKE '%多吉美%' OR OrdersName LIKE '%飞尼妥%' OR OrdersName LIKE '%福可维%' OR OrdersName LIKE '%Welireg%' OR OrdersName LIKE '%拓益%' OR OrdersName LIKE '%百泽安%' OR OrdersName LIKE '%艾瑞卡%' OR OrdersName LIKE '%达伯舒%' OR OrdersName LIKE '%可瑞达%' OR OrdersName LIKE '%欧狄沃%' OR OrdersName LIKE '%逸沃%') a -- 住院用药医嘱（MedicationOrders）
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.inpatientvisitid = c.encounterid and a.bigdata_data_tag = 1 and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
LEFT JOIN skzbk.dict_frequency e 
ON a.Frequency=e.code 
WHERE TO_DATE(a.WriteRecipeTime) >= '2019-12-01' AND concat(a.id, '-' , 1) NOT in (SELECT f.p_id from skzbk.form_medication_orders_kidney_temp f);

-- 处方用药医嘱form_medication_orders：p_id
insert into table skzbk.form_medication_orders_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, c.PatientNo as patient_no, c.id as encounter_id, CAST(NULL AS string) as inpatient_number, c.patid as outpatient_number, c.visitdatetime as in_hospital_time, c.visitdeptname as inhospital_dept_name, 6 AS question_id, '2' AS medication_type, concat(b.id, '-' , 2) as p_id, b.id as master_orders_id, 'None' as orders_attribute_id, 'NoneNone' as orders_attribute_name, b.recipetypecode as orders_type_code, b.recipetypename as orders_type_name, b.ProjectTypeCode as project_type_code, b.ProjectTypeName as project_type_name, b.projectcode as orders_code, b.projectname as origin_name, b.Specifications as specifications, b.Dosage as dosage, b.DosageUnit as dosage_unit, b.ProjectNumber as project_number, b.ProjectNumberUnit as project_number_unit, b.Frequency as frequency, e.frequency as pc, b.Pathway as pathway, a.writerecipedatetime as write_recipe_time, a.writerecipedate AS orders_start_time, a.writerecipetime AS stop_time, d.idno as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date 
FROM cdr_V_OUTPATIENTRECIPE a -- 门诊处方
JOIN (SELECT * FROM cdr_V_OUTPATIENTRECIPEDETAIL WHERE projectname LIKE '%索坦%' OR projectname LIKE '%维全特%' OR projectname LIKE '%英立达%' OR projectname LIKE '%Cabometyx%' OR projectname LIKE '%乐卫玛%' OR projectname LIKE '%多吉美%' OR projectname LIKE '%飞尼妥%' OR projectname LIKE '%福可维%' OR projectname LIKE '%Welireg%' OR projectname LIKE '%拓益%' OR projectname LIKE '%百泽安%' OR projectname LIKE '%艾瑞卡%' OR projectname LIKE '%达伯舒%' OR projectname LIKE '%可瑞达%' OR projectname LIKE '%欧狄沃%' OR projectname LIKE '%逸沃%') b -- （门诊处方明细）
ON a.id = b.outpatientrecipeid and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_OUTPATIENTVISITRECORD c -- （门诊就诊记录）
ON a.PatientNo = c.PatientNo and a.outpatientvisitid = c.id and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno, idno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
LEFT JOIN skzbk.dict_frequency e 
ON b.Frequency=e.code 
WHERE TO_DATE(a.writerecipedatetime) >= '2019-12-01' AND concat(b.id, '-' , 2) NOT in (SELECT f.p_id from skzbk.form_medication_orders_kidney_temp f);

-- 表单四：手术信息+术后病理form_operate_pathology_info：p_id
-- DROP TABLE skzbk.form_operate_pathology_info_kidney_temp;
-- CREATE TABLE skzbk.form_operate_pathology_info_kidney_temp AS 
insert into table skzbk.form_operate_pathology_info_kidney_temp 
SELECT * FROM 
(SELECT *, row_number() over (partition by patient_no, operate_date, operate_code order by ts_exam asc) num FROM 
(SELECT distinct NVL(e.EMPI, e.PatientNo) AS empi, e.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, h.OperateDate as operate_date, h.OperateCode as operate_code, h.OperateName as operate_name, h.operate_process, CASE WHEN h.OperateName LIKE '%腹腔镜%' THEN '腹腔镜手术' WHEN h.OperateName LIKE '%达芬奇下%' THEN '机器人辅助手术' ELSE '开放手术' end as sslx, CASE WHEN h.OperateName LIKE '%肾病损切除术%' or h.OperateName LIKE '%肾部分切除术%' or h.OperateName LIKE '%部分肾切除术%' THEN '肾部分切除术' WHEN h.OperateName LIKE '%肾根治切除术%' or h.OperateName LIKE '%单侧肾切除术%' THEN '根治性肾切除术' WHEN h.OperateName LIKE '%肾病损射频消融术%' THEN '微波辅助肿瘤剜除术' WHEN h.OperateName LIKE '%下腔静脉取栓术%' or h.OperateName LIKE '%上腔静脉取栓术%' or h.OperateName LIKE '%肾静脉切开取栓术%' or h.OperateName LIKE '%肾动脉切开取栓术%' THEN '瘤栓取出术' ELSE NULL end as ssfs, h.EncounterType as encounter_type, h.id as p_id, h.Patientimageid as report_id, h.TSExam as ts_exam, h.ReportName as report_name, h.ExamFind as exam_find, h.ExamConclusion as exam_conclusion, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, h.reportclasscode as report_class_code, h.reportclassname as report_class_name 
FROM cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
JOIN 
(select d.*, a.EncounterType, a.Patientimageid, b.ReportID, a.TSExam, a.ReportName, b.ExamFind, b.ExamConclusion, a.reportclasscode, a.reportclassname from 
(SELECT f.*, cc.operate_process, g.patientno, g.encounterid from cdr_V_MedicalRecordOperate f join (select distinct id, patientno, inpatientnumber, encounterid from cdr_V_MedicalRecordMain WHERE bigdata_data_tag = 1) g on f.MedicalRecordID=g.id and f.bigdata_data_tag = 1 left join (SELECT aa.HDSD0006144 as inpatientnumber, aa.HDSD0006073 as operate_process, bb.HDSD0006078 as operate_time FROM cdr_tb_ybssjl aa join cdr_tb_ybssjl_ss bb on aa.id = bb.refid and aa.bigdata_data_tag = 1 and bb.bigdata_data_tag = 1) cc on replace(substr(f.operatedate, 0, 10), '-', '') = substr(cc.operate_time, 1, 8) and g.inpatientnumber = cc.inpatientnumber) d -- 病案手术表（MedicalRecordOperate）
LEFT JOIN 
(select * from cdr_V_OTHERREPORT where (reportclasscode = 'Pathology' or reportclasscode = 'BL') and ReportName not like '%免疫组化%' and bigdata_data_tag = 1) a -- 病理报告（PathologyReport）
ON d.PatientNo = a.PatientNo and d.encounterid = a.encounterid 
LEFT JOIN cdr_V_OTHERRESULT b -- 病理报告项目结果（PathologyResult）
ON a.id = b.ReportID and b.bigdata_data_tag = 1 
where (a.tsexam is NULL or d.Operatedate <= a.tsexam) AND TRIM(d.OperateName) IN ('腹腔镜下肾部分切除术', '腹腔镜下肾部分切除术', '腹腔镜下肾癌根治术', '肾部分切除术', '腹腔镜下肾切除术', '腹腔镜下肾肿瘤射频消融', '肾癌根治术', '腹腔镜下单侧肾切除术', '腹腔镜下肾肿瘤射频消融术', '单侧肾切除术', '腹腔镜下肾癌根治术', '腹腔镜下肾根治切除术', '达芬奇下肾部分切除术', '腹腔镜下肾脏肿瘤切除术', '达芬奇下肾部分切除术', '腹腔镜下肾病损射频消融术', '腹腔镜下肾肿瘤射频消融', '达芬奇下肾部分切除术', '肾肿瘤射频消融术', '肾切除术单侧', '肾癌根治术', '腹腔镜下肾根治切除术术', '肾部分切除术', '肾病损切除术', '腹腔镜下肾周粘连松解术', '部分肾切除术', '肾根治切除术', '腹腔镜下肾(肿瘤)消融术', '肾肿瘤切除术', '达芬奇下肾癌根治术', '腹腔镜下肾脏肿瘤切除术', '肾静脉取栓术', '腹腔镜下肾病损切除术', '达芬奇下单侧肾切除术', '腹腔镜下肾根治性切除术', '肾肿瘤射频消融术', '下腔静脉取栓术', '肾病损射频消融术', 'B超引导下经皮肾肿瘤微波消融术', '肾病损切除术', '双侧肾切除术', '肾周围粘连松解术', '超声造影下肾肿瘤穿刺术', '达芬奇下肾癌根治术', '腹腔镜下肾病损切除术', '肾静脉切开取栓术', '肾根治切除术', '肾门淋巴结清扫术', '开放性肾活组织检查', '肾周围活检术', '腹腔镜下肾周或输尿管周围粘连的松解术', '肾探查术', '超声引导下肾病损射频消融术', '肾肿瘤切除术', '肾周围粘连松解', '腹腔镜下肾肿瘤冷冻消融术', '腹腔镜下肾探查术', '达芬奇下肾病损切除术', '肾病损或组织的其他和未特指切除', '腹腔镜下肾切除术', '下腔静脉病损切除术', '残留肾切除术', '肾周活组织检查', '肾周病损切除术', '腹腔镜中转开放肾癌根治术', '肾病损或组织的经皮消融术', '双侧肾切除术')) h 
ON c.id = h.MedicalRecordID and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) e 
ON c.PatientNo = e.PatientNo 
WHERE TO_DATE(h.OperateDate) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120)) i) last 
where last.num = 1 AND last.p_id NOT in (SELECT k.p_id from skzbk.form_operate_pathology_info_kidney_temp k);

-- 门诊就诊随访form_outpatient_visit：encounter_id(满足随访时间条件范围取最近一条)
-- DROP TABLE skzbk.form_outpatient_visit_kidney_temp;
-- CREATE TABLE skzbk.form_outpatient_visit_kidney_temp AS 
insert into table skzbk.form_outpatient_visit_kidney_temp 
SELECT * FROM 
(SELECT DISTINCT empi, patient_no, encounter_id, cardno, patient_name, age, patient_sex, outpatient_code, outpatient_name, category_code, category_name, visit_date, visit_time, visit_state_code, visit_state_name, visit_dept_code, visit_dept_name, patid, visit_state_mapid, outpatient_class_code, outpatient_class_name, visit_date_time, primary_diagnosis, primary_diagnosis_code, visit_deptid, hospital_code, create_date from 
(SELECT d.*, row_number() over (partition by d.empi, d.inpatientnumber, d.follow_time order by abs((UNIX_TIMESTAMP(d.visit_date_time) - UNIX_TIMESTAMP( d.follow_time))/3600), d.encounter_id) ranks from 
(SELECT DISTINCT c.empi, a.patientno as patient_no, a.id as encounter_id, a.cardno, a.patientname as patient_name, a.age, a.patientsex as patient_sex, a.outpatientcode as outpatient_code, a.outpatientname as outpatient_name, a.categorycode as category_code, a.categoryname as category_name, a.visitdate as visit_date, a.visittime as visit_time, a.visitstatecode as visit_state_code, a.visitstatename as visit_state_name, a.visitdeptcode as visit_dept_code, a.visitdeptname as visit_dept_name, a.patid, a.visitstatemapid as visit_state_mapid, a.outpatientclasscode as outpatient_class_code, a.outpatientclassname as outpatient_class_name, a.visitdatetime as visit_date_time, a.primarydiagnosis as primary_diagnosis, a.primarydiagnosiscode as primary_diagnosis_code, a.visitdeptid as visit_deptid, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, b.inpatientnumber, case when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 3), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 3), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 3), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 6), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 6), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 6), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 9), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 9), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 9), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 12), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 12), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 12), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 15), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 15), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 15), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 18), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 18), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 18), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 21), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 21), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 21), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 24), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 24), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 24), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 30), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 30), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 30), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 36), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 36), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 36), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 42), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 42), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 42), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 48), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 48), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 48), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 54), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 54), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 54), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 60), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 60), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 60), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 72), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 72), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 72), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 84), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 84), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 84), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 96), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 96), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 96), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 108), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 108), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 108), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 120), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 120), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 120), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 132), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 132), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 132), substr(b.outhospitaltime, 11, 9)) when a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 144), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 144), 14), substr(b.outhospitaltime, 11, 9)) then concat(add_months(b.outhospitaltime, 144), substr(b.outhospitaltime, 11, 9)) end as follow_time 
FROM CDR_V_OUTPATIENTVISITRECORD a 
JOIN (SELECT * FROM CDR_V_MEDICALRECORDMAIN WHERE id in (SELECT medicalrecordid FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) b 
ON a.PATIENTNO = b.PATIENTNO and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1 
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1) c 
ON a.PatientNo = c.PatientNo 
WHERE (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 3), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 3), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 6), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 6), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 9), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 9), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 12), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 12), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 15), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 15), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 18), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 18), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 21), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 21), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 24), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 24), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 30), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 30), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 36), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 36), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 42), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 42), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 48), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 48), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 54), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 54), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 60), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 60), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 72), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 72), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 84), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 84), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 96), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 96), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 108), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 108), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 120), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 120), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 132), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 132), 14), substr(b.outhospitaltime, 11, 9))) or (a.visitdatetime BETWEEN concat(date_sub(add_months(b.outhospitaltime, 144), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 144), 14), substr(b.outhospitaltime, 11, 9)))) d) e WHERE e.ranks = 1) f 
WHERE f.encounter_id NOT IN (SELECT g.encounter_id FROM skzbk.form_outpatient_visit_kidney_temp g);

-- 门诊就诊随访form_outpatient_visit：encounter_id(不满足随访时间条件范围)
insert into table skzbk.form_outpatient_visit_kidney_temp 
SELECT DISTINCT c.empi, a.patientno as patient_no, a.id as encounter_id, a.cardno, a.patientname as patient_name, a.age, a.patientsex as patient_sex, a.outpatientcode as outpatient_code, a.outpatientname as outpatient_name, a.categorycode as category_code, a.categoryname as category_name, a.visitdate as visit_date, a.visittime as visit_time, a.visitstatecode as visit_state_code, a.visitstatename as visit_state_name, a.visitdeptcode as visit_dept_code, a.visitdeptname as visit_dept_name, a.patid, a.visitstatemapid as visit_state_mapid, a.outpatientclasscode as outpatient_class_code, a.outpatientclassname as outpatient_class_name, a.visitdatetime as visit_date_time, a.primarydiagnosis as primary_diagnosis, a.primarydiagnosiscode as primary_diagnosis_code, a.visitdeptid as visit_deptid, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date 
FROM CDR_V_OUTPATIENTVISITRECORD a 
JOIN (SELECT * FROM CDR_V_MEDICALRECORDMAIN WHERE id in (SELECT medicalrecordid FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) b 
ON a.PATIENTNO = b.PATIENTNO and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1 
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1) c 
ON a.PatientNo = c.PatientNo 
WHERE (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 3), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 3), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 6), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 6), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 9), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 9), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 12), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 12), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 15), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 15), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 18), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 18), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 21), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 21), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 24), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 24), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 30), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 30), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 36), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 36), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 42), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 42), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 48), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 48), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 54), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 54), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 60), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 60), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 72), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 72), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 84), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 84), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 96), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 96), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 108), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 108), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 120), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 120), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 132), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 132), 14), substr(b.outhospitaltime, 11, 9))) AND (a.visitdatetime NOT BETWEEN concat(date_sub(add_months(b.outhospitaltime, 144), 14), substr(b.outhospitaltime, 11, 9)) AND concat(date_add(add_months(b.outhospitaltime, 144), 14), substr(b.outhospitaltime, 11, 9))) 
AND a.id NOT IN (SELECT d.encounter_id FROM skzbk.form_outpatient_visit_kidney_temp d);

-- 表单四：病理form_pathology_info：p_id
-- DROP TABLE skzbk.form_pathology_info_kidney_temp;
-- CREATE TABLE skzbk.form_pathology_info_kidney_temp AS 
insert into table skzbk.form_pathology_info_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, c.PatientNo as patient_no, a.PatientImageID AS patient_image_id, a.TSExam as ts_exam, a.ReportName as report_name, b.id as p_id, b.BodySite AS body_site, b.ExamFind as exam_find, b.ExamConclusion as exam_conclusion, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.reportclasscode as report_class_code, a.reportclassname as report_class_name 
FROM cdr_V_OTHERREPORT a -- 病理报告（PathologyReport）
JOIN cdr_V_OTHERRESULT b -- 病理报告项目结果（PathologyResult）
ON a.id = b.ReportID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (a.reportclasscode = 'Pathology' or a.reportclasscode = 'BL') 
AND TO_DATE(a.TSExam) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_pathology_info_kidney_temp e);

-- 表单九：放射：Hisform_radiology_info：p_id
-- DROP TABLE skzbk.form_radiology_info_kidney_temp;
-- CREATE TABLE skzbk.form_radiology_info_kidney_temp AS 
insert into table skzbk.form_radiology_info_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, a.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, a.ReportName as report_name, CASE WHEN a.ReportName LIKE '%CT%' AND a.ReportName not LIKE '%CTU%' THEN 'CT' WHEN a.ReportName LIKE '%MR%' THEN 'MR' ELSE '其他' end as jclx, a.TSExam as ts_exam, b.id as p_id, b.ExamName as exam_name, b.BodySite as body_site, CASE WHEN b.BodySite LIKE '%肾脏%' THEN '肾脏' WHEN b.BodySite LIKE '%输尿管%' THEN '输尿管' WHEN b.BodySite LIKE '%上腹部%' THEN '上腹部' WHEN b.BodySite LIKE '%CTU%' THEN 'CTU' WHEN b.BodySite LIKE '%肾CTA%' THEN '肾CTA' WHEN b.BodySite LIKE '%肾MR%' THEN '肾MR' WHEN b.BodySite LIKE '%MRU%' THEN 'MRU' WHEN b.BodySite LIKE '%胸部%' THEN '胸部' ELSE '其他' end as jcbw, b.ExamMethod as exam_method, b.ExamTech as exam_tech, b.ExamFind as exam_find, b.ExamConclusion as exam_conclusion, b.ReportID as report_id, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.reportclasscode as report_class_code, a.reportclassname as report_class_name 
FROM cdr_V_OTHERREPORT a -- 放射报告（RadiologyReport）其他检查报告（OtherReport）
JOIN cdr_V_OTHERRESULT b -- 放射报告项目结果（RadiologyResult）其他检查报告（OtherReport）
ON a.id = b.ReportID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (a.reportclasscode = 'Radiology' or a.reportclasscode = 'CT' or a.reportclasscode = 'MR') 
AND TO_DATE(a.TSExam) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_radiology_info_kidney_temp e);

-- 表单十：核医学检查form_radiotherapy_info：p_id
-- DROP TABLE skzbk.form_radiotherapy_info_kidney_temp;
-- CREATE TABLE skzbk.form_radiotherapy_info_kidney_temp AS 
insert into table skzbk.form_radiotherapy_info_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.ReportID as report_id, a.ReportName as report_name, a.examdeptcode as exam_dept_code, a.examdeptname as exam_dept_name, b.ExamName as exam_name, a.TSExam as ts_exam, b.BodySite as body_site, b.ExamMethod as exam_method, b.ExamTech as exam_tech, b.ExamConclusion as exam_conclusion, b.eyefind as eye_find, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.reportclasscode as report_class_code, a.reportclassname as report_class_name, CASE WHEN a.reportclassname = 'PET/CT全身检查' THEN 'PET/CT' WHEN b.ExamName in ('SPECT显像检查', 'SPECT显像检查(核医学)') or a.reportclassname in ('骨全身断层显像', '骨全身显像') THEN 'ECT' WHEN a.reportclassname in ('肾图+肾小球滤过率（GFR）显像', '肾图+肾有效血浆流量（ERPF）显像') THEN 'GFR' ELSE NULL end as jcxm 
FROM cdr_V_OTHERREPORT a -- 核医学报告（RadiotherapyReport）其他检查报告（OtherReport）
JOIN cdr_V_OTHERRESULT b -- 核医学报告项目结果（RadiotherapyResult）其他检查报告（OtherReport）
ON a.id = b.ReportID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (a.reportclasscode = 'Radiotherapy' or a.reportclassname LIKE '%PET/CT%' or a.reportclassname LIKE '%GFR%' or a.reportclassname LIKE '%肾图%' or a.reportname like '%SPECT%' or b.examname like '%SPECT%' or a.reportname like '%核医学报告%') 
AND TO_DATE(a.TSExam) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_radiotherapy_info_kidney_temp e);

-- 物理检查随访form_ris_follow：p_id
-- DROP TABLE skzbk.form_ris_follow_kidney_temp;
-- CREATE TABLE skzbk.form_ris_follow_kidney_temp AS 
insert into table skzbk.form_ris_follow_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, c.PatientNo as patient_no, c.id as encounter_id, c.patid as outpatient_number, a.EncounterType as encounter_type, CASE WHEN a.reportclasscode = 'Ultrasound' or a.reportclasscode = 'US' or a.reportclasscode = 'NUS' THEN 8 WHEN a.reportclasscode = 'Radiology' or a.reportclasscode = 'CT' or a.reportclasscode = 'MR' THEN 9 WHEN a.reportclasscode = 'Radiotherapy' or a.reportclassname LIKE '%PET/CT%' or a.reportclassname LIKE '%GFR%' or a.reportclassname LIKE '%肾图%' or a.reportname like '%SPECT%' or b.examname like '%SPECT%' or a.reportname like '%核医学报告%' THEN 10 WHEN a.reportclasscode = 'Electrocardio' or a.reportclasscode = 'ECG' THEN 11 ELSE NULL END as question_id, b.id as p_id, b.ReportID as report_id, a.diagnosis, a.patientimageid as patient_imageid, a.ReportName as report_name, a.examoperator as exam_operator, a.TSExam as ts_exam, b.BodySite as body_site, b.ExamName as exam_name, b.ExamTech as exam_tech, B.ExamMethod AS exam_method, b.ExamFind as exam_find, b.ExamConclusion as exam_conclusion, a.reportclasscode as report_class_code, a.reportclassname as report_class_name, CASE WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND a.ReportName LIKE '%CT%' AND a.ReportName not LIKE '%CTU%' THEN 'CT' WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND a.ReportName LIKE '%MR%' THEN 'MR' WHEN a.reportclasscode NOT IN ('Radiology', 'CT', 'MR') THEN NULL ELSE '其他' end as jclx, CASE WHEN a.reportclasscode IN ('Ultrasound', 'US', 'NUS') AND b.BodySite like '%输尿管%' THEN '输尿管' WHEN a.reportclasscode IN ('Ultrasound', 'US', 'NUS') AND b.BodySite like '%后腹膜淋巴结%' THEN '后腹膜淋巴结' WHEN a.reportclasscode IN ('Ultrasound', 'US', 'NUS') AND b.BodySite like '%肾静脉%' THEN '肾静脉' WHEN (a.reportclasscode = 'Radiotherapy' or a.reportclassname LIKE '%PET/CT%' or a.reportclassname LIKE '%GFR%' or a.reportclassname LIKE '%肾图%' or a.reportname like '%SPECT%' or b.examname like '%SPECT%' or a.reportname like '%核医学报告%') AND a.reportclassname = 'PET/CT全身检查' THEN 'PET/CT' WHEN (a.reportclasscode = 'Radiotherapy' or a.reportclassname LIKE '%PET/CT%' or a.reportclassname LIKE '%GFR%' or a.reportclassname LIKE '%肾图%' or a.reportname like '%SPECT%' or b.examname like '%SPECT%' or a.reportname like '%核医学报告%') AND (b.ExamName in ('SPECT显像检查', 'SPECT显像检查(核医学)') or a.reportclassname in ('骨全身断层显像', '骨全身显像')) THEN 'ECT' WHEN (a.reportclasscode = 'Radiotherapy' or a.reportclassname LIKE '%PET/CT%' or a.reportclassname LIKE '%GFR%' or a.reportclassname LIKE '%肾图%' or a.reportname like '%SPECT%' or b.examname like '%SPECT%' or a.reportname like '%核医学报告%') AND a.reportclassname in ('肾图+肾小球滤过率（GFR）显像', '肾图+肾有效血浆流量（ERPF）显像') THEN 'GFR' ELSE NULL end as jcxm, CASE WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND b.BodySite LIKE '%肾脏%' THEN '肾脏' WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND b.BodySite LIKE '%输尿管%' THEN '输尿管' WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND b.BodySite LIKE '%上腹部%' THEN '上腹部' WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND b.BodySite LIKE '%CTU%' THEN 'CTU' WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND b.BodySite LIKE '%肾CTA%' THEN '肾CTA' WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND b.BodySite LIKE '%肾MR%' THEN '肾MR' WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND b.BodySite LIKE '%MRU%' THEN 'MRU' WHEN a.reportclasscode IN ('Radiology', 'CT', 'MR') AND b.BodySite LIKE '%胸部%' THEN '胸部' WHEN a.reportclasscode NOT IN ('Radiology', 'CT', 'MR') THEN NULL ELSE '其他' end as jcbw, CAST(NULL AS string) as jcbw_qt, d.idno as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date 
FROM cdr_V_OTHERREPORT a -- B超报告（UltrasoundReport）
JOIN cdr_V_OTHERRESULT b -- B超报告项目结果（UltrasoundResult）
ON a.id = b.ReportID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_OUTPATIENTVISITRECORD c -- 门诊就诊记录
ON a.PatientNo = c.PatientNo and a.encounterid = c.id and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno, idno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE TO_DATE(a.TSExam) >= '2019-12-01' AND b.id NOT in (SELECT e.p_id from skzbk.form_ris_follow_kidney_temp e);

-- 评分随访form_scale_follow：p_id
-- DROP TABLE skzbk.form_scale_follow_kidney_temp;
-- CREATE TABLE skzbk.form_scale_follow_kidney_temp AS 
insert into table skzbk.form_scale_follow_kidney_temp 
SELECT * FROM 
(SELECT f.*, row_number() over (partition by f.empi, f.test_item_name order by f.ts_draw) num FROM 
(SELECT distinct NVL(d.EMPI, d.PatientNo) AS empi, c.PatientNo as patient_no, c.encounterid as encounter_id, c.inpatientnumber as inpatient_number, c.outpatientnumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.LabGenericID as lab_generic_id, a.TSTest as ts_test, a.Tsdraw as ts_draw, a.ReportName, a.SpecimenClassName, b.TestItemCode as test_item_code, CASE WHEN b.TestItemName in ('钙', '钙（全血）', '钙(全血)', '钙离子') THEN '血清校正钙' WHEN b.TestItemName IN ('嗜中性粒细胞绝对值', '嗜中性细胞绝对值') THEN '中性粒细胞计数绝对值' WHEN b.TestItemName = '血小板计数' THEN '血小板计数绝对值' WHEN b.TestItemName = '乳酸脱氢酶' THEN '乳酸脱氢酶LDH' ELSE b.TestItemName end as test_item_name, b.PrintValue as print_value, b.ResultValue as result_value, b.ResultUnit as result_unit, b.ReferenceText as reference_text, b.AbnormalFlag as abnormal_flag, NVL(NVL(b.AbnormalFlagName, b.AbnormalFlag), 'NA') as abnormal_flag_name, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date 
FROM cdr_V_LabGenericReport a -- 实验室检查报告（LabGenericReport）
JOIN cdr_V_LabGenericResult b -- 实验室检查报告项目结果（LabGenericResult）
ON a.id = b.LabGenericID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN (select * from cdr_V_MedicalRecordMain where id in (SELECT DISTINCT medicalrecordid FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) c -- 病案首页 
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1) d 
ON c.PatientNo=d.PatientNo 
WHERE ((b.TestItemName = '血红蛋白' or b.TestItemName = '嗜中性粒细胞绝对值' or b.TestItemName = '嗜中性细胞绝对值' or b.TestItemName = '血小板计数') and a.SpecimenClassName LIKE '%血%') or b.TestItemName = '血钙' or b.TestItemName = '钙' or b.TestItemName = '钙（全血）' or b.TestItemName = '钙(全血)' or b.TestItemName = '钙离子' or b.TestItemName = '乳酸脱氢酶') f) g 
WHERE g.num = 1 AND concat(g.empi, g.test_item_name) not in (SELECT concat(e.empi, e.test_item_name) from skzbk.form_scale_follow_kidney_temp e);

-- 表单八：B超form_ultrasound_info：p_id
-- DROP TABLE skzbk.form_ultrasound_info_kidney_temp;
-- CREATE TABLE skzbk.form_ultrasound_info_kidney_temp AS 
insert into table skzbk.form_ultrasound_info_kidney_temp 
SELECT distinct 
NVL(d.EMPI, d.PatientNo) AS empi, d.EMPI AS empi_old, c.PatientNo as patient_no, c.EncounterID as encounter_id, c.InpatientNumber as inpatient_number, c.OutpatientNumber as outpatient_number, a.EncounterType as encounter_type, b.id as p_id, b.ReportID as report_id, a.ReportName as report_name, b.ExamName as exam_name, b.BodySite as body_site, a.TSExam as ts_exam, b.ExamFind as exam_find, b.ExamConclusion as exam_conclusion, CASE WHEN b.BodySite like '%输尿管%' THEN '输尿管' WHEN b.BodySite like '%后腹膜淋巴结%' THEN '后腹膜淋巴结' WHEN b.BodySite like '%肾静脉%' THEN '肾静脉' ELSE NULL end as jcxm, c.IDNumber as id_number, '42502657200' as hospital_code, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as create_date, a.reportclasscode as report_class_code, a.reportclassname as report_class_name 
FROM cdr_V_OTHERREPORT a -- B超报告（UltrasoundReport）
JOIN cdr_V_OTHERRESULT b -- B超报告项目结果（UltrasoundResult）
ON a.id = b.ReportID and a.bigdata_data_tag = 1 and b.bigdata_data_tag = 1
JOIN cdr_V_MedicalRecordMain c -- 病案首页（MedicalRecordMain）
ON a.PatientNo = c.PatientNo and a.encounterid = c.encounterid and c.bigdata_data_tag = 1
JOIN (select distinct empi, patientno from dw.empi_cdw where effect_flag = 1 and patientno in (SELECT DISTINCT patientno FROM skzbk.lxy_sa_medicalrecordmain_ls_temp)) d 
ON c.PatientNo=d.PatientNo 
WHERE (a.reportclasscode = 'Ultrasound' or a.reportclasscode = 'US' or a.reportclasscode = 'NUS') 
AND TO_DATE(a.TSExam) > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),120) AND b.id NOT in (SELECT e.p_id from skzbk.form_ultrasound_info_kidney_temp e);


-- 患者信息表：cdr_patient_info
DROP TABLE skzbk.cdr_patient_info;
CREATE TABLE skzbk.cdr_patient_info AS 
SELECT * FROM skzbk.cdr_patient_info_temp WHERE patient_no not in (SELECT a.patient_no FROM skzbk.cdr_patient_info_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 待入组数据(姓名、身份证号不脱敏)：qs_drz_patient
DROP TABLE skzbk.qs_drz_patient_kidney;
CREATE TABLE skzbk.qs_drz_patient_kidney AS 
SELECT * FROM skzbk.qs_drz_patient_kidney_temp WHERE empi not in (SELECT a.empi FROM skzbk.qs_drz_patient_kidney_temp a WHERE a.insert_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 待入组手术：qs_drz_patient_operation
DROP TABLE skzbk.qs_drz_patient_operation_kidney;
CREATE TABLE skzbk.qs_drz_patient_operation_kidney AS 
SELECT * FROM skzbk.qs_drz_patient_operation_kidney_temp WHERE operation_num not in (SELECT a.operation_num FROM skzbk.qs_drz_patient_operation_kidney_temp a WHERE a.insert_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单一基础信息：form_basic_info
DROP TABLE skzbk.form_basic_info_kidney;
CREATE TABLE skzbk.form_basic_info_kidney AS 
SELECT * FROM skzbk.form_basic_info_kidney_temp WHERE empi not in (SELECT a.empi FROM skzbk.form_basic_info_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十一：心电图检查：form_electrocardio_info
DROP TABLE skzbk.form_electrocardio_info_kidney;
CREATE TABLE skzbk.form_electrocardio_info_kidney AS 
SELECT * FROM skzbk.form_electrocardio_info_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_electrocardio_info_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 生化1：form_labinfo_biochemistry
DROP TABLE skzbk.form_labinfo_biochemistry_kidney;
CREATE TABLE skzbk.form_labinfo_biochemistry_kidney AS 
SELECT * FROM skzbk.form_labinfo_biochemistry_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_biochemistry_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 C反应蛋白2：form_labinfo_creactiveprotein
DROP TABLE skzbk.form_labinfo_creactiveprotein_kidney;
CREATE TABLE skzbk.form_labinfo_creactiveprotein_kidney AS 
SELECT * FROM skzbk.form_labinfo_creactiveprotein_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_creactiveprotein_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 粪常规3：form_labinfo_dungroutine
DROP TABLE skzbk.form_labinfo_dungroutine_kidney;
CREATE TABLE skzbk.form_labinfo_dungroutine_kidney AS 
SELECT * FROM skzbk.form_labinfo_dungroutine_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_dungroutine_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 电解质4：form_labinfo_electrolyte
DROP TABLE skzbk.form_labinfo_electrolyte_kidney;
CREATE TABLE skzbk.form_labinfo_electrolyte_kidney AS 
SELECT * FROM skzbk.form_labinfo_electrolyte_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_electrolyte_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 血沉5：form_labinfo_esr
DROP TABLE skzbk.form_labinfo_esr_kidney;
CREATE TABLE skzbk.form_labinfo_esr_kidney AS 
SELECT * FROM skzbk.form_labinfo_esr_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_esr_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 肝功能6：form_labinfo_liverfunction
DROP TABLE skzbk.form_labinfo_liverfunction_kidney;
CREATE TABLE skzbk.form_labinfo_liverfunction_kidney AS 
SELECT * FROM skzbk.form_labinfo_liverfunction_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_liverfunction_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 心梗标志物7：form_labinfo_myocardialinfarctionmarkers
DROP TABLE skzbk.form_labinfo_myocardialinfarctionmarkers_kidney;
CREATE TABLE skzbk.form_labinfo_myocardialinfarctionmarkers_kidney AS 
SELECT * FROM skzbk.form_labinfo_myocardialinfarctionmarkers_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_myocardialinfarctionmarkers_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 肾功能+胱抑素C8：form_labinfo_renalfunction
DROP TABLE skzbk.form_labinfo_renalfunction_kidney;
CREATE TABLE skzbk.form_labinfo_renalfunction_kidney AS 
SELECT * FROM skzbk.form_labinfo_renalfunction_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_renalfunction_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 甲状腺与旁腺类9：form_labinfo_thyroidparathyroids
DROP TABLE skzbk.form_labinfo_thyroidparathyroids_kidney;
CREATE TABLE skzbk.form_labinfo_thyroidparathyroids_kidney AS 
SELECT * FROM skzbk.form_labinfo_thyroidparathyroids_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_thyroidparathyroids_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 尿液分析组合10：form_labinfo_urineanalysis
DROP TABLE skzbk.form_labinfo_urineanalysis_kidney;
CREATE TABLE skzbk.form_labinfo_urineanalysis_kidney AS 
SELECT * FROM skzbk.form_labinfo_urineanalysis_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_urineanalysis_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十二：实验室检查 全血细胞分析11：form_labinfo_wholeblood
DROP TABLE skzbk.form_labinfo_wholeblood_kidney;
CREATE TABLE skzbk.form_labinfo_wholeblood_kidney AS 
SELECT * FROM skzbk.form_labinfo_wholeblood_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_labinfo_wholeblood_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 检验随访：form_lis_follow
DROP TABLE skzbk.form_lis_follow_kidney;
CREATE TABLE skzbk.form_lis_follow_kidney AS 
SELECT * FROM skzbk.form_lis_follow_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_lis_follow_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 用药医嘱：form_medication_orders
DROP TABLE skzbk.form_medication_orders_kidney;
CREATE TABLE skzbk.form_medication_orders_kidney AS 
SELECT * FROM skzbk.form_medication_orders_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_medication_orders_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单四：手术信息+术后病理：form_operate_pathology_info
DROP TABLE skzbk.form_operate_pathology_info_kidney;
CREATE TABLE skzbk.form_operate_pathology_info_kidney AS 
SELECT * FROM skzbk.form_operate_pathology_info_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_operate_pathology_info_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 门诊就诊记录：form_outpatient_visit
DROP TABLE skzbk.form_outpatient_visit_kidney;
CREATE TABLE skzbk.form_outpatient_visit_kidney AS 
SELECT * FROM skzbk.form_outpatient_visit_kidney_temp WHERE encounter_id not in (SELECT a.encounter_id FROM skzbk.form_outpatient_visit_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单四：病理：form_pathology_info
DROP TABLE skzbk.form_pathology_info_kidney;
CREATE TABLE skzbk.form_pathology_info_kidney AS 
SELECT * FROM skzbk.form_pathology_info_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_pathology_info_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单九：放射：His：form_radiology_info
DROP TABLE skzbk.form_radiology_info_kidney;
CREATE TABLE skzbk.form_radiology_info_kidney AS 
SELECT * FROM skzbk.form_radiology_info_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_radiology_info_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单十：核医学检查：form_radiotherapy_info
DROP TABLE skzbk.form_radiotherapy_info_kidney;
CREATE TABLE skzbk.form_radiotherapy_info_kidney AS 
SELECT * FROM skzbk.form_radiotherapy_info_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_radiotherapy_info_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 检查随访：form_ris_follow
DROP TABLE skzbk.form_ris_follow_kidney;
CREATE TABLE skzbk.form_ris_follow_kidney AS 
SELECT * FROM skzbk.form_ris_follow_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_ris_follow_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 评分随访：form_scale_follow
DROP TABLE skzbk.form_scale_follow_kidney;
CREATE TABLE skzbk.form_scale_follow_kidney AS 
SELECT * FROM skzbk.form_scale_follow_kidney_temp WHERE concat(empi, test_item_name) not in (SELECT concat(a.empi, a.test_item_name) FROM skzbk.form_scale_follow_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

-- 表单八：B超：form_ultrasound_info
DROP TABLE skzbk.form_ultrasound_info_kidney;
CREATE TABLE skzbk.form_ultrasound_info_kidney AS 
SELECT * FROM skzbk.form_ultrasound_info_kidney_temp WHERE p_id not in (SELECT a.p_id FROM skzbk.form_ultrasound_info_kidney_temp a WHERE a.create_date < from_unixtime(unix_timestamp()-43200,'yyyy-MM-dd HH:mm:ss'));

