#!/bin/bash
/opt/module/apache-hive-1.2.1-bin/bin/beeline -u "jdbc:hive2://172.28.18.124:10000" -n skzbk -p skzbk123 -f /usr/local/kidney_tumor/rj_kidney_tumor_to_mysql.sql;

#同步hive新增数据到mysql
/usr/local/python3/bin/python3 -W ignore /usr/local/kidney_tumor/main.py

