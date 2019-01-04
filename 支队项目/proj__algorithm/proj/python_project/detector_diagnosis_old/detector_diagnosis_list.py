#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#############################
# Created on Mon Nov 26 2018
# Description: detector quality on signal specialist
# @author:  DDD
#############################
import cx_Oracle
import logging
import os
import pandas as pd
import datetime as dt
import psycopg2
from proj.config.database import Postgres
from proj.config.database import Oracle


def call_oracle(cur_date_str, logger):
    detector_all = pd.DataFrame({})
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        ora = Oracle()
        # db = ora.db_conn()
        # print('oracle connect succeed.')
        # print('reaching datas...')

        sql = "select A.FSTR_INTERSECTID, to_char(round(A.SCORE*10)) SCORE, to_char(B.LANENUM) lanenum " \
              "FROM(SELECT FSTR_INTERSECTID, SCORE FROM AVAILABLE_SCORE WHERE FSTR_DATE = '{0}' ) A LEFT JOIN (" \
               "select count(*) lanenum, FSTR_INTERSECTID from HZ_SCATS_DETECTOR_STATE WHERE FSTR_DATE = '{0}' " \
               "GROUP BY FSTR_INTERSECTID ) B ON A.FSTR_INTERSECTID = B.FSTR_INTERSECTID " \
               "order by to_number(a.score) desc" \
            .format(cur_date_str)
        # print(sql1)  # '331', '325', '326', '327', '328', '329', '330', '331', '445', '187', '276'
        try:
            # cr = db.cursor()
            detector_all = ora.call_oracle_data(sql, fram=True)
            # ora.execute(sql1)
            # rs1 = ora.fetchall()
            # detector_all = pd.DataFrame(rs1)
            # detector_all.columns = ['FSTR_INTERSECTID', 'SCORE', 'LANENUM']
            # cr.close()
        except Exception as e:
            logger.error('when reaching detector_all, ' + str(e))
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return detector_all


def call_postgresql(logger):
    IntInfor = pd.DataFrame({})
    try:
        # conn = psycopg2.connect(database="signal_specialist", user="postgres", password="postgres",
        #                         host="192.168.20.46",
        #                         port="5432")
        pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
                              'host': "33.83.100.145", 'port': "5432"})
        # conn = pg.db_conn()
        # print('postgresql connect succeed.')
        # print('reaching datas...')
        try:
            sql = "select a.sys_code FSTR_INTERSECTID, a.node_id, b.node_name from pe_tobj_node_info a left join " \
                  "pe_tobj_node b on a.node_id = b.node_id where length(a.sys_code) < 5"
            # cr = conn.cursor()
            IntInfor = pg.call_pg_data(sql, fram=True)
            # rs1 = cr.fetchall()
            # IntInfor = pd.DataFrame(rs1)
            # IntInfor.columns = ['FSTR_INTERSECTID', 'node_name']
            # print(IntInfor)
            # input()
            # conn.close()
        except Exception as e:
            logger.error('when reaching interinfo, ' + str(e))
        finally:
            pg.db_close()
    except Exception as e:
        logger.error('when calling postgresql, ' + str(e))
    return IntInfor


def main_detector_quality(cur_date_str, logger):
    # cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
    # cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
    # print(cur_date_str)
    # cur_date_str = '2018-11-15'
    interInfor = call_postgresql(logger)
    # print(interInfor)
    detector_all = call_oracle(cur_date_str, logger)
    interInfor.rename(columns={'fstr_intersectid': 'FSTR_INTERSECTID'}, inplace=True)
    merge_detector_all = pd.DataFrame({})
    if not detector_all.empty and not interInfor.empty:
        # detector_all = detector_all.fillna(0)
        merge_detector_all = pd.merge(detector_all, interInfor, on='FSTR_INTERSECTID', how='left')
        # print(merge_detector_all)
        merge_detector_all = merge_detector_all.dropna(axis=0, how='any')
        # merge_detector_all = merge_detector_all.reset_index(drop=True)
        # print(merge_detector_all)
        merge_detector_all.rename(columns={'FSTR_INTERSECTID': 'scatsId', 'SCORE': 'score',
                                           'LANENUM': 'laneNum', 'node_name': 'intName', 'node_id': 'nodeId'}, inplace=True)
        # print(merge_detector_all)
    return merge_detector_all


if __name__ == '__main__':
    foldername = 'log'
    cur_path_string = os.path.abspath('.')
    if not os.path.exists(cur_path_string + "\\" + foldername):  # 文件夹不存在
        os.makedirs(cur_path_string + "\\" + foldername)
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    handler = logging.FileHandler("log/all" + dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d") + ".log")  # 日志文件
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - [line:%(lineno)d] - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.addHandler(console)
    print('log file created.')

    main_detector_quality('2018-11-15', logger)

