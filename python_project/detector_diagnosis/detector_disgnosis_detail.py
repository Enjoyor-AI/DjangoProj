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
import random
import string
from proj.config.database import Postgres
from proj.config.database import Oracle


def call_oracle(cur_date_str, scatsid, logger):
    detector_detail = pd.DataFrame({})
    available_score = pd.DataFrame({})
    detector_randomid = pd.DataFrame({})
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        # print('oracle connect succeed.')
        # print('reaching datas...')
        ora = Oracle()

        sql = "select distinct a.FINT_DETECTORID, a.FSTR_ERRORCODE, a.FSTR_ERRORNAME, a.FSTR_ERRORDETAIL, b.PHASENO from(" \
              "select fstr_intersectid, FINT_DETECTORID, FSTR_ERRORCODE, FSTR_ERRORNAME, FSTR_ERRORDETAIL " \
               "from hz_scats_detector_state where fstr_date = '{0}' and fstr_intersectid = '{1}' )a " \
               "left join INT_STR_INPUT b on a.fstr_intersectid = b.siteid and (" \
               "a.FINT_DETECTORID = b.LANE1 OR a.FINT_DETECTORID = b.LANE2 OR a.FINT_DETECTORID = b.LANE3 " \
               "OR a.FINT_DETECTORID = b.LANE4) order by fint_detectorid".format(cur_date_str, scatsid)
        # print(sql)
        sql2 = "select A.FSTR_INTERSECTID, B.SITENAME, A.SCORE FROM AVAILABLE_SCORE A " \
               "LEFT JOIN INTERSECT_INFORMATION B ON A.FSTR_INTERSECTID = B.SITEID WHERE A.FSTR_DATE = '{0}' " \
               "AND A.FSTR_INTERSECTID = '{1}'".format(cur_date_str, scatsid)
        sql3 = "select FINT_DETECTORID, FSTR_DETECTOR_RANDOM_ID from DETECTOR_RANDOM_ID " \
               "where fstr_intersectid = '{0}'".format(scatsid)
        try:
            detector_detail = ora.call_oracle_data(sql, fram=True)
            if not detector_detail.empty:
                available_score = ora.call_oracle_data(sql2)
                # print(detector_detail)
                detector_randomid = ora.call_oracle_data(sql3, fram=True)
                # print(detector_randomid)
                if detector_randomid.empty:
                    detector_R_id = detector_detail['FINT_DETECTORID'].tolist()
                    random_id = []
                    insert_pram = [':' + str(i + 1) for i in range(3)]
                    insert_pram = ','.join(insert_pram)
                    a = []
                    for det in detector_R_id:
                        random_id.append(''.join(random.sample(string.ascii_letters + string.digits, 8)))
                        a.append([scatsid, det, ''.join(random.sample(string.ascii_letters + string.digits, 6))])
                    # print(a)
                    # input()
                    sql4 = "insert into DETECTOR_RANDOM_ID values ({0})"
                    # print(sql4.format(insert_pram))
                    ora.send_oracle_data(sql4.format(insert_pram), a)
                    detector_randomid = pd.DataFrame(a, columns=['FSTR_INTERSECTID', 'FINT_DETECTORID', 'FSTR_DETECTOR_RANDOM_ID'])
                # cr = db.cursor()
                # cr.execute(sql1)
                # rs1 = cr.fetchall()
                # detector_all = pd.DataFrame(rs1)
                # detector_all.columns = ['FSTR_INTERSECTID', 'FINT_DETECTORID', 'FSTR_ERRORNAME']
                # cr.close()
                # print(detector_randomid)
        except Exception as e:
            logger.error('when reaching detector_all, ' + str(e))
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return detector_detail, available_score, detector_randomid


def call_postgresql(scatsid, logger):
    laneInfor = pd.DataFrame({})
    try:
        # conn = psycopg2.connect(database="signal_specialist", user="postgres", password="postgres",
        #                         host="192.168.20.46",
        #                         port="5432")
        pg = Postgres()
        try:
            sql = "select cast(m.coil_code as numeric), n.dir_name, " \
                  "case when m.function_name = 'icon-func-straight' then '直行' " \
                  "when m.function_name = 'icon-func-left-straight' then '左直' " \
                  "when m.function_name = 'icon-func-straight-right' then '直右' " \
                  "when m.function_name = 'icon-func-left' then '左转' " \
                  "when m.function_name = 'icon-func-right' then '右转' " \
                  "when m.function_name = 'icon-func-round' then '掉头' " \
                  "when m.function_name = 'icon-func-round-left' then '左转掉头' " \
                  "when m.function_name = 'icon-func-round-straight' then '直行掉头' " \
                  "when m.function_name = 'icon-func-left-right' then '左右转' " \
                  "when m.function_name = 'icon-func-three' then '直左右' else '出口' end function_name " \
                  "from(select a.sys_code, b.trancet_id, b.function_name, b.coil_code " \
                  "from pe_tobj_node_info a left join pe_tobj_node_lane b on a.node_id = b.node_id " \
                  "where a.sys_code = '{0}' ) m left join pe_tobj_node_round n on m.trancet_id = n.roadsect_id " \
                  "where m.coil_code is not null and m.coil_code != '-' ".format(scatsid)
            laneInfor = pg.call_pg_data(sql, fram=True)
            # cr = conn.cursor()
            # cr.execute(sql)
            # rs1 = cr.fetchall()
            # IntInfor = pd.DataFrame(rs1)
            # IntInfor.columns = ['FSTR_INTERSECTID', 'node_name']
            # cr.close()
        except Exception as e:
            logger.error('when reaching lanerinfo, ' + str(e))
        finally:
            pg.db_close()
    except Exception as e:
        logger.error('when calling postgresql, ' + str(e))
    return laneInfor


def main_detector_detail(cur_date_str, scatsid, logger):
    # cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
    # cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
    # print(cur_date_str)
    laneInfor = call_postgresql(scatsid, logger)
    # print(laneInfor)
    detector_detail, available_score, detector_randomid = call_oracle(cur_date_str, scatsid, logger)
    # print(laneInfor)
    # print(detector_detail)
    merge_detector_detail = pd.DataFrame({})
    if not detector_detail.empty:
        # detector_all = detector_all.fillna(0)
        laneInfor.rename(columns={'coil_code': 'FINT_DETECTORID'}, inplace=True)
        # print(laneInfor)
        merge_detector_detail = pd.merge(detector_detail, laneInfor, on='FINT_DETECTORID', how='left')
        # print(merge_detector_detail)
        # merge_detector_all = merge_detector_all.dropna(axis=0, how='any')
        # merge_detector_all = merge_detector_all.reset_index(drop=True)
        # print(merge_detector_all)
        merge_detector_detail['function'] = merge_detector_detail['dir_name'] + '口' + merge_detector_detail['function_name']
        # print(merge_detector_detail)
        del merge_detector_detail['dir_name']
        del merge_detector_detail['function_name']
        merge_detector_detail = merge_detector_detail.fillna('')
        merge_detector_detail = pd.merge(merge_detector_detail, detector_randomid, on='FINT_DETECTORID', how='left')
        # print(merge_detector_detail)
        ifmaintain = []
        # group_data = merge_detector_detail.groupby[('PHASENO', 'function')]
        # for group in group_data.groups:
        #     grouped_data = group_data.get_group(group)
        #     for m in range(len(grouped_data)):

        for m in range(len(merge_detector_detail)):
            if merge_detector_detail.iloc[m]['FSTR_ERRORCODE'] in ['ERR0001', 'ERR0002', 'ERR0003', 'ERR0004', 'ERR0005']:
                ifmaintain.append('是')
            else:
                ifmaintain.append('')
        merge_detector_detail = pd.concat([merge_detector_detail, pd.DataFrame({'ifMaintain': ifmaintain})], axis=1)
        del merge_detector_detail['FSTR_ERRORCODE']
        # print(merge_detector_detail)
        merge_detector_detail.rename(columns={'FINT_DETECTORID': 'detectorCode', 'FSTR_ERRORNAME': 'dataType',
                                              'FSTR_ERRORDETAIL': 'dataDetail', 'PHASENO': 'phase',
                                              'FSTR_DETECTOR_RANDOM_ID': 'detectorId'}, inplace=True)
        # print(merge_detector_detail)
        # print(available_score)

    return merge_detector_detail, available_score


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
    cur_date_str = '2018-11-15'

    main_detector_detail(cur_date_str, '10', logger)

