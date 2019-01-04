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
from proj.proj.config.database import Postgres
from proj.proj.config.database import Oracle
from Data_Recovery import Online


def call_oracle(cur_date_str, cur_time, before_time, scatsid, logger):
    detector_detail = pd.DataFrame({})
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        # print('oracle connect succeed.')
        # print('reaching datas...')
        ora = Oracle()

        sql = "select FSTR_DATE, FSTR_INTERSECTID, FINT_DETECTORID, FSTR_PHASE, FSTR_CYCLE_STARTTIME, FINT_DS, " \
              "FINT_ACTUALVOLUME FROM HZ_SCATS_OUTPUT WHERE TO_CHAR(FSTR_DATE, 'YYYY-MM-DD') = '{0}' " \
              "AND (FSTR_CYCLE_STARTTIME BETWEEN '{2}' AND '{1}')"\
            .format(cur_date_str, cur_time, before_time)
        # print(sql)
        try:
            detector_detail = ora.call_oracle_data(sql, fram=True)
        except Exception as e:
            logger.error('when reaching detector_detail, ' + str(e))
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return detector_detail


def call_postgresql(logger):
    laneInfor = pd.DataFrame({})
    IntInfor = pd.DataFrame({})
    try:
        # conn = psycopg2.connect(database="signal_specialist", user="postgres", password="postgres",
        #                         host="192.168.20.46",
        #                         port="5432")
        pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
                              'host': "192.168.20.46", 'port': "5432"})
        try:
            sql = "select x.coil_code, x.dir_name, x.function_name, y.sys_code from( " \
                  "select n.node_id, cast(m.coil_code as numeric), n.dir_name, " \
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
                  " ) m left join pe_tobj_node_round n on m.trancet_id = n.roadsect_id " \
                  "where m.coil_code is not null and m.coil_code != '-' ) x left join pe_tobj_node_info y on " \
                  "x.node_id = y.node_id "
            laneInfor = pg.call_pg_data(sql, fram=True)
            sql1 = "select sys_code from pe_tobj_node_info where length(sys_code) < 4"
            IntInfor = pg.call_pg_data(sql1, fram=True)
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
    return laneInfor, IntInfor


def main_detector_online(cur_date_str, cur_time, before_time, scatsid, logger):
    # cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
    # cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
    # print(cur_date_str)
    print(dt.datetime.now())
    detector_detail = call_oracle(cur_date_str, cur_time, before_time, scatsid, logger)
    # print(detector_detail)


    # detector_detail, available_score, detector_randomid = call_oracle(cur_date_str, scatsid, logger)
    # print(laneInfor)
    # print(detector_detail)
    if not detector_detail.empty:
        laneInfor, IntInfor = call_postgresql(logger)
        # print(laneInfor)
        df = detector_detail.groupby(["FSTR_INTERSECTID", "FINT_DETECTORID"])

        result = []
        for group in df.groups:
            # print(group)
            grouped_data = df.get_group(group)
            # print(grouped_data)
            ol_restore = Online()
            error_zero_list, error_list, normal_list = ol_restore.Online_Select(grouped_data)
            # print(error_list,normal_list)
            outputsate = ol_restore.Data_Quality_Labeling(grouped_data, error_zero_list, error_list)
            # print(outputsate)

            temp_lane = laneInfor[(laneInfor.sys_code == group[0]) & (laneInfor.coil_code == str(group[1]))]
            if not temp_lane.empty:
                function = temp_lane.iloc[0]['dir_name'] + '口' + temp_lane.iloc[0]['function_name']
            else:
                function = ''
            # print(grouped_data)

            result.append([group[0], group[1], outputsate.error_name, outputsate.detail_error,
                           grouped_data.iloc[0]['FSTR_PHASE'], function])
            # try:
            #     detector_dict[group[0]][group[1]] = {'scatsDataType': outputsate.error_name,
            #                                          'scatsDataDetail': outputsate.detail_error}
            # except KeyError:
            #     detector_dict[group[0]] = {}
            #     detector_dict[group[0]][group[1]] = {'scatsDataType': outputsate.error_name,
            #                                          'scatsDataDetail': outputsate.detail_error}

        # print(detector_dict)
        print(dt.datetime.now())
        result_df = pd.DataFrame(result, columns=['scatsId', 'detectorCode', 'dataType', 'dataDetail', 'phase', 'function'])
        result_df.to_csv('online_detector.csv', index=False, header=True, encoding="utf-8-sig")
        # detector_all = detector_all.fillna(0)
        # laneInfor.rename(columns={'coil_code': 'FINT_DETECTORID'}, inplace=True)
        # # print(laneInfor)
        # merge_detector_detail = pd.merge(detector_detail, laneInfor, on='FINT_DETECTORID', how='left')
        # # print(merge_detector_detail)
        # # merge_detector_all = merge_detector_all.dropna(axis=0, how='any')
        # # merge_detector_all = merge_detector_all.reset_index(drop=True)
        # # print(merge_detector_all)
        # merge_detector_detail['function'] = merge_detector_detail['dir_name'] + '口' + merge_detector_detail['function_name']
        # # print(merge_detector_detail)
        # del merge_detector_detail['dir_name']
        # del merge_detector_detail['function_name']
        # merge_detector_detail = merge_detector_detail.fillna('')
        # merge_detector_detail = pd.merge(merge_detector_detail, detector_randomid, on='FINT_DETECTORID', how='left')
        # # print(merge_detector_detail)
        # ifmaintain = []
        # # group_data = merge_detector_detail.groupby[('PHASENO', 'function')]
        # # for group in group_data.groups:
        # #     grouped_data = group_data.get_group(group)
        # #     for m in range(len(grouped_data)):
        #
        # for m in range(len(merge_detector_detail)):
        #     if merge_detector_detail.iloc[m]['FSTR_ERRORCODE'] in ['ERR0001', 'ERR0002', 'ERR0003', 'ERR0004', 'ERR0005']:
        #         ifmaintain.append('是')
        #     else:
        #         ifmaintain.append('')
        # merge_detector_detail = pd.concat([merge_detector_detail, pd.DataFrame({'ifMaintain': ifmaintain})], axis=1)
        # del merge_detector_detail['FSTR_ERRORCODE']
        # # print(merge_detector_detail)
        # merge_detector_detail.rename(columns={'FINT_DETECTORID': 'detectorCode', 'FSTR_ERRORNAME': 'dataType',
        #                                       'FSTR_ERRORDETAIL': 'dataDetail', 'PHASENO': 'phase',
        #                                       'FSTR_DETECTOR_RANDOM_ID': 'detectorId'}, inplace=True)
        # # print(merge_detector_detail)
        # # print(available_score)

    return


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
    cur_date_str = '2018-11-12'
    cur_time = '14:00:00'
    before_time = '13:30:00'

    main_detector_online(cur_date_str, cur_time, before_time, '1', logger)

