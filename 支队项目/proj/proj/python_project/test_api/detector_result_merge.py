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
    scats_detector = pd.DataFrame({})
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        # print('oracle connect succeed.')
        # print('reaching datas...')
        ora = Oracle()

        sql = "select a.FINT_DETECTORID, a.FSTR_ERRORNAME, a.FSTR_ERRORDETAIL, b.PHASENO FROM " \
              "HZ_SCATS_DETECTOR_STATE a left join INT_STR_INPUT B ON a.fstr_intersectid = b.siteid and (" \
               "a.FINT_DETECTORID = b.LANE1 OR a.FINT_DETECTORID = b.LANE2 OR a.FINT_DETECTORID = b.LANE3 " \
               "OR a.FINT_DETECTORID = b.LANE4) WHERE A.FSTR_DATE = '{0}' AND A.FSTR_INTERSECTID = '{1}'"\
            .format(cur_date_str, scatsid)
        # print(sql)
        try:
            scats_detector = ora.call_oracle_data(sql, fram=True)
        except Exception as e:
            logger.error('when reaching scats_detector, ' + str(e))
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return scats_detector


def call_postgresql(cur_date_str, scatsid, logger):
    laneInfor = pd.DataFrame({})
    geom_detector = pd.DataFrame({})
    try:
        # conn = psycopg2.connect(database="signal_specialist", user="postgres", password="postgres",
        #                         host="192.168.20.46",
        #                         port="5432")
        try:
            pg = Postgres(pg_inf={'database': "inter_info", 'user': "postgres", 'password': "postgres",
                                  'host': "192.168.20.46", 'port': "5432"})
            sql = "select x.coil_code, x.dir_name, x.function_name from( " \
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
                  "x.node_id = y.node_id where y.sys_code = '{0}'".format(scatsid)
            laneInfor = pg.call_pg_data(sql, fram=True)
            pg.db_close()
            pg = Postgres(pg_inf={'database': "detector", 'user': "postgres", 'password': "postgres",
                                  'host': "192.168.20.56", 'port': "5432"})
            sql1 = "select scats_lane, error_name, error_detail from detector_test_results " \
                   "where datetime = '{0}' and devc_id = '{1}'".format('2018-08-08', scatsid)
            geom_detector = pg.call_pg_data(sql1, fram=True)
            pg.db_close()
            # cr = conn.cursor()
            # cr.execute(sql)
            # rs1 = cr.fetchall()
            # IntInfor = pd.DataFrame(rs1)
            # IntInfor.columns = ['FSTR_INTERSECTID', 'node_name']
            # cr.close()
        except Exception as e:
            logger.error('when reaching lanerinfo, ' + str(e))
    except Exception as e:
        logger.error('when calling postgresql, ' + str(e))
    return laneInfor, geom_detector


def main_detector_result_merge(cur_date_str, scatsid, logger):
    # cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
    # cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
    # print(cur_date_str)
    # print(dt.datetime.now())
    scats_detector = call_oracle(cur_date_str, scatsid, logger)
    laneInfor, geom_detector = call_postgresql(cur_date_str, scatsid, logger)
    # print(laneInfor)
    # print(geom_detector)
    # input()
    merge_detector_detail = pd.DataFrame({})
    if not scats_detector.empty:
        if not geom_detector.empty:
            geom_detector.rename(columns={'scats_lane': 'FINT_DETECTORID'}, inplace=True)
            merge_df = pd.merge(scats_detector, geom_detector, on='FINT_DETECTORID', how='right')

            # print(merge_df)
            merge_label = []
            rec_resourse = []
            temp_list = ['数据正常', '数据缺陷', '数据异常', '数据缺失']
            for m in range(len(merge_df)):
                if str(merge_df.iloc[m]['FSTR_ERRORNAME']) == 'nan':
                    merge_label.append(merge_df.iloc[m]['error_name'])
                    rec_resourse.append('地磁')
                else:
                    if merge_df.iloc[m]['FSTR_ERRORNAME'] == merge_df.iloc[m]['error_name']:
                        merge_label.append(merge_df.iloc[m]['error_name'])
                        rec_resourse.append('均可')
                    elif temp_list.index(merge_df.iloc[m]['FSTR_ERRORNAME']) > temp_list.index(merge_df.iloc[m]['error_name']):
                        merge_label.append(merge_df.iloc[m]['FSTR_ERRORNAME'])
                        rec_resourse.append('地磁')
                    else:
                        merge_label.append(merge_df.iloc[m]['error_name'])
                        rec_resourse.append('scats')
            result = pd.DataFrame({'merge': merge_label, 'source': rec_resourse})
            merge_detector_detail = pd.concat([merge_df, result], axis=1)
            laneInfor.rename(columns={'coil_code': 'FINT_DETECTORID'}, inplace=True)
            merge_detector_detail = pd.merge(merge_detector_detail, laneInfor, on='FINT_DETECTORID', how='left')
            # print(merge_detector_detail)
            # merge_detector_all = merge_detector_all.dropna(axis=0, how='any')
            # merge_detector_all = merge_detector_all.reset_index(drop=True)
            # print(merge_detector_all)
            merge_detector_detail['function'] = merge_detector_detail['dir_name'] + '口' + merge_detector_detail[
                'function_name']
            # print(merge_detector_detail)
            del merge_detector_detail['dir_name']
            del merge_detector_detail['function_name']
            merge_detector_detail.rename(columns={'FINT_DETECTORID': 'detectorId', 'FSTR_ERRORNAME': 'scatsDataType',
                                                  'FSTR_ERRORDETAIL': 'scatsDataDetail', 'error_name': 'geoDataType',
                                                  'error_detail': 'geoDataDetail', 'PHASENO': 'phase'}, inplace=True)
            merge_detector_detail = merge_detector_detail.fillna('')
            merge_detector_detail = merge_detector_detail.sort_values(by="detectorId")
            # print(merge_detector_detail)


        # df = detector_detail.groupby(["FSTR_INTERSECTID", "FINT_DETECTORID"])
        #
        # result = []
        # for group in df.groups:
        #     # print(group)
        #     grouped_data = df.get_group(group)
        #     # print(grouped_data)
        #     ol_restore = Online()
        #     error_zero_list, error_list, normal_list = ol_restore.Online_Select(grouped_data)
        #     # print(error_list,normal_list)
        #     outputsate = ol_restore.Data_Quality_Labeling(grouped_data, error_zero_list, error_list)
        #     # print(outputsate)
        #
        #     temp_lane = laneInfor[(laneInfor.sys_code == group[0]) & (laneInfor.coil_code == str(group[1]))]
        #     if not temp_lane.empty:
        #         function = temp_lane.iloc[0]['dir_name'] + '口' + temp_lane.iloc[0]['function_name']
        #     else:
        #         function = ''
        #     # print(grouped_data)
        #
        #     result.append([group[0], group[1], outputsate.error_name, outputsate.detail_error,
        #                    grouped_data.iloc[0]['FSTR_PHASE'], function])
            # try:
            #     detector_dict[group[0]][group[1]] = {'scatsDataType': outputsate.error_name,
            #                                          'scatsDataDetail': outputsate.detail_error}
            # except KeyError:
            #     detector_dict[group[0]] = {}
            #     detector_dict[group[0]][group[1]] = {'scatsDataType': outputsate.error_name,
            #                                          'scatsDataDetail': outputsate.detail_error}

        # print(detector_dict)
        # print(dt.datetime.now())
        # result_df = pd.DataFrame(result, columns=['scatsId', 'detectorCode', 'dataType', 'dataDetail', 'phase', 'function'])
        # result_df.to_csv('online_detector.csv', index=False, header=True, encoding="utf-8-sig")
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

    return merge_detector_detail


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

    main_detector_result_merge(cur_date_str, '1', logger)

