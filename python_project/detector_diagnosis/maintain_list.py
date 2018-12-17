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
import operator
from proj.config.database import Postgres
from proj.config.database import Oracle


def call_postgresql(int_list, logger):
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
            sql = "select sys_code from pe_tobj_node_info  where node_id in {} ".format(tuple(int_list))
            # cr = conn.cursor()
            IntInfor = pg.call_pg_data(sql, fram=True)
            # print(IntInfor)
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
    return IntInfor['sys_code'].tolist()


def call_oracle(listname, interInfor, ran_str, cur_date_str, logger):
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        ora = Oracle()
        # db = ora.db_conn()
        # print('oracle connect succeed.')
        # print('reaching datas...')

        sql = "insert into MAINTAIN_LIST values ({0})"
        sql1 = "select distinct fstr_name, fstr_intersectid from MAINTAIN_LIST"
        # print(sql)  # '331', '325', '326', '327', '328', '329', '330', '331', '445', '187', '276'
        try:
            # cr = db.cursor()
            exist_maintain = ora.call_oracle_data(sql1, fram=True)
            # print(exist_maintain)
            temp_df = exist_maintain[exist_maintain.FSTR_NAME == listname]
            update = True
            if not temp_df.empty:
                a = list(set(interInfor))
                b = list(set(temp_df['FSTR_INTERSECTID'].tolist()))
                if len(a) == len(b) == len(set(a).intersection(set(b))):
                    print('计划已存在')
                    update = False
            if update:
                insert_pram = [':'+str(i+1) for i in range(4)]
                insert_pram = ','.join(insert_pram)
                a = [[ran_str, cur_date_str, listname, i] for i in interInfor]
                # print(a)
                # print(sql.format(insert_pram))
                ora.send_oracle_data(sql.format(insert_pram), a)
            # ora.execute(sql1)
            # rs1 = ora.fetchall()
            # detector_all = pd.DataFrame(rs1)
            # detector_all.columns = ['FSTR_INTERSECTID', 'SCORE', 'LANENUM']
            # cr.close()
        except Exception as e:
            logger.error('when insert new maintain list, ' + str(e))
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return


def call_oracle2(logger):
    exist_maintain = pd.DataFrame({})
    try:
        ora = Oracle()
        sql = "select distinct fstr_name, fstr_id from MAINTAIN_LIST "
        try:
            # cr = db.cursor()
            exist_maintain = ora.call_oracle_data(sql, fram=True)
            # print(exist_maintain)
        except Exception as e:
            logger.error('when reaching maintain list, ' + str(e))
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return exist_maintain


def call_oracle3(listid, logger):
    list_detail = pd.DataFrame({})
    try:
        ora = Oracle()
        sql = "select x.*, y.sitename from (select m.fstr_intersectid, m.fint_detectorid, m.fstr_errorname, " \
              "m.fstr_errordetail, m.FSTR_ERRORCODE, round(n.score*10) score from (" \
              "select a.fstr_intersectid, a.fstr_date, b.fint_detectorid, b.fstr_errorname, " \
              "b.FSTR_ERRORCODE, b.fstr_errordetail from (select fstr_intersectid, fstr_date from MAINTAIN_LIST " \
              "where fstr_id = '{0}' ) a left join hz_scats_detector_state b " \
              "on a.fstr_intersectid = b.fstr_intersectid and a.fstr_date = b.fstr_date ) m " \
              "left join available_score n on m.fstr_intersectid = n.fstr_intersectid and m.fstr_date = n.FSTR_DATE " \
              ") x left join INTERSECT_INFORMATION y on x.fstr_intersectid = y.siteid".format(listid)
        try:
            # cr = db.cursor()
            list_detail = ora.call_oracle_data(sql, fram=True)
            # print(exist_maintain)
        except Exception as e:
            logger.error('when reaching one maintain list, ' + str(e))
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return list_detail


def call_oracle4(cur_date_str, listid, logger):
    ori_list = pd.DataFrame({})
    cur_service = pd.DataFrame({})
    ori_date = []
    int_name = pd.DataFrame({})
    try:
        ora = Oracle()
        sql = "select a.fstr_intersectid, b.fint_detectorid, b.FSTR_ERRORCODE ori_code from (" \
              "select fstr_intersectid, fstr_date from MAINTAIN_LIST where fstr_id = '{0}' ) a " \
              "left join hz_scats_detector_state b on a.fstr_intersectid = b.fstr_intersectid " \
              "and a.fstr_date = b.fstr_date where b.FSTR_ERRORCODE " \
              "in ('ERR0001', 'ERR0002', 'ERR0003', 'ERR0004', 'ERR0005')".format(listid)
        sql2 = "select a.fstr_intersectid, b.fint_detectorid, b.FSTR_ERRORCODE cur_code " \
               "from (select fstr_intersectid from MAINTAIN_LIST where fstr_id = '{0}') a left join " \
               "hz_scats_detector_state b on a.fstr_intersectid = b.fstr_intersectid " \
               "where b.fstr_date = '{1}'".format(listid, cur_date_str)
        sql3 = "select distinct fstr_date from MAINTAIN_LIST where fstr_id = '{}'".format(listid)
        sql4 = "select a.fstr_intersectid, b.sitename from MAINTAIN_LIST a left join INTERSECT_INFORMATION b " \
               "on a.fstr_intersectid = b.SITEID where fstr_id = '{}'".format(listid)
        try:
            # cr = db.cursor()
            ori_list = ora.call_oracle_data(sql, fram=True)
            cur_service = ora.call_oracle_data(sql2, fram=True)
            ori_date = ora.call_oracle_data(sql3)
            int_name = ora.call_oracle_data(sql4, fram=True)
            # print(exist_maintain)
        except Exception as e:
            logger.error('when reaching one maintain list, ' + str(e))
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return ori_list, cur_service, ori_date, int_name


def main_add_maintain_list(add_maintain_data, cur_date_str, logger):
    # print(11111111111111111111111111111111)
    interInfor = call_postgresql(add_maintain_data['listNode'], logger)
    ran_str = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    # print(ran_str)

    call_oracle(add_maintain_data['listName'], interInfor, ran_str, cur_date_str, logger)
    return


def main_get_maintain_list(logger):
    exist_maintain = call_oracle2(logger)
    exist_maintain.rename(columns={'FSTR_NAME': 'listName', 'FSTR_ID': 'listId'}, inplace=True)
    # print(exist_maintain)
    return exist_maintain


def main_get_one_list(listid, logger):
    list_detail = call_oracle3(listid, logger)
    # print(list_detail)
    if not list_detail.empty:
        ifmaintain = []
        # group_data = merge_detector_detail.groupby[('PHASENO', 'function')]
        # for group in group_data.groups:
        #     grouped_data = group_data.get_group(group)
        #     for m in range(len(grouped_data)):

        for m in range(len(list_detail)):
            if list_detail.iloc[m]['FSTR_ERRORCODE'] in ['ERR0001', 'ERR0002', 'ERR0003', 'ERR0004', 'ERR0005']:
                ifmaintain.append('是')
            else:
                ifmaintain.append('')
        list_detail = pd.concat([list_detail, pd.DataFrame({'ifMaintain': ifmaintain})], axis=1)
        del list_detail['FSTR_ERRORCODE']
        list_detail.rename(columns={'FSTR_INTERSECTID': 'scatsId', 'FINT_DETECTORID': 'detectorId',
                                    'FSTR_ERRORNAME': 'dataType', 'FSTR_ERRORDETAIL': 'dataDetail',
                                    'SITENAME': 'intName', 'SCORE': 'availableScore'}, inplace=True)
        # print(list_detail)
    return list_detail


def main_get_service_state(cur_date_str, listid, logger):
    ori_list, cur_service, ori_date, int_name_df = call_oracle4(cur_date_str, listid, logger)
    result = {}
    if not ori_list.empty:
        if not cur_service.empty:
            int_num = len(int_name_df)
            # print(ori_list)
            # print(cur_service)
            merge_result = pd.merge(ori_list, cur_service, on=['FSTR_INTERSECTID', 'FINT_DETECTORID'], how='left')
            # print(merge_result)
            need_service_num = len(merge_result)
            group_data = merge_result.groupby(['FSTR_INTERSECTID'])
            recovery_num = 0
            list_result = []
            for ints in group_data.groups:
                intname = int_name_df[int_name_df.FSTR_INTERSECTID == ints].iloc[0]['SITENAME']
                temp_recovery = []
                temp_unrecovery = []
                grouped_data = group_data.get_group(ints)
                # print(grouped_data)

                for m in range(len(grouped_data)):
                    if grouped_data.iloc[m]['ORI_CODE'] != grouped_data.iloc[m]['CUR_CODE']:
                        recovery_num += 1
                        temp_recovery.append(str(grouped_data.iloc[m]['FINT_DETECTORID']))
                    else:
                        temp_unrecovery.append(str(grouped_data.iloc[m]['FINT_DETECTORID']))
                # print('1111', ','.join([]))
                # if len(temp_recovery) != 1:
                temp_recovery_str = ','.join(temp_recovery)
                # else:
                #     temp_recovery_str = str(temp_recovery[0])
                # if len(temp_unrecovery) != 1:
                #     print(temp_unrecovery)
                temp_unrecovery_str = ','.join(temp_unrecovery)
                # else:
                #     temp_unrecovery_str = str(temp_unrecovery[0])
                list_result.append({'scatsId': ints, 'intName': intname, 'recoveryDetector': temp_recovery_str,
                                    'unrecoveryDetector': temp_unrecovery_str,
                                    'recoveryRate': round(100 * len(temp_recovery) / len(grouped_data))})
            # print(list_result)
            result = {'reportDate': ori_date[0][0], 'intNumbers': int_num, 'detectorNumbers': need_service_num,
                      'recoveryDetectorAll': recovery_num,
                      'recoveryRateAll': round(100 * recovery_num / need_service_num), 'data': list_result}
    return result


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

    data = {"listName": "测试清单名称", "listNode":[
        "00f8f971963472e943a508b7c50817c0",
        "93cd4bc6e84934100a6e70d8b86515c8",
        "01e9d9b4802a72cd7d6bd3a0cc7ffc07"]}
    # main_add_maintain_list(data, '2018-11-12', logger)
    # main_get_maintain_list(logger)
    # main_get_one_list('yNnYvp3S', logger)
    main_get_service_state('2018-11-15', 'yNnYvp3S', logger)
