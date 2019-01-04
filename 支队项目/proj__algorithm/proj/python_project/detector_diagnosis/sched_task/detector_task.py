#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#############################
# Created on Tue Jan 2 2019
# Description: detector available task management
# @author:  DDD
#############################
import cx_Oracle
import logging
import datetime as dt
import os
import pandas as pd
from proj.config.database import Oracle
from proj.config.database import Postgres
import pickle
import json
logger = logging.getLogger('schedulerTask')

def call_oracle1(ora, cur_date_str, logger):
    detector_all = pd.DataFrame({})
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        # ora = Oracle()
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
        # finally:
        #     ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return detector_all


def call_postgresql1(pg, logger):
    IntInfor = pd.DataFrame({})
    try:
        # conn = psycopg2.connect(database="signal_specialist", user="postgres", password="postgres",
        #                         host="192.168.20.46",
        #                         port="5432")
        # pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
        #                       'host': "192.168.20.46", 'port': "5432"})
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
        # finally:
        #     pg.db_close()
    except Exception as e:
        logger.error('when calling postgresql, ' + str(e))
    return IntInfor


def availa_task(ora, str_date, logger):
    try:
        # ora = Oracle()
        sql = "select FSTR_INTERSECTID, case when score > 10 then '100' when score < 0 then '0' " \
              "else to_char(score*10) end Score FROM AVAILABLE_SCORE WHERE FSTR_DATE = '{0}'".format(str_date)
        result_df = ora.call_oracle_data(sql, fram=True)
        # result_df.rename(columns={'FSTR_INTERSECTID': 'scatsID', 'SCORE': 'score'}, inplace=True)
        # print(result_df)
        result_df.to_pickle('detector_available.pkl')
        # a = pd.read_pickle('detector_available_all.pkl')
        # print(a)
    except Exception as e:
        logger.error(e)


def call_oracle2(ora, cur_date_str, logger):
    abnormal_int = pd.DataFrame({})
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        # print('oracle connect succeed.')
        # print('reaching datas...')
        # ora = Oracle()

        sql = "select a.siteid, a.sitename from (select siteid, sitename from INTERSECT_INFORMATION) a left join (" \
              "select distinct FSTR_INTERSECTID from hz_scats_output" \
              " WHERE to_char(FSTR_DATE, 'yyyy-mm-dd') = '{0}') b on a.siteid = b.FSTR_INTERSECTID " \
              "where b.FSTR_INTERSECTID is null ".format(cur_date_str)
        try:
            abnormal_int = ora.call_oracle_data(sql, fram=True)
            # cr = db.cursor()
            # cr.execute(sql1)
            # rs1 = cr.fetchall()
            # detector_all = pd.DataFrame(rs1)
            # detector_all.columns = ['FSTR_INTERSECTID', 'FINT_DETECTORID', 'FSTR_ERRORNAME']
            # cr.close()
        except Exception as e:
            logger.error('when reaching abnormal intersect, ' + str(e))
        # finally:
        #     ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return abnormal_int


def call_oracle3(ora, cur_date_str, logger):
    detector_plot_data = pd.DataFrame({})
    detector_id = pd.DataFrame({})
    detector_state = pd.DataFrame({})
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        # ora = Oracle()
        # db = ora.db_conn()
        # print('oracle connect succeed.')
        # print('reaching datas...')

        # sql = "select FSTR_CYCLE_STARTTIME, FINT_DS, FINT_ACTUALVOLUME FROM HZ_SCATS_OUTPUT " \
        #       "WHERE TO_CHAR(FSTR_DATE, 'YYYY-MM-DD') = '{0}' AND FSTR_INTERSECTID = '{1}' AND FINT_DETECTORID = {2}" \
        #       "select fint_detectorid in DETECTOR_RANDOM_ID where FSTR_DETECTOR_RANDOM_ID = '{2}'" \
        #     .format(cur_date_str, scatsid, detectorid)
        # sql = "select a.FSTR_CYCLE_STARTTIME, a.FINT_DS, a.FINT_ACTUALVOLUME FROM HZ_SCATS_OUTPUT a right join (" \
        #       "select fint_detectorid from DETECTOR_RANDOM_ID where FSTR_DETECTOR_RANDOM_ID = '{2}') b " \
        #       "on a.FINT_DETECTORID = b.FINT_DETECTORID WHERE TO_CHAR(FSTR_DATE, 'YYYY-MM-DD') = " \
        #       "'{0}' AND FSTR_INTERSECTID = '{1}'".format(cur_date_str, scatsid, detectorid)
        sql = "select FSTR_INTERSECTID, FINT_DETECTORID, FSTR_CYCLE_STARTTIME, FINT_DS, FINT_ACTUALVOLUME " \
              "FROM HZ_SCATS_OUTPUT where TO_CHAR(FSTR_DATE, 'YYYY-MM-DD') = '{0}'".format(cur_date_str)
        sql1 = "select * from DETECTOR_RANDOM_ID"
        # sql2 = "select a.fint_detectorid, a.fstr_errorname from hz_scats_detector_state a right join (" \
        #        "select fint_detectorid from DETECTOR_RANDOM_ID where FSTR_DETECTOR_RANDOM_ID = '{2}') b " \
        #        "on a.fint_detectorid = b.fint_detectorid WHERE a.FSTR_DATE = '{0}' " \
        #        "AND a.FSTR_INTERSECTID = '{1}' ".format(cur_date_str, scatsid, detectorid)
        sql2 = "select fstr_intersectid, fint_detectorid, fstr_errorname from hz_scats_detector_state " \
               "WHERE FSTR_DATE = '{0}'".format(cur_date_str)
        # print(sql)  # '331', '325', '326', '327', '328', '329', '330', '331', '445', '187', '276'
        try:
            # cr = db.cursor()
            detector_plot_data = ora.call_oracle_data(sql, fram=True)
            detector_id = ora.call_oracle_data(sql1, fram=True)
            # print(detector_plot_data)
            detector_state = ora.call_oracle_data(sql2, fram=True)
            # ora.execute(sql1)
            # rs1 = ora.fetchall()
            # detector_all = pd.DataFrame(rs1)
            # detector_all.columns = ['FSTR_INTERSECTID', 'SCORE', 'LANENUM']
            # cr.close()
            print(detector_plot_data)
            print(detector_id)
            print(detector_state)
            # input()
        except Exception as e:
            logger.error('when reaching detector plot data, ' + str(e))
        # finally:
        #     ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return detector_plot_data, detector_state, detector_id


def detector_list_task(pg, ora, str_date, logger):
    interInfor = call_postgresql1(pg, logger)
    # print(interInfor)
    detector_all = call_oracle1(ora, str_date, logger)
    interInfor.rename(columns={'fstr_intersectid': 'FSTR_INTERSECTID'}, inplace=True)
    merge_detector_all = pd.DataFrame({})
    if not detector_all.empty and not interInfor.empty:
        # detector_all = detector_all.fillna(0)
        merge_detector_all = pd.merge(detector_all, interInfor, on='FSTR_INTERSECTID', how='left')
        # print(merge_detector_all)
        merge_detector_all = merge_detector_all.dropna(axis=0, how='any')
        merge_detector_all = merge_detector_all.reset_index(drop=True)
        # print(merge_detector_all)
        merge_detector_all.rename(columns={'FSTR_INTERSECTID': 'scatsId', 'SCORE': 'score',
                                           'LANENUM': 'laneNum', 'node_name': 'intName', 'node_id': 'nodeId'},
                                  inplace=True)
        # print(merge_detector_all)
    # merge_detector_all.to_csv('merge_detector_all.csv', index=False, header=True, encoding="utf-8-sig")
    merge_detector_all.to_pickle('detector_list.pkl')
    a = pd.read_pickle('detector_list.pkl')
    # print(a)


def abnormal_int_task(ora, cur_date_str, logger):
    abnormal_int = call_oracle2(ora, cur_date_str, logger)
    if not abnormal_int.empty:
        abnormal_int.rename(columns={'SITEID': 'scatsId', 'SITENAME': 'intName'}, inplace=True)
        # result_df = abnormal_int.to_json(orient='records', force_ascii=False)
        abnormal_int.to_pickle('abnormal_int.pkl')
        # print(abnormal_int)
        # result_df = json.loads(result_df)


def detector_plot_task(ora, cur_date_str, logger):
    # cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
    # cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
    # print(cur_date_str)
    # cur_date_str = '2018-11-15'
    # interInfor = call_postgresql(logger)
    detector_plot_data, detector_state, detector_id = call_oracle3(ora, cur_date_str, logger)
    # print(detector_plot_data)
    # print(detector_state)
    # merge_detector_all = pd.DataFrame({})
    if not detector_plot_data.empty:
        int_list = list(set(detector_id['FSTR_INTERSECTID'].tolist()))
        result_dict = {}
        for ints in int_list:
            result_dict[ints] = {}
        for m in range(len(detector_id)):
            int_num = detector_id.iloc[m]['FSTR_INTERSECTID']
            # if int_num == '593':
            det_num = detector_id.iloc[m]['FINT_DETECTORID']
            det_random = detector_id.iloc[m]['FSTR_DETECTOR_RANDOM_ID']
            print(int_num, det_num, det_random)
            # group_data = detector_plot_data.groupby(['FSTR_INTERSECTID', 'FINT_DETECTORID'])
            # for groups in group_data.groups:
            #     grouped_data = group_data.get_group(groups)
            #     print(groups)
            # int_num = groups[0]
            # det_num = groups[1]
            # det_random = detector_id[(detector_id.FSTR_INTERSECTID == int_num) &]
            grouped_data = detector_plot_data[(detector_plot_data.FSTR_INTERSECTID == int_num) &
                                              (detector_plot_data.FINT_DETECTORID == det_num)]
            if not grouped_data.empty:
                del grouped_data['FSTR_INTERSECTID']
                del grouped_data['FINT_DETECTORID']

                state_df = detector_state[(detector_state.FSTR_INTERSECTID == int_num) &
                                          (detector_state.FINT_DETECTORID == det_num)]
                if not state_df.empty:
                    det_state = state_df.iloc[0]['FSTR_ERRORNAME']
                else:
                    det_state = ''
                print('det_state', det_state)
                # detector_all = detector_all.fillna(0)
                # print(detector_plot_data)
                grouped_data.sort_values(by="FSTR_CYCLE_STARTTIME", inplace=True)
                grouped_data['datetime'] = grouped_data['FSTR_CYCLE_STARTTIME'].map(
                    lambda x: dt.datetime.strptime(cur_date_str + ' ' + x, "%Y-%m-%d %H:%M:%S"))
                grouped_data.set_index(["datetime"], inplace=True)

                flow_cg = grouped_data['FINT_ACTUALVOLUME'].resample('10min', label='left', closed='left').sum()
                flow_cg = flow_cg.fillna(0)
                ds_cg = grouped_data['FINT_DS'].resample('10min', label='left', closed='left').max()
                ds_cg = ds_cg.fillna(0)
                flow_cg = flow_cg.map(lambda x: str(x))
                ds_cg = ds_cg.map(lambda x: str(x))
                time_str = []
                for time_dt in flow_cg.index:
                    time_str.append(str(time_dt)[11:])
                # print(type(flow_cg.iloc[0]))
                # print(list(flow_cg))
                # print(list(ds_cg))
                # print(detector_plot_data.index.tolist())
                result_data = {'flow': list(flow_cg), 'ds': list(ds_cg), 'time': time_str,
                               'detectorid': det_num, 'state': det_state}
                print(result_data)
                result_dict[int_num][det_random] = result_data
        print(result_dict)
        print('writing...')
        with open('detector_plot.pkl', 'wb') as f:
            pickle.dump(json.dumps(result_dict, ensure_ascii=False), f, pickle.HIGHEST_PROTOCOL)
        f.close()
    return


def task_main(str_date):
    ora = Oracle()
    # pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
    #                       'host': "192.168.20.46", 'port': "5432"})
    pg = Postgres(pg_inf={'database': "inter_info", 'user': "postgres", 'password': "postgres",
                          'host': "33.83.100.145", 'port': "5432"})
    availa_task(ora, str_date, logger)
    print('检测器可用率已写入文件')
    detector_list_task(pg, ora, str_date, logger)
    pg.db_close()
    print('检测器列表已写入文件')
    detector_plot_task(ora, str_date, logger)
    print('画图数据已写入文件')
    abnormal_int_task(ora, str_date, logger)
    print('异常路口已写入文件')
    ora.db_close()


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

    task_main('2018-11-12', logger)
