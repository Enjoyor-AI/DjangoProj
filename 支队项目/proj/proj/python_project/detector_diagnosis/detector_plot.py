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


def call_oracle(cur_date_str, scatsid, detectorid, logger):
    detector_plot_data = pd.DataFrame({})
    detector_state = []
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        ora = Oracle()
        # db = ora.db_conn()
        # print('oracle connect succeed.')
        # print('reaching datas...')

        # sql = "select FSTR_CYCLE_STARTTIME, FINT_DS, FINT_ACTUALVOLUME FROM HZ_SCATS_OUTPUT " \
        #       "WHERE TO_CHAR(FSTR_DATE, 'YYYY-MM-DD') = '{0}' AND FSTR_INTERSECTID = '{1}' AND FINT_DETECTORID = {2}" \
        #       "select fint_detectorid in DETECTOR_RANDOM_ID where FSTR_DETECTOR_RANDOM_ID = '{2}'" \
        #     .format(cur_date_str, scatsid, detectorid)
        sql = "select a.FSTR_CYCLE_STARTTIME, a.FINT_DS, a.FINT_ACTUALVOLUME FROM HZ_SCATS_OUTPUT a right join (" \
              "select fint_detectorid from DETECTOR_RANDOM_ID where FSTR_DETECTOR_RANDOM_ID = '{2}') b " \
              "on a.FINT_DETECTORID = b.FINT_DETECTORID WHERE TO_CHAR(FSTR_DATE, 'YYYY-MM-DD') = " \
              "'{0}' AND FSTR_INTERSECTID = '{1}'".format(cur_date_str, scatsid, detectorid)
        sql2 = "select a.fint_detectorid, a.fstr_errorname from hz_scats_detector_state a right join (" \
               "select fint_detectorid from DETECTOR_RANDOM_ID where FSTR_DETECTOR_RANDOM_ID = '{2}') b " \
               "on a.fint_detectorid = b.fint_detectorid WHERE a.FSTR_DATE = '{0}' " \
               "AND a.FSTR_INTERSECTID = '{1}' ".format(cur_date_str, scatsid, detectorid)
        # print(sql)  # '331', '325', '326', '327', '328', '329', '330', '331', '445', '187', '276'
        try:
            # cr = db.cursor()
            detector_plot_data = ora.call_oracle_data(sql, fram=True)
            # print(detector_plot_data)
            detector_state = ora.call_oracle_data(sql2)
            # ora.execute(sql1)
            # rs1 = ora.fetchall()
            # detector_all = pd.DataFrame(rs1)
            # detector_all.columns = ['FSTR_INTERSECTID', 'SCORE', 'LANENUM']
            # cr.close()
        except Exception as e:
            logger.error('when reaching detector plot data, ' + str(e))
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return detector_plot_data, detector_state


def call_postgresql(logger):
    IntInfor = pd.DataFrame({})
    try:
        # conn = psycopg2.connect(database="signal_specialist", user="postgres", password="postgres",
        #                         host="192.168.20.46",
        #                         port="5432")
        pg = Postgres()
        # conn = pg.db_conn()
        # print('postgresql connect succeed.')
        # print('reaching datas...')
        try:
            sql = "select a.sys_code FSTR_INTERSECTID, b.node_name from pe_tobj_node_info a left join pe_tobj_node b " \
                  "on a.node_id = b.node_id where length(a.sys_code) < 5"
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


def main_detector_plot(cur_date_str, scatsid, detectorid, logger):
    # cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
    # cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
    # print(cur_date_str)
    # cur_date_str = '2018-11-15'
    # interInfor = call_postgresql(logger)
    detector_plot_data, detector_state = call_oracle(cur_date_str, scatsid, detectorid, logger)
    merge_detector_all = pd.DataFrame({})
    if not detector_plot_data.empty:
        # detector_all = detector_all.fillna(0)
        # print(detector_plot_data)
        detector_plot_data.sort_values(by="FSTR_CYCLE_STARTTIME", inplace=True)
        detector_plot_data['datetime'] = detector_plot_data['FSTR_CYCLE_STARTTIME'].map(
            lambda x: dt.datetime.strptime(cur_date_str + ' ' + x, "%Y-%m-%d %H:%M:%S"))
        detector_plot_data.set_index(["datetime"], inplace=True)
        # print(detector_plot_data)
        # detector_plot_data.sort_index()
        # temp_volume_series.index = temp_time_series
        # ts = pd.Series(temp_volume_series)

        flow_cg = detector_plot_data['FINT_ACTUALVOLUME'].resample('10min', label='left', closed='left').sum()
        flow_cg = flow_cg.fillna(0)
        ds_cg = detector_plot_data['FINT_DS'].resample('10min', label='left', closed='left').max()
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
        plot_data = {'flow': list(flow_cg), 'ds': list(ds_cg),
                     'time': time_str}
        # print(plot_data)
        # merge_detector_all = pd.merge(detector_all, interInfor, on='FSTR_INTERSECTID', how='left')
        # # print(merge_detector_all)
        # merge_detector_all = merge_detector_all.dropna(axis=0, how='any')
        # # merge_detector_all = merge_detector_all.reset_index(drop=True)
        # # print(merge_detector_all)
        # merge_detector_all.rename(columns={'FSTR_INTERSECTID': 'scatsId', 'SCORE': 'score',
        #                                    'LANENUM': 'laneNum', 'node_name': 'intName'}, inplace=True)
    else:
        plot_data = {}
    return plot_data, detector_state


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

    main_detector_plot('2018-11-15', '1', '6', logger)

