#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#############################
# Created on Mon Nov 26 2018
# Description: detector quality on signal specialist
# @author:  DDD
#############################
import cx_Oracle
import logging
import datetime as dt
import os
import pandas as pd
from proj.proj.config.database import Oracle


def call_oracle(cur_date_str, logger):
    abnormal_int = pd.DataFrame({})
    try:
        # db = cx_Oracle.connect('SIG_OPT_ADMIN/admin@192.168.20.56/orcl')
        # print('oracle connect succeed.')
        # print('reaching datas...')
        ora = Oracle()

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
        finally:
            ora.db_close()
    except Exception as e:
        logger.error('when calling oracle, ' + str(e))
    return abnormal_int


def main_abnormal_int(cur_date_str, logger):
    abnormal_int = call_oracle(cur_date_str, logger)
    if not abnormal_int.empty:
        abnormal_int.rename(columns={'SITEID': 'scatsId', 'SITENAME': 'intName'}, inplace=True)
        result_df = abnormal_int.to_json(orient='records', force_ascii=False)
        print(result_df)
        # result_df = json.loads(result_df)


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

    main_abnormal_int(cur_date_str, logger)

