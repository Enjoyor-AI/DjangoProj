from proj.config.database import Oracle, Postgres,ConnectInf
from proj.python_project.scats_interface.pram_conf import *
import datetime as dt
import cx_Oracle
import pandas as pd
from proj.tools.func_timer import timer
import requests
import json
from proj.config.sql_text import SqlText
from .data_request_check import InterfaceCheck


# def time_count(func):
#     def warpper(*args, **kwargs):
#         stime = dt.datetime.now()
#         res = func(*args, **kwargs)
#         etime = dt.datetime.now()
#         print("[DeBug] {0} CostTime: {1} ".format(func.__name__, (etime - stime).seconds))
#         return res
#
#     return warpper


class InterfaceStatus():

    def __init__(self):
        self.status = None
        self.pg = Postgres(SqlText.pg_inf_arith)
        self.ora = Oracle(SqlText.OracleUser)
        self.conn, self.cr = self.ora.db_conn()

    @timer
    def salklist_status(self, date, stime, etime):

        try:
            self.cr.execute(sql_get_salklist_status, a=date, b=stime, f=etime)
        except cx_Oracle.InterfaceError:
            self.conn, self.cr = self.ora.db_conn()
            self.cr.execute(sql_get_salklist_status, a=date, b=stime, f=etime)

        result = self.cr.fetchall()
        # print(result)
        self.conn.commit()
        # self.ora.db_close()
        data_num = result[0][0]
        return data_num

    @timer
    def operate_status(self, stime, etime):
        # stime = dt.datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')
        # etime = dt.datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')

        try:
            self.cr.execute(sql_get_opetate_status, a=stime, b=etime)
        except cx_Oracle.InterfaceError:
            self.conn, self.cr = self.ora.db_conn()
            self.cr.execute(sql_get_opetate_status, a=stime, b=etime)
        result = self.cr.fetchall()
        # print(result)
        self.conn.commit()
        data_num = result[0][0]
        return data_num

    @timer
    def parsing_failed_check(self, date, start_time, end_time):
        def tuple2frame(result, index):
            column_name = []
            for i in range(len(index)):
                index_name = index[i][0]
                column_name.append(index_name)
            result = pd.DataFrame(result, columns=column_name)
            return result
        # stime = dt.datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')
        # etime = dt.datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')
        try:
            self.cr.execute(sql_failed_detector, a=date, b=parse_failed_judge,c=start_time,d=end_time)
            print(sql_failed_detector)
        except cx_Oracle.InterfaceError:
            # 如果连接异常，重新连接数据库
            self.conn, self.cr = self.ora.db_conn()
            self.cr.execute(sql_failed_detector, a=date, b=parse_failed_judge,c=start_time,d=end_time)

        index = self.cr.description
        result = self.cr.fetchall()
        fresult = None
        if result is not None:
            fresult = tuple2frame(result, index)
        if IF_TEST:     #测试模式
            result = list(result)
            for i in range(len(result)):
                result[i] = list(result[i])
                # result[i][3] = str(dt.datetime.now().date())
        self.conn.commit()
        return fresult

    def salk_send(self, delta=15):
        current_time = dt.datetime.now()
        start_time = current_time - 2*dt.timedelta(minutes=delta)
        end_time = current_time - dt.timedelta(minutes=delta)
        current_date = str(current_time.date())
        if IF_TEST:
            current_date = TEST_DATE
            test_time = TEST_DATE + ' ' + str(current_time.strftime('%H:%M:%S'))
            start_time = dt.datetime.strptime(test_time, "%Y-%m-%d %H:%M:%S") - dt.timedelta(minutes=delta)
        # print(current_date, start_time.strftime("%H:%M:%S"), current_time.strftime("%H:%M:%S"))
        salk_num = self.salklist_status(current_date, start_time.strftime("%H:%M:%S"),
                                        end_time.strftime("%H:%M:%S"))
        if salk_num == 0:
            StartTime = (dt.datetime.now() - dt.timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
            EndTime = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            SiteID = '1'
            payload1 = {'SiteID': SiteID, 'STime': StartTime, 'ETime': EndTime}
            # 4.3获取战略运行记录
            try:
                RunStrInformation = requests.get(r'http://33.83.100.138:8080/getStrategicmonitor.html',
                                                 params=payload1, timeout=5)  # 4.3
                RunStrInformation = RunStrInformation.text
            except Exception as e:
                print(type(e))
                # Log.warning("RunStrInformation: request timeout!")
                exception = "request failed,error type: %s" % type(e)
            else:
                RunStrInformation = json.loads(RunStrInformation)
                if len(RunStrInformation['resultList']) > 0:
                    exception = "request success,data length:%s" % len(RunStrInformation['resultList'])
                else:
                    exception = "request success,but no data"
        else:
            exception = None
        message_salk = [['战略运行记录接口', current_time, salk_num, exception]]
        self.pg.send_pg_data(sql=sql_send_message, data=message_salk)
        return

    def operate_send(self, delta=15):
        current_time = dt.datetime.now()
        # start_time = current_time - dt.timedelta(minutes=delta)
        start_time = current_time - 2 * dt.timedelta(minutes=delta)
        end_time = current_time - dt.timedelta(minutes=delta)
        if IF_TEST:
            test_time = TEST_DATE + ' ' + str(current_time.strftime('%H:%M:%S'))
            test_time = dt.datetime.strptime(test_time, "%Y-%m-%d %H:%M:%S")
            test_start_time = current_time - dt.timedelta(minutes=delta)
            operate_num = self.operate_status(test_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                              test_time.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            operate_num = self.operate_status(start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                              end_time.strftime("%Y-%m-%d %H:%M:%S"))
        if operate_num == 0:
            StartTime = (dt.datetime.now() - dt.timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
            EndTime = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            payload1 = {r'STime': StartTime, r'ETime': EndTime}
            try:
                get_response = requests.get(r'http://33.83.100.138:8080/getOperatorIntervention.html',
                                            params=payload1, timeout=10)  # 4.7
                GetManoperationRecord = get_response.text
                print("操作记录请求成功")
            except Exception as e:
                print(e)
                exception = "request failed,error type: %s" % type(e)
            else:
                GetManoperationRecord = json.loads(GetManoperationRecord)
                if len(GetManoperationRecord['resultList']) > 0:
                    exception = "request success,data length:%s" % len(GetManoperationRecord['resultList'])
                else:
                    exception = "request success,but no data"
        else:
            exception = None
        message_operate = [['人工操作记录接口', current_time, operate_num, exception]]
        self.pg.send_pg_data(sql=sql_send_message, data=message_operate)
        return

    def parse_failed_detector_send(self, delta):
        current_time = dt.datetime.now()
        # current_date = current_time.date()
        start_time = (current_time - dt.timedelta(minutes=delta)).strftime('%H:%M:%S')
        end_time = (current_time).strftime('%H:%M:%S')
        current_date = str(current_time.date())
        if IF_TEST:
            current_date = TEST_DATE
        failed_detector = self.parsing_failed_check(current_date, start_time, end_time)


        if failed_detector is not None:
            try:
                pg = Postgres(ConnectInf.pg_inf_inter_info)
                pg.execute(sql_delete_channel_data)
                node_inf = pg.call_pg_data(sql_get_node_inf, fram=True)
                node_inf.columns = ['node_id', 'FSTR_INTERSECTID']
                merge_result = pd.merge(failed_detector, node_inf, how='left', on=['FSTR_INTERSECTID'])
                return_message = merge_result.values.tolist()
                pg.send_pg_data(sql=sql_send_parse_failed_detector, data=return_message)
            except Exception as e:
                print(e)
        return


def main(delta):
    I = InterfaceStatus()
    I.salk_send(delta)
    I.operate_send(delta)
    I.parse_failed_detector_send(delta)
    I.ora.db_close()


if __name__ == "__main__":
    main(15)
    # current_time = dt.datetime.now()
    # I = InterfaceStatus()
    # salk_num = I.salklist_status('2018-11-05', '09:00:00', '10:00:00')
    # operate_num = I.operate_status('2018-11-05 09:00:00', '2018-11-05 10:00:00')
    # message_salk = [['战略运行记录接口', current_time, salk_num, None]]
    # I.pg.send_pg_data(sql=sql_send_message, data=message_salk)
    # message_operate = [['人工操作记录接口', current_time, operate_num, None]]
    # I.pg.send_pg_data(sql=sql_send_message, data=message_operate)
    # failed_detector = I.parsing_failed_check('2018-11-05')
    # I.pg.send_pg_data(sql=sql_send_parse_failed_detector, data=failed_detector)
    # df_failed_detector = pd.DataFrame(failed_detector, columns=['scats_id', 'sa_no', 'detector'])
    # print("salk_num:", salk_num)
    # print("operate_num:", operate_num)
    # print("parsing_failed_check:", len(failed_detector))
    # # json = df_failed_detector.to_json(orient='records')
    # dict = df_failed_detector.to_dict(orient='records')
    # print(dict)
