# -*- coding: utf-8 -*-
from django.http import HttpResponse
from django.shortcuts import render_to_response
import json
from django.http import JsonResponse
from ..interfaceImpl.TestModelTestSvcImpl import *
from ..vo.TestModelTestVO import *
from ..config.task_registed.task_registed import RegistedTask
from ..controller.TaskModel import TaskRegister
import os, time,re
import datetime as dt
import pandas as pd
from ..controller import TaskModel
import json
from ..config.database import Postgres, Oracle
from ..config.sql_text import SqlText
import logging
from ..python_project.ali_alarm.alarm_priority_algorithm1126.alarm_auto_dispose import OperateAutoDis
from ..python_project.ali_alarm import main as ali_main
from ..python_project.detector_diagnosis.detector_diagnosis_list import main_detector_quality
from ..python_project.detector_diagnosis.detector_disgnosis_detail import main_detector_detail
from ..python_project.detector_diagnosis.detector_plot import main_detector_plot
from ..python_project.detector_diagnosis.maintain_list import main_add_maintain_list
from ..python_project.detector_diagnosis.maintain_list import main_get_maintain_list
from ..python_project.detector_diagnosis.maintain_list import main_get_one_list
from ..python_project.detector_diagnosis.maintain_list import main_get_service_state
from ..python_project.test_api.detector_result_merge import main_detector_result_merge

from ..python_project.scats_interface.data_request_check import InterfaceCheck
# import multiprocessing
# from multiprocessing import Queue, Process
from ..controller import TaskModel
import json
from ..config.database import Postgres,Oracle
from ..config.sql_text import SqlText
from ..tools.inter_face_return_manage import inter_face_manage
global task_inf

log = logging.getLogger('scripts')

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


task_inf = {}
RUNNING_TASK = {}
OperateStatus = False
autoAlarm = OperateAutoDis()


def getJson2(request):
    global task_inf
    todo_list = [
        {"id": "1", "content": "吃饭"},
        {"id": "2", "content": "吃饭"},
    ]
    if request.GET:
        if 'Interface' in request.GET and 'Flag' in request.GET:
            interface_name = request.GET['Interface']
            control_flag = request.GET['Flag']
            print(interface_name, control_flag)
            if interface_name in RegistedTask['TaskName']:
                if control_flag == "start" and interface_name not in RUNNING_TASK.keys():
                    task = TaskModel.TaskRegister(interface_name)
                    task.start_task()
                    RUNNING_TASK[interface_name] = task
                    task.check_status()
                    task_inf[interface_name] = task.task_state
                elif control_flag == "stop" and interface_name in RUNNING_TASK.keys():
                    task = RUNNING_TASK[interface_name]
                    task.stop_task()
                    RUNNING_TASK.pop(interface_name)

                elif control_flag == "restart" and interface_name in RUNNING_TASK.keys():
                    task = RUNNING_TASK[interface_name]
                    task.restart_task()
                elif control_flag == "state" and interface_name not in RUNNING_TASK.keys():
                    task = TaskModel.TaskRegister(interface_name)
                    task_inf[interface_name] = task.task_state

            todo_list = {"id": "1", "content": interface_name}
            # vo = TestModelTestVO(interface_name, "Running")
            # impl = TestModelTestSvcImpl()
            # impl.addOneRecode(vo)
        else:
            todo_list = {"id": "1", "content": "吃饭"}

    # print(task_inf)
    response = JsonResponse(task_inf, safe=False, json_dumps_params={'ensure_ascii': False})
    return response
    # resp = {'errorcode': 100, 'detail': 'Get success'}
    # return HttpResponse(json.dumps(resp), content_type="application/json")


def getJson1(request):
    todo_list = [
        {"id": "1", "content": "吃饭"},
        {"id": "2", "content": "吃饭"},
    ]
    if request.GET:
        if 'q' in request.GET:
            # print(todo_list)
            name = request.GET['q']
            todo_list = {"id": "1", "content": name}
            vo = TestModelTestVO(name)
            impl = TestModelTestSvcImpl()
            impl.addOneRecode(vo)
        else:
            todo_list = {"id": "1", "content": "吃饭"}
    response = JsonResponse(todo_list, safe=False, json_dumps_params={'ensure_ascii': False})
    return response
    # resp = {'errorcode': 100, 'detail': 'Get success'}
    # return HttpResponse(json.dumps(resp), content_type="application/json")

@inter_face_manage
def getOperate(request):
    # json_demo = {'userid':userid,'oper_time':oper_time,'scats_id':scats_id,'oper':oper,'meaning':meaning,
    #              'oper_type':oper_type,'siteid':siteid,'inter_id':inter_id,'inter_name':inter_name,'alarm_time':alarm_time,
    #              'alarm_id':alarm_id,'disp_id':disp_id}
    json_demo = {'appcode': False, 'result': []}
    print("get request!")
    if request.GET:
        json_demo2 = {'appcode': True, 'result': []}
        sql = SqlText.sql_getscats_operate
        if 'scatsId' in request.GET:
            inter_id = request.GET['scatsId']
            local_time = dt.datetime.now()
            edate = dt.datetime.strftime(local_time, '%Y-%m-%d %H:%M:%S')
            sdate = str(local_time.date()) + ' 00:00:00'
            # today = '2018-10-22'
            sql = sql.format(str(inter_id), sdate, edate)
            pg = Postgres()
            result = pg.call_pg_data(sql)
            pg.db_close()
            for i in result:
                operate = {'userId': None, 'userName': None, 'operTime': None, 'oper': None, 'operType': None}
                userid = i[0]
                user_name = i[4]
                oper_desc = i[5]
                operate['userId'] = userid
                if user_name is None:
                    operate['userName'] = '其他单位'
                else:
                    operate['userName'] = user_name
                if oper_desc is None:
                    operate['operType'] = i[3]
                else:
                    operate['operType'] = oper_desc
                oper_time = i[1]
                operate['operTime'] = oper_time.time()
                operate['oper'] = i[2]
                json_demo2['result'].append(operate)

        else:
            json_demo2['appcode'] = False
            # json_result = json.dumps(json_demo2, ensure_ascii=False)
        # print("接口返回：", json_demo2)
        # response = JsonResponse(json_demo2, safe=False, json_dumps_params={'ensure_ascii': False})
        response = JsonResponse(json_demo2, safe=True, json_dumps_params={'ensure_ascii': False})
        return response
    else:
        # print("接口返回：", json_demo)
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response


def getOperateRatio(request):
    # demo_pg_inf = {'database': "zkr", 'user': "postgres", 'password': "postgres",
    #                'host': "192.168.20.46", 'port': "5432"}
    json_demo = {'appcode': False, 'spilt': [], 'cycle': []}
    # print(request.method)
    if request.method == 'GET':
        json_demo = {'appcode': True, 'spilt': [], 'cycle': []}
        sql1 = SqlText.sql_get_spilt_number
        sql2 = SqlText.sql_get_cycle_number
        pg = Postgres.get_instance()
        result1 = pg.call_pg_data(sql1)
        result2 = pg.call_pg_data(sql2)

        # print(result1)
        # print(result2)
        x1 = 0
        y1 = 0
        z1 = 0
        operate = [{'text': '自适应方案', 'value': ''}, {'text': '固定方案', 'value': ''}, {'text': '其他方案', 'value': ''}]
        if result1 is not None:
            for i in result1:
                if i[0] == 'adaptive':
                    x1 = i[1]
                if i[0] == 'fixed':
                    y1 = i[1]
                if i[0] == 'other':
                    z1 = i[1]
            if x1 == 0 and y1 == 0 and z1 == 0:
                x1 = 1
                y1 = 1
                z1 = 1
            adaptive = round((x1 / (x1 + y1 + z1)) * 100, 1)
            fixed = round((y1 / (x1 + y1 + z1)) * 100, 1)
            other = round((z1 / (x1 + y1 + z1)) * 100, 1)
            operate[0]['value'] = adaptive
            operate[1]['value'] = fixed
            operate[2]['value'] = other
            json_demo['spilt'] = operate
            x2 = 0
            y2 = 0
        if result2 is not None:
            for i in result2:
                operate = [{'text': '自适应方案', 'value': ''}, {'text': '固定方案', 'value': ''}]
                if i[0] == 'unlock':
                    x2 = i[1]
                if i[0] == 'lock':
                    y2 = i[1]
            if x2 == 0 and y2 == 0:
                x2 = 1
                y2 = 1
            adaptive = round((x2 / (x2 + y2)) * 100, 1)
            fixed = round((y2 / (x2 + y2)) * 100, 1)
            operate[0]['value'] = adaptive
            operate[1]['value'] = fixed
            json_demo['cycle'] = operate

        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response

    elif request.POST:
        # print('无法POST')
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response

@inter_face_manage
def getAlarmAuto(request):
    global autoAlarm
    json_demo = {'appcode': False, 'result': []}
    if request.GET:
        json_demo2 = {'appcode': True, 'result': []}
        if 'intList' in request.GET:
            inter_id = request.GET['intList']
            log.info(inter_id)
            try:
                result = json.loads(inter_id)
            except Exception as e:
                message = "json数据格式有误"
                json_demo2['result'].append(message)
            else:
                try:
                    # print("result", result)
                    int_list = []
                    for i in result:
                        int_id = i['interId']
                        int_list.append(int_id)
                    result = autoAlarm.alarm_auto_judge(int_list)
                    print('result', result)
                except Exception as e:
                    print(e)
                    message = "计算异常，超时"
                    json_demo2['result'].append(message)

                else:
                    json_demo2['result'] = result
            finally:
                log.info(json_demo2)
                response = JsonResponse(json_demo2, safe=False, json_dumps_params={'ensure_ascii': False})
                return response
    else:
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response


# @inter_face_manage
def getAvailability(request):
    result = {'appCode': 0, 'result': 0}
    if request.GET:
        result = {'appCode': 1, 'result': 0}
        if 'scatsID' in request.GET:
            scatsid = request.GET.get('scatsID')
            print('getAvailability---scatsid', scatsid)
            result_json = []
            if scatsid == 'all':
                # 所有路口
                sql = "select a.FSTR_INTERSECTID, B.SITENAME, to_char(A.SCORE*10) SCORE from AVAILABLE_SCORE a LEFT JOIN " \
                      "INTERSECT_INFORMATION B ON a.FSTR_INTERSECTID = B.SITEID " \
                      "WHERE a.FSTR_DATE = '{0}' ORDER BY TO_NUMBER(A.SCORE) DESC"
                print(sql)
                date = str(dt.datetime.now() - dt.timedelta(days=1))[:10]
                # print('date', date)
                before_nowtime = "2018-11-12"
                sql = sql.format(date)
                try:
                    O1 = Oracle()
                    result_df = O1.call_oracle_data(sql, fram=False)
                    # print('result_df', result_df)
                    result_json = result_df.to_json(orient='index', force_ascii=False)
                    # print('result_json', result_json)
                    O1.db_close()
                except Exception as e:
                    print('e ', e)
            else:
                # 单路口
                sql = "select FSTR_INTERSECTID, to_char(round(SCORE*10)) score from AVAILABLE_SCORE " \
                      "WHERE FSTR_DATE = '{0}' and FSTR_INTERSECTID = '{1}'"
                date = str(dt.datetime.now() - dt.timedelta(days=1))[:10]
                # print('date', date)
                before_nowtime = "2018-11-12"
                sql = sql.format(date, scatsid)
                try:
                    O1 = Oracle()
                    result_df = O1.call_oracle_data(sql, fram=False)
                    # print('result_df', result_df)
                    # result_df.rename(columns={'FSTR_INTERSECTID': 'scatsID', 'SCORE': 'score'},
                    #                  inplace=True)
                    if not result_df.empty:
                        result_json = {'scatsID': result_df.iloc[0]['FSTR_INTERSECTID'], 'score': result_df.iloc[0]['SCORE']}
                    else:
                        result_json = {}
                    # result_json = result_df.to_json(orient='records', force_ascii=False)
                    print('result_json', result_json)
                    O1.db_close()
                except Exception as e:
                    print('e ', e)
            # result = {'appCode': 1, 'Result': {'ScatsID': scatsid, 'AvilableRate': score}}
            result = {'appCode': 1, 'result': result_json}
    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    # response = result_json
    return response


# 检测器质量诊断列表
def getDetectorList(request):
    result = {'appCode': 0, 'result': {}}
    if request.GET:
        if 'PageSize' in request.GET and 'PageNum' in request.GET:
            pageSize = int(request.GET.get('PageSize'))
            pageNum = int(request.GET.get('PageNum'))
            try:
                cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
                cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
                # cur_date_str = '2018-11-15'
                merge_detector_all = main_detector_quality(cur_date_str, log)
                if not merge_detector_all.empty:
                    data_length = len(merge_detector_all)
                    result_df = merge_detector_all.iloc[((pageNum - 1)*pageSize):(pageNum*pageSize), :]
                    if not result_df.empty:
                        result_df = result_df.to_json(orient='records', force_ascii=False)
                        result_df = json.loads(result_df)
                        result = {'appCode': 1, 'result': {'dataLength': data_length, 'data': result_df}}
                    else:
                        result = {'appCode': 0, 'result': {'dataLength': data_length, 'data': []}}
            except Exception as e:
                print(e)
                log.error(e)
    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    return response

# 连inter_info的接口有getDetectorList, getDetectorDetail, getDetectorPlot, addMaintainList


# 路口检测器质量详情
def getDetectorDetail(request):
    result = {'appCode': 0, 'result': {}}
    if request.GET:
        if 'nodeId' in request.GET:
            nodeid = request.GET.get('nodeId')
            pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
                                  'host': "33.83.100.145", 'port': "5432"})
            sql = "select sys_code from pe_tobj_node_info where node_id = '{0}'".format(nodeid)
            IntInfor = pg.call_pg_data(sql)
            scatsid = IntInfor[0][0]
            print('scatsid', scatsid)
            pg.db_close()
            try:
                cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
                cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
                # cur_date_str = '2018-11-15'
                merge_detector_detail, available_score = main_detector_detail(cur_date_str, scatsid, log)
                if not merge_detector_detail.empty and available_score:
                    result_df = merge_detector_detail.to_json(orient='records', force_ascii=False)
                    result_df = json.loads(result_df)
                    result = {'appCode': 1, 'result': {'scatsId': available_score[0][0],
                                                       'intName': available_score[0][1],
                                                       'availableScore': round(float(available_score[0][2])*10),
                                                       'data': result_df}}
                else:
                    result = {'appCode': 0, 'result': {}}
            except Exception as e:
                print(e)
                log.error(e)

    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    return response


# 路口检测器画图
def getDetectorPlot(request):
    result = {'appCode': 0, 'result': {}}
    if request.GET:
        if 'nodeId' in request.GET and 'detectorId' in request.GET:
            nodeid = request.GET.get('nodeId')
            pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
                                  'host': "33.83.100.145", 'port': "5432"})
            sql = "select sys_code from pe_tobj_node_info where node_id = '{0}'".format(nodeid)
            IntInfor = pg.call_pg_data(sql)
            scatsid = IntInfor[0][0]
            pg.db_close()

            # scatsid = request.GET.get('scatsId')
            # print('scatsid', scatsid)
            detectorid = request.GET.get('detectorId')
            cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
            cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
            # cur_date_str = '2018-11-15'
            try:
                plot_data, detector_state = main_detector_plot(cur_date_str, scatsid, detectorid, log)
                result = {'appCode': 1, 'result': {'detectorCode': detector_state[0][0],
                                                   'detectorState': detector_state[0][1], 'plotData': plot_data}}
                # print(plot_data)
            except Exception as e:
                log.error(e)
                print(e)
    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    return response


# 新增运维清单
def addMaintainList(request):
    if request.method == 'POST':
        postBody = request.body
        json_result = json.loads(postBody)
        # json_result = {"listName": "测试清单名称", "listNode":[
        # "93cd4bc6e84934100a6e70d8b86515c8",
        # "93cd4bc6e84934100a6e70d8b86515c8",
        # "93cd4bc6e84934100a6e70d8b86515c8"]}
        print('addMaintainList', json_result)
        try:
            cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
            cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
            # cur_date_str = '2018-11-12'
            main_add_maintain_list(json_result, cur_date_str, log)
            result = {'appCode': 1}
        except Exception as e:
            log.error(e)
            print('addMaintainList', e)
            result = {'appCode': 0}
    else:
        result = {'appCode': 0}
    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    return response


# 显示所有运维清单
def getMaintainList(request):
    result = {'appCode': 0, 'result': []}
    # if request.GET:
    # if 'date' in request.GET:
    try:
        exist_maintain = main_get_maintain_list(log)
        if not exist_maintain.empty:
            result_df = exist_maintain.to_json(orient='records', force_ascii=False)
            result_df = json.loads(result_df)
            result = {'appCode': 1, 'result': result_df}
    except Exception as e:
        log.error(e)
        print('getMaintainList', e)
    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    return response


# 删除一个运维清单
def deleteMaintainList(request):
    result = {'appCode': 0}
    if request.GET:
        if 'listId' in request.GET:
            listID = request.GET.get('listId')
            try:
                ora = Oracle()
                conn, cr = ora.db_conn()
                sql = "delete from MAINTAIN_LIST where fstr_id = '{0}'".format(listID)
                cr.execute(sql)
                conn.commit()
                ora.db_close()
                result = {'appCode': 1}
                # ora.
            except Exception as e:
                log.error(e)
                print(e)
    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})

    return response


# 获取异常路口列表
def getAbnormalIntersect(request):
    result = {'appCode': 0, 'result': []}
    # if request.GET:
    # if 'date' in request.GET:
    try:
        cur_date = request.GET.get('date')
        print(os.path.dirname(os.getcwd()))
        print(os.path.dirname(os.path.realpath(__file__)))
        print(os.path.abspath(os.path.dirname(os.getcwd())))
        f = open(os.path.dirname(os.getcwd()) + '\\proj\\proj\\python_project\\detector_diagnosis\\abnormal_intersection.txt')
        x = f.read()
        f.close()
        result_json = json.loads(x)
        print(result_json)
        result = {'appCode': 1, 'result': result_json}
    except Exception as e:
        log.error(e)
        print('aaaaaaaaaaaaaaaaaaaaaaaaaaa' + str(e))

    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})


    return response


# 导出原始运维检测器报告
def exportOriginalList(request):
    result = {'appCode': 0, 'result': []}
    if request.GET:
        if 'listId' in request.GET:
            listid = request.GET.get('listId')
            try:
                list_detail = main_get_one_list(listid, log)
                if not list_detail.empty:
                    result_df = list_detail.to_json(orient='records', force_ascii=False)
                    result_df = json.loads(result_df)
                    result = {'appCode': 1, 'result': result_df}
            except Exception as e:
                log.error(e)
                print(e)
        # result = {'appCode': 0, 'result': [{"scatsId": "263", "intName": "中诸葛路-德胜中路", "detectorId": 1, "dataType": "数据正常", "dataDetail": "数据准确", "availableScore": 90, "ifMaintain": ""},
        #                                    {"scatsId": "263", "intName": "中诸葛路-德胜中路", "detectorId": 1, "dataType": "数据异常", "dataDetail": "大量数据噪点", "availableScore": 90, "ifMaintain": "是"},
        #                                    {"scatsId": "263", "intName": "中诸葛路-德胜中路", "detectorId": 1, "dataType": "数据正常", "dataDetail": "少量数据噪点", "availableScore": 90, "ifMaintain": ""}]}
    # else:
        # result = {'appCode': 0, 'Result': 0}

    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    # response = result_json
    return response


# 导出检测器报告修复情况
def exportCurrentListState(request):
    result = {'appCode': 0, 'result': []}
    if request.GET:
        if 'listId' in request.GET:
            listid = request.GET.get('listId')
            # print(11111111111111111111111111111111)
            try:
                cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
                cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
                # cur_date_str = '2018-11-15'
                # print(listid)
                result = {'appCode': 1, 'result': main_get_service_state(cur_date_str, listid, log)}
            except Exception as e:
                log.error(e)
                print(e)
    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    # response = result_json
    return response


# 【测试接口】 检测器离线诊断及scats与地磁融合
def getDetectorMerge(request):
    result = {'appCode': 0, 'result': []}
    if request.GET:
        if 'nodeId' in request.GET:
            nodeid = request.GET.get('nodeId')
            try:
                # pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
                #                       'host': "192.168.20.46", 'port': "5432"})
                pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
                                      'host': "33.83.100.145", 'port': "5432"})

                sql = "select sys_code from pe_tobj_node_info where node_id = '{0}'".format(nodeid)
                IntInfor = pg.call_pg_data(sql)
                scatsid = IntInfor[0][0]
                print('scatsid', scatsid)
                pg.db_close()
                # scatsid = request.GET.get('scatsId')
                cur_date_dt = dt.datetime.now() - dt.timedelta(days=1)
                cur_date_str = dt.datetime.strftime(cur_date_dt, "%Y-%m-%d")
                # cur_date_str = '2018-11-12'
                merge_detector_detail = main_detector_result_merge(cur_date_str, scatsid, log)
                result_df = merge_detector_detail.to_json(orient='records', force_ascii=False)
                result_df = json.loads(result_df)
                result = {'appCode': 1, 'result': result_df}
                # print(result_df)
                # result = {'appCode': 1, 'result': [{'detectorId': 1, 'phase': 'A', 'function': '西口左转',
                #                                     'scatsDataType': '数据准确', 'scatsDataDetail': '数据准确',
                #                                     'geoDataType': '数据准确', 'geoDataDetail':
                #                                         '数据准确', 'merge': '数据准确'},
                #                                    {'detectorId': 2, 'phase': 'B', 'function': '西口直行',
                #                                      'scatsDataType': '数据准确',
                #                                      'scatsDataDetail': '数据准确', 'geoDataType': '数据准确',
                #                                      'geoDataDetail': '数据准确', 'merge': '数据准确'},
                #                                    {'detectorId': 3, 'phase': 'B', 'function': '西口直行',
                #                                      'scatsDataType': '数据准确',
                #                                      'scatsDataDetail': '数据准确', 'geoDataType': '数据准确',
                #                                      'geoDataDetail': '数据准确', 'merge': '数据准确'}
                #                                     ]}
            except Exception as e:
                log.error(e)
                print(e)
    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    return response


# 【测试接口】 路口列表 在线检测器
def testDetectorList(request):
    result = {'appCode': 0, 'result': []}
    try:
        # pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
        #                       'host': "192.168.20.46", 'port': "5432"})
        pg = Postgres(pg_inf={'database': "inter_info", 'user': "django", 'password': "postgres",
                              'host': "33.83.100.145", 'port': "5432"})
        sql = "select sys_code , node_id from pe_tobj_node_info where length(sys_code) < 5"
        IntInfor = pg.call_pg_data(sql, fram=True)
        pg.db_close()

        ora = Oracle()
        sql = "select SITEID, SITENAME FROM INTERSECT_INFORMATION"
        int_list = ora.call_oracle_data(sql, fram=True)
        int_list.rename(columns={'SITEID': 'sys_code'}, inplace=True)
        merge_int = pd.merge(int_list, IntInfor, on='sys_code', how='left')
        merge_int.rename(columns={'sys_code': 'scatsId', 'SITENAME': 'intName', 'node_id': 'nodeId'}, inplace=True)
        result_df = merge_int.to_json(orient='records', force_ascii=False)
        result_df = json.loads(result_df)
        result = {'appCode': 1, 'result': result_df}
    except Exception as e:
        log.error(e)
        print(e)

    # result = {'appCode': 1, 'result': [{'scatsId': '62', 'intName': '庆春东路-秋涛北路', 'nodeId': 'a1d7bd39c1106ea825ecd6520186b5e6'},
    #                                     {'scatsId': '680', 'intName': '天城路-明月桥路', 'nodeId': '04b7875d89a5d9dd7f52cf98e3b9267c'}]}
    # print(result)
    response = JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})
    return response


# 获取接口请求数据量信息
@inter_face_manage
def getInterfaceStatus(request):
    # print(request.method)
    def plot_color_judge(record_time, loss_time):
        if record_time in loss_time:
            plot_color = 2
        else:
            plot_color = 1
        return plot_color

    if request.method == 'GET':
        if 'sTime' in request.GET and 'eTime' in request.GET:
            # json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': []}
            sTime = request.GET['sTime']
            eTime = request.GET['eTime']
            # print(sTime,type(sTime))
            # print(eTime)
            pg = Postgres(pg_inf=SqlText.pg_inf_arith)
            result = pg.call_pg_data(SqlText.sql_get_interface_status.format(sTime,eTime), fram=True)

            ################################
            result['plot_color'] = None
            interface_neme = result['interface_name'].drop_duplicates().values.tolist()
            # today = dt.datetime.now().strftime("%Y-%m-%d") + ' 00:00:00'
            # now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            LossCheck = InterfaceCheck()
            final_df = pd.DataFrame(columns=result.columns)
            for name in interface_neme:
                loss_time = LossCheck.loss_data_period_check(name, sTime, eTime)
                match_result = result[result['interface_name']==name]
                print('loss_time',loss_time)
                # print(match_result['record_time'].tolist())
                # print(type(match_result['record_time'].values.tolist()[0]))
                match_result['plot_color'] = match_result['record_time'].apply(lambda x: 1 if x.strftime("%Y-%m-%d %H:%M:%S") not in loss_time else 2)
                # print(match_result)
                final_df = final_df.append(match_result)
            # print(final_df,'final_df')
            dict = final_df.to_dict(orient='records')
            ################################
            json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}
        else:
            json_demo = {'appcode': False, 'message': '请求失败，参数有误！', 'result': []}
            pass
        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response
    elif request.POST:
        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response

# 获取解析失败路口检测器信息
@inter_face_manage
def getParseFailed(request):
    json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确!', 'result': []}
    print(request.method)

    if request.method == 'GET':
        if 'date' in request.GET:
            date = request.GET['date']
            pg = Postgres(pg_inf=SqlText.pg_inf_arith)
            print(date)
            try:
                assert re.search(r"(\d{4}-\d{1,2}-\d{1,2})", date), '日期格式错误'
                result = pg.call_pg_data(SqlText.sql_get_parse_failed_detector.format(date), fram=True)
            except AssertionError:
                print("参数格式有误")
                json_demo = {'appcode': False, 'message': '请求失败，参数有误！', 'result': []}
            else:
                dict = result.to_dict(orient='records')
                json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}
            response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
            return response
        else:
            json_demo = {'appcode': False, 'message': '请求失败，参数有误！', 'result': []}
            response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
            return response

    elif request.POST:

        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确！', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response

# 获取定时任务状态
@inter_face_manage
def getScheTask(request):
    json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确!', 'result': []}
    print(request.method)
    if request.method == 'GET':

        pg = Postgres(pg_inf=SqlText.pg_inf_django)
        result = pg.call_pg_data(SqlText.sql_sche_check, fram=True)
        dict = result.to_dict(orient='records')
        json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}
        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response

    elif request.POST:

        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确！', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response

# 获取车道饱和状态
@inter_face_manage
def getLaneStatus(request):
    json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确!', 'result': []}
    print(request.method)
    if request.method == 'GET':
        ora = Oracle()
        result = ora.call_oracle_data(SqlText.sql_get_lane_status, fram=True)
        result.columns = ['scats_id', 'detector_id','date','time','cycle','BLC','MLC','OLC','VOLUMN']
        dict = result.to_dict(orient='records')
        json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}

        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response

    elif request.POST:

        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确！', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response

# 获取接口请求记录
def getRequestManage(request):
    json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确!', 'result': []}
    print(request.method)
    if request.method == 'GET':
        pg = Postgres(SqlText.pg_inf_django)
        result = pg.call_pg_data(SqlText.sql_get_request_manage, fram=True)
        # result.columns = ['scats_id', 'detector_id','date','time','cycle','BLC','MLC','OLC','VOLUMN']
        dict = result.to_dict(orient='records')
        json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}
        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response

    elif request.POST:

        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确！', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response


def pagedisplay(request):
    dict={}
    sql="select * from scats_int_state_feedback"
    demo_pg_inf = {'database': "signal_specialist", 'user': "postgres", 'password': "postgres",
                   'host': "33.83.100.146", 'port': "5432"}
    pp=demo_pg_inf
    db = psycopg2.connect(dbname=pp['database'], user=pp['user'], password=pp['password'], host=pp['host'],
                          port=pp['port'])
    cr = db.cursor()
    cr.execute(sql)
    x=cr.fetchall()
    db.close()
    for i in x:
        siteid=i[8]
        updatatime=i[9]
        user_name=i[0]
        split=i[1]
        cycle=i[2]
        coordination=i[3]
        dwell=i[5]
        pp=i[4]
        xsf=i[6]
        other=i[7]
        split_plan_selection=i[10]
        dict[siteid]={'siteid':siteid,'updatatime':updatatime,'user_name':user_name,'split':split,'cycle':cycle,
                        'coordination':coordination,'dwell':dwell,'pp':pp,'xsf':xsf,'other':other,
                      'split_plan_selection':split_plan_selection}
    if request.method=='GET':
        response = JsonResponse(dict, safe=False, json_dumps_params={'ensure_ascii': False})
        return response
    else:
        dict = {'call data failure': 0}
        response = JsonResponse(dict, safe=False, json_dumps_params={'ensure_ascii': False})
        return response