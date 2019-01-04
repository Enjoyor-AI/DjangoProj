#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#############################
# Created on Tue Nov 16 2017
# Description: Plan Selection
# @author:  A
#############################

import pandas as pd
import cx_Oracle
import datetime as dt
import numpy as np
import psycopg2
import math
import logging
import json
# from TransportationSignalOptPlatform import GlobalVar
#from ..scats_operate_parsing_wr import GlobalVar
import GlobalVar

import os


class CONSTANTS:
    MAX_DEV = 10
    MAX_ADP_NUM = 0
    MAX_FIX_NUM = 0

    PHASE_NUM = 7

    tmp_rate = 25
    max_plan_rate = 50
    min_plan_rate = 10

    time_delta = 250  # 判断异常点是否连续的时间间隔
    lock_num = 120  # 判断是否连续锁定的周期个数（6小时）
    fix_plan_unfit_per = 20  # 判断固定方案不合理的异常点百分比

    morning_peak_start = '06:30:00'
    morning_peak_end = '09:30:00'
    afternoon_peak_start = '16:30:00'
    afternoon_peak_end = '19:30:00'
    peak_num = 60
    peak_ab_per = 50


class PlanSelectionPro(object):
    def __init__(self, pro_code, pro_name, pro_detail, pro_title, pro_reference):
        self.pro_code = pro_code
        self.pro_name = pro_name
        self.pro_detail = pro_detail
        self.pro_title = pro_title
        self.pro_reference = pro_reference


class PlanSelectionStatus:
    no_selection_pro   = PlanSelectionPro(GlobalVar.no_selection_pro_code, GlobalVar.no_selection_pro_name, GlobalVar.no_selection_pro_detail, GlobalVar.no_selection_pro_title, GlobalVar.no_selection_pro_reference)
    no_select_all_plan = PlanSelectionPro(GlobalVar.no_select_all_plan_code, GlobalVar.no_select_all_plan_name, GlobalVar.no_select_all_plan_detail, GlobalVar.no_select_all_plan_title, GlobalVar.no_select_all_plan_reference)
    tmp_rate_high      = PlanSelectionPro(GlobalVar.tmp_rate_high_code, GlobalVar.tmp_rate_high_name, GlobalVar.tmp_rate_high_detail, GlobalVar.tmp_rate_high_title, GlobalVar.tmp_rate_high_reference)
    no_select_plan     = PlanSelectionPro(GlobalVar.no_select_plan_code, GlobalVar.no_select_plan_name, GlobalVar.no_select_plan_detail, GlobalVar.no_select_plan_title, GlobalVar.no_select_plan_reference)
    kept_one_plan      = PlanSelectionPro(GlobalVar.kept_one_plan_code, GlobalVar.kept_one_plan_name, GlobalVar.kept_one_plan_detail, GlobalVar.kept_one_plan_title, GlobalVar.kept_one_plan_reference)
    plan_irrational    = PlanSelectionPro(GlobalVar.plan_irrational_code, GlobalVar.plan_irrational_name, GlobalVar.plan_irrational_detail, GlobalVar.plan_irrational_title, GlobalVar.plan_irrational_reference)
    tmp_plan_lock      = PlanSelectionPro(GlobalVar.tmp_plan_lock_code, GlobalVar.tmp_plan_lock_name, GlobalVar.tmp_plan_lock_detail, GlobalVar.tmp_plan_lock_title, GlobalVar.tmp_plan_lock_reference)
    one_plan_lock      = PlanSelectionPro(GlobalVar.one_plan_lock_code, GlobalVar.one_plan_lock_name, GlobalVar.one_plan_lock_detail, GlobalVar.one_plan_lock_title, GlobalVar.one_plan_lock_reference)
    fix_plan_unfit     = PlanSelectionPro(GlobalVar.fix_plan_unfit_code, GlobalVar.fix_plan_unfit_name, GlobalVar.fix_plan_unfit_detail, GlobalVar.fix_plan_unfit_title, GlobalVar.fix_plan_unfit_reference)
    peak_no_fix        = PlanSelectionPro(GlobalVar.peak_no_fix_code, GlobalVar.peak_no_fix_name, GlobalVar.peak_no_fix_detail, GlobalVar.peak_no_fix_title, GlobalVar.peak_no_fix_reference)


# 类型定义
class SPLITPLAN(object):
    def __init__(self, intersectid, plantype, planid, planstarttime, planendtime, phase_a, phase_b, phase_c, phase_d,
                 phase_e, phase_f, phase_g, day_to_use):
        self.intersectid = intersectid
        self.plantype = plantype
        self.planid = planid

        """
        self.planstarttime=planstarttime
        self.planendtime=planendtime        
        """
        if plantype == "FIXED":
            self.planstarttime = dt.datetime.strptime(planstarttime, "%H:%M:%S")
        else:
            self.planstarttime = dt.datetime.strptime("00:00:00", "%H:%M:%S")

        if plantype == "FIXED":
            self.planendtime = dt.datetime.strptime(planendtime, "%H:%M:%S")
        else:
            self.planendtime = dt.datetime.strptime("00:00:00", "%H:%M:%S")

        self.phase_a = phase_a
        self.phase_b = phase_b
        self.phase_c = phase_c
        self.phase_d = phase_d
        self.phase_e = phase_e
        self.phase_f = phase_f
        self.phase_g = phase_g
        # TO SET THE WEEKDAY FOR USING THE PLAN
        self.day_to_use = day_to_use  # 0728 xujia

    def deviation(self, s_phase_a, s_phase_b, s_phase_c, s_phase_d, s_phase_e, s_phase_f, s_phase_g):
        # DING.ALTER
        if not s_phase_a or s_phase_a == 1:
            s_phase_a = 0
        if not s_phase_b or s_phase_b == 1:
            s_phase_b = 0
        if not s_phase_c or s_phase_c == 1:
            s_phase_c = 0
        if not s_phase_d or s_phase_d == 1:
            s_phase_d = 0
        if not s_phase_e or s_phase_e == 1:
            s_phase_e = 0
        if not s_phase_f or s_phase_f == 1:
            s_phase_f = 0
        if not s_phase_g or s_phase_g == 1:
            s_phase_g = 0
        # DING.ALTER_END

        dev = abs(self.phase_a - s_phase_a) + abs(self.phase_b - s_phase_b) + abs(self.phase_c - s_phase_c) + abs(
            self.phase_d - s_phase_d) + abs(self.phase_e - s_phase_e) + abs(self.phase_f - s_phase_f) + abs(
            self.phase_g - s_phase_g)
        return dev

    def suitable2(self, cycle_starttime: dt.datetime, query_date: dt.datetime = None):
        bool_suitable = False
        if self.plantype == "FIXTED":
            if self.day_to_use is None:
                planstarttime = self.planstarttime
                planendtime = self.planendtime
                if planstarttime < planendtime:
                    if planstarttime <= cycle_starttime <= planendtime:
                        bool_suitable = True
                elif planstarttime > planstarttime:
                    if cycle_starttime <= planendtime or cycle_starttime >= planstarttime:
                        bool_suitable = True
            else:  # day_to_use is not None:
                day_to_use = self.day_to_use  # WORKDAY or WEEKEND
                weekday = query_date.weekday()
                planstarttime = self.planstarttime
                planendtime = self.planendtime
                if day_to_use == 'WORKDAY':
                    if 1 <= weekday <= 5:
                        if planstarttime < planendtime:
                            if planstarttime <= cycle_starttime <= planendtime:
                                bool_suitable = True
                        elif planstarttime > planstarttime:
                            if cycle_starttime <= planendtime or cycle_starttime >= planstarttime:
                                bool_suitable = True
                elif day_to_use == 'WEEKEND':
                    if 6 <= weekday <= 7:
                        if planstarttime < planendtime:
                            if planstarttime <= cycle_starttime <= planendtime:
                                bool_suitable = True
                        elif planstarttime > planstarttime:
                            if cycle_starttime <= planendtime or cycle_starttime >= planstarttime:
                                bool_suitable = True

        return bool_suitable

    def suitable(self, cycle_starttime: dt.datetime, query_date: dt.datetime = None):
        # bool_suitable = False
        plan_suitable = False
        date_suitable = False
        time_suitable = False
        if self.plantype == "FIXED":
            plan_suitable = True

        if self.day_to_use is None:
            date_suitable = True
        else:  # day_to_use is not None:
            # day_to_use = self.day_to_use  # WORKDAY or WEEKEND
            weekday = query_date.weekday()
            if self.day_to_use == 'WORKDAY':
                if 1 <= weekday <= 5:
                    date_suitable = True
            elif self.day_to_use == 'WEEKEND':
                if 6 <= weekday <= 7:
                    date_suitable = True

        if plan_suitable:
            if self.planstarttime <= cycle_starttime <= self.planendtime:
                time_suitable = True
            elif self.planstarttime > self.planendtime \
                    and (cycle_starttime <= self.planendtime or cycle_starttime >= self.planstarttime):
                time_suitable = True

        # bool_suitable = plan_suitable and date_suitable and time_suitable
        return plan_suitable and date_suitable and time_suitable


# 函数定义


def plan_objects_preparation(df_plans: pd.DataFrame):
    plan_series = df_plans['FSTR_PLANID']
    len_series_plans = len(plan_series)

    whole_plan_list = []
    fixed_plan_list = []
    adaptive_plan_list = []
    offline_plan_list = []

    for i in range(0, len_series_plans):
        intersectid = df_plans['FSTR_INTERSECTID'][i]
        plantype = df_plans['FSTR_PLANTYPE'][i]
        planid = df_plans['FSTR_PLANID'][i]
        planstarttime = df_plans['FSTR_PLANSTARTTIME'][i]
        planendtime = df_plans['FSTR_PLANENDTIME'][i]
        phase_a = df_plans['PHASE_A'][i]
        phase_b = df_plans['PHASE_B'][i]
        phase_c = df_plans['PHASE_C'][i]
        phase_d = df_plans['PHASE_D'][i]
        phase_e = df_plans['PHASE_E'][i]
        phase_f = df_plans['PHASE_F'][i]
        phase_g = df_plans['PHASE_G'][i]
        day_to_use = df_plans['FSTR_DAYTOUSE'][i]
        cur_splitplan = SPLITPLAN(intersectid, plantype, planid, planstarttime, planendtime, phase_a, phase_b, phase_c,
                                  phase_d, phase_e, phase_f, phase_g, day_to_use)

        if plantype == "FIXED":
            fixed_plan_list.append(cur_splitplan)
        elif plantype == "ADAPTIVE":
            adaptive_plan_list.append(cur_splitplan)
        else:
            offline_plan_list.append(cur_splitplan)

            whole_plan_list.append(cur_splitplan)
    # print(len(whole_plan_list))
    return whole_plan_list, fixed_plan_list, adaptive_plan_list, offline_plan_list


def plan_selection_analyzing(df_splitselection, fixed_plan_list, adaptive_plan_list, offline_plan_list=None):
    df_cycle_dev = pd.DataFrame(columns=('CycleStartTime', 'Dev', 'Label'))
    df_abnormal_cycle = pd.DataFrame(columns=('CycleStartTime', 'Dev', 'Label'))
    series_adaptive_plan_selection = pd.DataFrame(columns=('CycleStartTime', 'PlanID', 'IdxOfPlan'))
    fixed_plan_selection = pd.DataFrame(columns=('CycleStartTime', 'PlanID', 'IdxOfPlan'))

    len_series_selections = len(df_splitselection['FSTR_CYCLE_STARTTIME'])
    # print('len2 ' + str(len_series_selections))
    for i in range(0, len_series_selections):
        s_intersectid = df_splitselection['FSTR_INTERSECTID'][i]
        s_date = dt.datetime.strptime(df_splitselection['FSTR_DATE'][i], "%Y-%m-%d")
        s_cyclestarttime = dt.datetime.strptime(df_splitselection['FSTR_CYCLE_STARTTIME'][i], "%H:%M:%S")
        # print(s_cyclestarttime)
        s_cyclelength = df_splitselection['FINT_CYCLE_LENGTH'][i]
        s_phase_a = df_splitselection['PHASE_A'][i]
        s_phase_b = df_splitselection['PHASE_B'][i]
        s_phase_c = df_splitselection['PHASE_C'][i]
        s_phase_d = df_splitselection['PHASE_D'][i]
        s_phase_e = df_splitselection['PHASE_E'][i]
        s_phase_f = df_splitselection['PHASE_F'][i]
        s_phase_g = df_splitselection['PHASE_G'][i]
        cur_plan_type = "UNKNOWN"

        # 检查固定方案
        for j in range(len(fixed_plan_list)):
            if fixed_plan_list[j].suitable(s_cyclestarttime, s_date):  # suitable for this day and cycle starttime
                cur_plan_type = "FIXED"
                cur_fixed_plan = fixed_plan_list[j]
                cur_dev = cur_fixed_plan.deviation(s_phase_a, s_phase_b, s_phase_c, s_phase_d, s_phase_e, s_phase_f,
                                                   s_phase_g)
                cur_fixed_plan_id = cur_fixed_plan.planid
                cur_fied_plan_idx = j + 1

                # 固定方案匹配结果
                # 在偏差值结果集中增加一行
                out_label = "FIXED, DEVIATION= " + str(cur_dev)
                df_cycle_dev.loc[len(df_cycle_dev)] = [s_cyclestarttime, cur_dev, out_label]

                # 判断非正常或非正常结果集
                if cur_dev >= CONSTANTS.MAX_DEV:
                    out_label_abnormal = "FIXED BUT ANOMALY, DEVIATION= " + str(cur_dev)
                    df_abnormal_cycle.loc[len(df_abnormal_cycle)] = [s_cyclestarttime, cur_dev, out_label_abnormal]
                else:
                    fixed_plan_selection.loc[len(fixed_plan_selection)] = [s_cyclestarttime,
                                                                           cur_fixed_plan_id,
                                                                           cur_fied_plan_idx]
                break

        # 检查自适应方案
        if cur_plan_type == "FIXED":
            continue
        else:
            cur_min_dev = 999
            cur_mindev_adptplan = "unknown"
            cur_mindev_adptplan_idx = "unknown"
            # Check each adaptive plan to find the closest one
            for k in range(len(adaptive_plan_list)):
                # print("adaptive"+str(i) )
                cur_adaptive_plan = adaptive_plan_list[k]
                cur_dev = cur_adaptive_plan.deviation(s_phase_a, s_phase_b, s_phase_c, s_phase_d, s_phase_e, s_phase_f,
                                                      s_phase_g)
                cur_adptplan = cur_adaptive_plan.planid
                if cur_dev < cur_min_dev:  ##更优
                    cur_min_dev = cur_dev
                    cur_mindev_adptplan = cur_adptplan
                    cur_mindev_adptplan_idx = k + 1

            # 在结果集中增加一行
            out_label = cur_mindev_adptplan + " , DEVIATION= " + str(cur_min_dev)
            df_cycle_dev.loc[len(df_cycle_dev)] = [s_cyclestarttime, cur_min_dev, out_label]
            # 非正常结果集
            if cur_min_dev >= CONSTANTS.MAX_DEV:
                out_label_abnormal = "ADATPITVE BUT ANOMALY, DEVIATION= " + str(cur_min_dev)
                df_abnormal_cycle.loc[len(df_abnormal_cycle)] = [s_cyclestarttime, cur_min_dev, out_label_abnormal]
            else:
                # 方案选择结果集
                series_adaptive_plan_selection.loc[len(series_adaptive_plan_selection)] = [s_cyclestarttime,
                                                                                           cur_mindev_adptplan,
                                                                                           cur_mindev_adptplan_idx]

    return df_cycle_dev, df_abnormal_cycle, series_adaptive_plan_selection, fixed_plan_selection


def plot_plan_selection_analyze(query_date, query_intersection, df_cycle_dev, df_abnormal_cycle,
                                series_adaptive_plan_selection, fixed_plan_selection, adaptive_plan_list,
                                fixed_plan_list, df_splitselection, output_count):
    # title = "INT" + str(query_intersection) + "_" + query_date
    #
    # plt.rcParams['font.sans-serif'] = ['SimHei']  # 中文字体配置
    # plt.rcParams['axes.unicode_minus'] = False  # 坐标轴负号格式配置
    # matplotlib.rcParams['figure.figsize'] = (12, 12)
    # matplotlib.style.use('ggplot')
    #
    # fig = plt.figure(figsize=(10, 10))
    # ax1 = fig.add_subplot(311)
    #
    # ax1.plot(df_cycle_dev['CycleStartTime'], df_cycle_dev['Dev'], linewidth=1.5, color='steelblue')
    # ax1.plot(df_abnormal_cycle['CycleStartTime'], df_abnormal_cycle['Dev'], '.', 10, 'palevioletred')  # 画散点图
    #
    # ax1.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))  # 设置时间标签显示格式
    # datetime_0 = dt.datetime.strptime("00:00:00", "%H:%M:%S")
    # datatime_1day = dt.timedelta(days=1)
    # datetime_24 = datetime_0 + datatime_1day
    # plt.xlim([datetime_0, datetime_24])
    # plt.xticks(pd.date_range(datetime_0, datetime_24, freq='2H'), rotation=0)  # 时间间隔
    # plt.ylim([0, 80])
    # plt.legend(['相对偏差', '异常点'], loc='upper right', ncol=1)
    # plt.title(title)
    # plt.xlabel('时间')
    # plt.ylabel('绿信比相对偏差')
    # # plt.show()
    #
    # # 自适应方案选择频度
    # ax2 = fig.add_subplot(325)
    # plan_selection = series_adaptive_plan_selection['PlanID']
    # if len(plan_selection) > 0:
    #     plan_hist = series_adaptive_plan_selection['PlanID'].value_counts()  # 自适应方案被选次数 记录至csv文件或database
    #     plan_hist_sum = plan_hist.sum()
    #     plan_hist_prob = 100 * plan_hist / plan_hist_sum
    #     plan_hist_prob.plot(kind='bar', color='darksalmon')
    #     plt.title('总选择次数: ' + str(plan_hist_sum) + ' 次')
    # plt.xlabel('自适应方案')
    # plt.ylabel('被选次数比率 (%)')
    #
    # plt.xticks(rotation=0)
    #
    # # 周期时长
    # ax3 = fig.add_subplot(326)
    # ax3.plot(df_cycle_dev['CycleStartTime'], df_splitselection['FINT_CYCLE_LENGTH'], linewidth=1.5,
    #          color='darkcyan')  # 画散点图
    # ax3.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))  # 设置时间标签显示格式
    # plt.xlim([datetime_0, datetime_24])
    # plt.xticks(pd.date_range(datetime_0, datetime_24, freq='4H'), rotation=0)  # 时间间隔
    # plt.ylim([50, 200])
    # plt.title('总周期数: ' + str(len(df_cycle_dev['CycleStartTime'])) + ' 次')
    # plt.xlabel('时间')
    # plt.ylabel('周期时长')
    #
    # # 自适应和固定方案选择时间分布
    # ax4 = fig.add_subplot(312)
    adaptive_plan_list_length = len(adaptive_plan_list)
    #
    fixed_plan_list_length = len(fixed_plan_list)
    # ax4.plot(series_adaptive_plan_selection['CycleStartTime'],
    #          series_adaptive_plan_selection['IdxOfPlan'], '.',
    #          color='mediumslateblue', alpha=0.5, markersize=8)
    #
    # ax4.plot(fixed_plan_selection['CycleStartTime'],
    #          adaptive_plan_list_length + fixed_plan_selection['IdxOfPlan'], '.',
    #          color='cadetblue', alpha=0.5, markersize=8)
    #
    # ax4.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))  # 设置时间标签显示格式
    # plt.xlim([datetime_0, datetime_24])
    # plt.xticks(pd.date_range(datetime_0, datetime_24, freq='2H'), rotation=0)  # 时间间隔
    # yrange = adaptive_plan_list_length + fixed_plan_list_length + 1
    # ylabels = [''] * 1  # 先填充一个空字符
    # plt.ylim([0.5, adaptive_plan_list_length + fixed_plan_list_length + 1])

    ADP_list = []
    FIX_list = []
    for l in range(adaptive_plan_list_length):
        cur_adaptive_plan = adaptive_plan_list[l]
        cur_adptplanid = cur_adaptive_plan.planid
        ADP_list.append(cur_adptplanid)
        # ylabels.append(cur_adptplanid)
    for l in range(fixed_plan_list_length):
        cur_fixed_plan = fixed_plan_list[l]
        cur_fixedplanid = cur_fixed_plan.planid
        FIX_list.append(cur_fixedplanid)
        # ylabels.append(cur_fixedplanid)
    # print(FIX_list)
    # print(ADP_list)
    # plt.yticks(np.arange(yrange), ylabels)  # 从0到自适应方案的个数,0坐标空着不画，1坐标对应ADP1
    # plt.xlabel('时间')
    # plt.ylabel('被选自适应方案')
    # plt.subplots_adjust(left=0.16, wspace=0.35, hspace=0.38, bottom=0.08, top=0.93)

    # 保存各种次数 output_count
    output_count.append(str(len(df_cycle_dev)))  # 总周期数
    output_count.append(str(len(series_adaptive_plan_selection)))  # 自适应方案次数
    output_count.append(str(len(ADP_list)))
    n = 0

    # DINGCY_alter
    ADP_list.sort()

    hist_sort = series_adaptive_plan_selection['PlanID'].value_counts().sort_index()
    # print(adaptive_plan_list_length)
    for m in range(adaptive_plan_list_length):
        if m - n < len(hist_sort):
            if ADP_list[m] == hist_sort.index[m - n]:
                output_count.append(str(hist_sort.data[m - n]))  # 保存自适应方案被选次数
            else:
                output_count.append('0')
                n = n + 1
        else:
            output_count.append('0')

    if len(ADP_list) < CONSTANTS.MAX_ADP_NUM:
        for i in range(CONSTANTS.MAX_ADP_NUM - len(ADP_list)):
            output_count.append('NULL')
    output_count.append(str(len(fixed_plan_selection)))  # 固定方案次数
    output_count.append(str(len(FIX_list)))  # 固定方案套数

    fix_sort = fixed_plan_selection['PlanID'].value_counts().sort_index()
    # print(fix_sort)
    if len(fix_sort):
        for m in range(len(fix_sort)):
            output_count.append(str(fix_sort.data[m]))

    # output_count.
    output_count.append(
        str(len(df_cycle_dev) - len(fixed_plan_selection) - len(series_adaptive_plan_selection)))  # 未被选择次数
    # print(output_count)

    return output_count


def get_max_ADP_FIX_num(df_plans_all):
    a = df_plans_all['FSTR_PLANID'].value_counts()
    for i in range(len(a)):
        if a.index[i].find('ADP') == 0:
            CONSTANTS.MAX_ADP_NUM = CONSTANTS.MAX_ADP_NUM + 1
        elif a.index[i].find('FIX') == 0:
            CONSTANTS.MAX_FIX_NUM = CONSTANTS.MAX_FIX_NUM + 1
    return


def call_oracle(str_date, str_int, db, logger):
    match_records = pd.DataFrame({})
    # df_plans = pd.DataFrame({})
    df_splitselection_all = pd.DataFrame({})
    try:  # 数据库连接超时
        # db = cx_Oracle.connect(GlobalVar.OracleUser)
        cr = db.cursor()
        # try:  # 表名错误或日期错误
            # # 获取战略运行记录
            # sql1 = " select * from " + GlobalVar.StrRecordTableName + " where FSTR_INTERSECTID = '" + str_int + \
            #        "' and FSTR_DATE = '" + str_date + "' order by TO_NUMBER(FSTR_INTERSECTID), FSTR_CYCLE_STARTTIME "
            # cr.execute(sql1)
            # rs1 = cr.fetchall()
            # match_records = pd.DataFrame(rs1)
            # match_records.columns = ['FSTR_INTERSECTID', 'FINT_SA', 'FINT_DETECTORID', 'FSTR_CYCLE_STARTTIME',
            #                          'FSTR_PHASE_STARTTIME', 'FSTR_PHASE', 'FINT_PHASE_LENGTH', 'FINT_CYCLE_LENGTH',
            #                          'FINT_DS', 'FINT_ACTUALVOLUME', 'FSTR_DATE', 'FSTR_WEEKDAY', 'FSTR_CONFIGVERSION']
            # 获取某车道的静态方案
            # sql2 = " select * from " + GlobalVar.StatisticPlanTableName + " where FSTR_INTERSECTID = '" + \
            #        str_int + "'"
            # cr.execute(sql2)
            # rs2 = cr.fetchall()
            # df_plans = pd.DataFrame(rs2)
            # df_plans.columns = ['FSTR_INTERSECTID', 'FSTR_PLANTYPE', 'FSTR_PLANID', 'FSTR_PLANSTARTTIME',
            #                     'FSTR_PLANENDTIME', 'PHASE_A', 'PHASE_B', 'PHASE_C', 'PHASE_D', 'PHASE_E',
            #                     'PHASE_F', 'PHASE_G', 'FSTR_DAYTOUSE']
            # 获取绿信比运行记录
        sql3 = " select * from " + GlobalVar.PlanSelectionTableName + " where FSTR_DATE = '" + str_date + \
               "' and FSTR_INTERSECTID = '" + str_int + "' order by FSTR_CYCLE_STARTTIME "
        cr.execute(sql3)
        rs3 = cr.fetchall()
        df_splitselection_all = pd.DataFrame(rs3)
        df_splitselection_all.columns = ['FSTR_INTERSECTID', 'FSTR_DATE', 'FSTR_CYCLE_STARTTIME',
                                         'FINT_CYCLE_LENGTH', 'PHASE_A', 'PHASE_B', 'PHASE_C', 'PHASE_D',
                                         'PHASE_E', 'PHASE_F', 'PHASE_G']
        cr.close()
        # print(df_splitselection_all)
    #     except cx_Oracle.DatabaseError as e:
    #         print(GlobalVar.OracleError_TableName)
    #         # print(e)
    #     except ValueError as e:
    #         print(GlobalVar.OracleError_DateName)
    #         # print(e)
    # except cx_Oracle.DatabaseError as e:
    #     print(GlobalVar.OracleError_Connection)
    #     print(e)
    except Exception as e:
        logger.error('int' + str_int + str(e))
    return df_splitselection_all  # match_records,


def write_postgresql(str_int, str_date, data_type, json_all_dict, conn, logger):

    try:  # 数据库连接超时
        # 是否是测试模式
        # if if_test_mode:
        #     conn = psycopg2.connect(database=GlobalVar.PgUser_test_db, user=GlobalVar.PgUser_test_us,
        #                             password=GlobalVar.PgUser_test_pw, host=GlobalVar.PgUser_test_ht,
        #                             port=GlobalVar.PgUser_test_pt)
        #     # conn = psycopg2.connect(database="postgres", user="postgres", password="19861015dh", host="localhost", port="5432")
        #     # conn = GlobalVar.PostgresqlUser_test
        #     sql = "INSERT INTO " + GlobalVar.PostgresqlTableName_test + "(data_type, intersect_id, item_date, json_data) " \
        #                                                                 "VALUES(%s, %s, %s, %s) "
        #     sql_delete = "delete from " + GlobalVar.PostgresqlTableName_test + " where item_date = '" + str_date + \
        #                  "' and intersect_id = '" + str_int + "' and data_type = '" + data_type + "'"
        # else:
        #     conn = psycopg2.connect(database=GlobalVar.PgUser_db, user=GlobalVar.PgUser_us,
        #                             password=GlobalVar.PgUser_pw, host=GlobalVar.PgUser_ht,
        #                             port=GlobalVar.PgUser_pt)
            # conn = psycopg2.connect(database="superpower", user="postgres", password="postgres", host="192.168.20.62", port="5432")
            # conn = GlobalVar.PostgresqlUser
        sql = "INSERT INTO " + GlobalVar.PostgresqlTableName + " (data_type, intersect_id, item_date, json_data) " \
                                                               "VALUES(%s, %s, %s, %s) "
        sql_delete = "delete from " + GlobalVar.PostgresqlTableName + " where item_date = '" + str_date + \
                     "' and intersect_id = '" + str_int + "' and data_type = '" + data_type + "'"

        cur = conn.cursor()
        data = (data_type, str_int, str_date, json_all_dict, )
        try:
            cur.execute(sql, data)
            # print(GlobalVar.Postgresql_Insert)
        except psycopg2.IntegrityError:
            conn.commit()
            cur.execute(sql_delete)
            cur.execute(sql, data)
            # print(GlobalVar.Postgresql_Delete)
        conn.commit()
        cur.close()
    # except psycopg2.OperationalError as e:
    #     print(GlobalVar.PostgresqlError_Connection)
    #     print(e)
    except Exception as e:
        logger.error('int' + str_int + str(e))
    return


def avg_volume_per_cycle(match_records):

    grouped_data = match_records.groupby(['FSTR_CYCLE_STARTTIME'])
    vo_series_new = []
    time = []
    for one_cycle_group in grouped_data.groups:
        one_cycle_data = grouped_data.get_group(one_cycle_group)
        vo_series = pd.DataFrame({'Volume': one_cycle_data['FINT_ACTUALVOLUME']})
        time.append(one_cycle_group)
        des = vo_series.describe(percentiles=[.80])
        vo = math.ceil(des['Volume']['80%'])
        vo_series_new.append(vo)
    # for m in range(len(vo_series_new)):
    #     if vo_series_new
    # print(vo_series_new)
    # print(len(vo_series_new))
    return pd.DataFrame({'Volume': vo_series_new, 'CycleStartTime': time})


# 所有周期选择结果dataframe
def all_cycle_select_df(match_records, df_splitselection, series_adaptive_plan_selection, fixed_plan_selection):

    ADP = []
    FIX = []
    TMP = []
    ADP_new = []
    FIX_new = []
    TMP_new = []
    ADP_p = []
    FIX_p = []
    TMP_p = []
    # temp_p = ''
    concat_df = pd.DataFrame({'Intersect': df_splitselection['FSTR_INTERSECTID'],
                              'Date': df_splitselection['FSTR_DATE'],
                              'CycleStartTime': df_splitselection['FSTR_CYCLE_STARTTIME'],
                              'CycleLength': df_splitselection['FINT_CYCLE_LENGTH']})
    # print(df_splitselection)
    adp_time_series = []
    adp_series = []
    fix_series = []
    fix_time_series = []
    for m in range(len(series_adaptive_plan_selection)):
        temp = dt.datetime.strftime(series_adaptive_plan_selection['CycleStartTime'][m], "%H:%M:%S")
        adp_time_series.append(temp)
        adp_series.append(series_adaptive_plan_selection['PlanID'][m])
    for m in range(len(fixed_plan_selection)):
        temp = dt.datetime.strftime(fixed_plan_selection['CycleStartTime'][m], "%H:%M:%S")
        fix_time_series.append(temp)
        fix_series.append(fixed_plan_selection['PlanID'][m])

    adp_time_series = pd.Series(adp_time_series)
    adp_series = pd.Series(adp_series)
    adp_df = pd.DataFrame({'CycleStartTime': adp_time_series, 'PlanID': adp_series})
    fix_time_series = pd.Series(fix_time_series)
    fix_series = pd.Series(fix_series)
    fix_df = pd.DataFrame({'CycleStartTime': fix_time_series, 'PlanID': fix_series})
    concat_df = pd.merge(concat_df, adp_df, how='left', on='CycleStartTime')
    concat_df = pd.merge(concat_df, fix_df, how='left', on='CycleStartTime')
    vo_df = avg_volume_per_cycle(match_records)
    concat_df = pd.merge(concat_df, vo_df, how='left', on='CycleStartTime')
    for m in range(len(concat_df)):
        if str(concat_df['Volume'][m]) == 'nan':
            concat_df['Volume'][m] = 0
    # print(len(concat_df))
    # print(concat_df['Volume'].tolist())

    if concat_df['PlanID_x'][0]:
        temp_p = str(concat_df['PlanID_x'][0])[0]
    elif concat_df['PlanID_y'][0]:
        temp_p = str(concat_df['PlanID_y'][0])[0]
    else:
        temp_p = 'T'
    # print(concat_df)
    for j in range(len(concat_df)):
        if str(concat_df['PlanID_x'][j])[0] == 'A':
            if temp_p == 'A':
                ADP_new.append(str(concat_df['Volume'][j]))
                FIX_new.append('-')
                TMP_new.append('-')
            else:
                temp_p = 'A'
                if ADP_new:
                    ADP_new.pop()
                    ADP_new.append(str(concat_df['Volume'][j-1]))
                ADP_new.append(str(concat_df['Volume'][j]))
                FIX_new.append('-')
                TMP_new.append('-')
            ADP.append(str(concat_df['PlanID_x'][j])[-1])
            FIX.append('-')
            TMP.append('-')
            ADP_p.append(str(concat_df['Volume'][j]))
            FIX_p.append('-')
            TMP_p.append('-')

        elif str(concat_df['PlanID_y'][j])[0] == 'F':
            if temp_p == 'F':
                ADP_new.append('-')
                FIX_new.append(str(concat_df['Volume'][j]))
                TMP_new.append('-')
            else:
                temp_p = 'F'
                ADP_new.append('-')
                if FIX_new:
                    FIX_new.pop()
                    FIX_new.append(str(concat_df['Volume'][j-1]))
                FIX_new.append(str(concat_df['Volume'][j]))
                TMP_new.append('-')
            ADP.append('-')
            FIX.append(str(int(str(concat_df['PlanID_y'][j])[-1]) + CONSTANTS.MAX_ADP_NUM))
            TMP.append('-')
            ADP_p.append('-')
            FIX_p.append(str(concat_df['Volume'][j]))
            TMP_p.append('-')

        else:
            if temp_p == 'T':
                ADP_new.append('-')
                FIX_new.append('-')
                TMP_new.append(str(concat_df['Volume'][j]))
            else:
                temp_p = 'T'
                ADP_new.append('-')
                FIX_new.append('-')
                if TMP_new:
                    TMP_new.pop()
                    TMP_new.append(str(concat_df['Volume'][j-1]))
                TMP_new.append(str(concat_df['Volume'][j]))
            ADP.append('-')
            FIX.append('-')
            TMP.append('0')
            ADP_p.append('-')
            FIX_p.append('-')
            TMP_p.append(str(concat_df['Volume'][j]))
    # print(FIX)

    return ADP_new, FIX_new, TMP_new, ADP, FIX, TMP, ADP_p, FIX_p, TMP_p, concat_df


def concat_plan_str(df_splitselection, index):
    # print(df_splitselection)
    # print(index)
    plan_string = str(df_splitselection['PHASE_A'][index]) + ',' + str(df_splitselection['PHASE_B'][index]) + ',' + \
               str(df_splitselection['PHASE_C'][index]) + ',' + str(df_splitselection['PHASE_D'][index]) + ',' + \
               str(df_splitselection['PHASE_E'][index]) + ',' + str(df_splitselection['PHASE_F'][index]) + ',' + \
               str(df_splitselection['PHASE_G'][index])
    return plan_string


def adp_state(str_date, str_int, concat_df, db, logger, length):
    if len(concat_df) != 0:
        adp_num = 0
        # print(concat_df)
        for m in range(len(concat_df) - 1):

            if str(concat_df.iloc[m]['PlanID_x']) != str(concat_df.iloc[m + 1]['PlanID_x']):
                # print('concat_df.iloc[m]', concat_df.iloc[m]['PlanID_x'])
                # print('concat_df.iloc[m+1]', concat_df.iloc[m + 1]['PlanID_x'])
                adp_num += 1
        adp_rate = round(adp_num / len(concat_df) * 100, 2)
        # print(adp_num)
        # print(len(concat_df))
        print(str_date, str_int, adp_rate, adp_num, len(concat_df))
        data = [str_date, str_int, adp_rate, adp_num, len(concat_df)]
    else:
        data = [str_date, str_int, 0, 0, length]
        # print(data)
    try:  # 数据库连接超时
        # db = cx_Oracle.connect(GlobalVar.OracleUser)
        cr = db.cursor()
        sql_write = "insert into HZ_SCATS_PLAN_ADP_RATE " \
                    "(FSTR_DATE, FSTR_INTERSECTID, ADP_RATE, ADP_NUM, CYC_NUM) " \
                    "values(:1, :2, :3, :4, :5)"
        try:
            # cr.prepare(sql_write)
            cr.execute(sql_write, data)
            db.commit()
            # print(GlobalVar.Oracle_Insert)
        except cx_Oracle.IntegrityError:
            db.commit()
            cr1 = db.cursor()
            sql_delete = "delete from HZ_SCATS_PLAN_ADP_RATE where FSTR_DATE = '" + str_date + \
                         "' and FSTR_INTERSECTID = '" + str_int + "'"
            cr1.execute(sql_delete)
            cr.execute(sql_write, data)
            db.commit()
        cr.close()
    except Exception as e:
        logger.error('int' + str_int + str(e))


def plan_selection_problem(str_date, str_int, concat_df, output_df, df_abnormal_cycle, df_splitselection, df_plans,
                           plan_select_state, hour_or_num, unit, num_in_detail, db, logger):
    print(df_plans)
    # print(df_abnormal_cycle)
    plan_rate = []
    tmp_rate = 0
    # 计算每个方案被选绿
    if len(output_df):
        tmp_rate = round(int(output_df[-1]) / int(output_df[0]) * 100, 2)
        # print(tmp_rate)
        for m in range(int(output_df[2])):
            temp_rate = round(int(output_df[2 + m + 1]) / int(output_df[0]) * 100, 2)
            plan_rate.append(temp_rate)
    else:
        for i in range(CONSTANTS.MAX_ADP_NUM):
            plan_rate.append(0)
    # print(plan_rate)
    # print('这里')
    # 开始判断绿信比方案问题
    # print(tmp_rate)
    if tmp_rate > CONSTANTS.tmp_rate:  # 选临时方案大于30%
        # print('S002')
        plan_select_state = PlanSelectionStatus.tmp_rate_high
        hour_or_num = math.ceil(int(output_df[-1]) * 3 / 60)
        unit = GlobalVar.unit_hour
        num_in_detail = ''

        # 判断是否锁定临时方案
        cycle_num = 0
        lock_flag = 0
        plan_str = ''
        # print(concat_df)
        # concat_df.drop_duplicates()
        # print(concat_df)
        concat_df = concat_df.drop_duplicates().reset_index(drop=True)
        # print(concat_df)
        # df_splitselection = df_splitselection.drop_duplicates().reset_index(drop=True)
        # print(df_splitselection)
        # input()
        for m in range(len(concat_df)):  # 遍历
            if str(concat_df.iloc[m]['PlanID_x'])[0] != 'A' and str(concat_df.iloc[m]['PlanID_y'])[0] != 'F':
                # print(concat_df.iloc[m].tolist())
                if cycle_num == 0:
                    cycle_num += 1
                    # print(111)
                    plan_str = concat_plan_str(df_splitselection, m)
                    # print(plan_str)
                else:
                    t1 = dt.datetime.strptime(concat_df.iloc[m - 1]['CycleStartTime'], "%H:%M:%S")
                    t2 = dt.datetime.strptime(concat_df.iloc[m]['CycleStartTime'], "%H:%M:%S")
                    if (t2 - t1).seconds < CONSTANTS.time_delta:  # 连续两周期
                        # print(222)
                        plan_str_new = concat_plan_str(df_splitselection, m)
                        if plan_str == plan_str_new:  # 与上一个临时方案相同
                            cycle_num += 1
                            if cycle_num > CONSTANTS.lock_num:
                                lock_flag = 1
                                break
                        else:  # 与上一个临时方案不同
                            # print(333)
                            plan_str = concat_plan_str(df_splitselection, m)
                            cycle_num = 0
                    else:  # 不是连续两周期
                        # print(444)
                        plan_str = concat_plan_str(df_splitselection, m)
                        cycle_num = 0
            else:
                cycle_num = 0
        if lock_flag:  # 已判断为锁定
            # print('S006')
            plan_select_state = PlanSelectionStatus.tmp_plan_lock
            hour_or_num = math.ceil(int(cycle_num) * 3 / 60)
            unit = GlobalVar.unit_hour
            num_in_detail = ''

    elif plan_rate.count(0) > 0 and tmp_rate:  # 某几个方案没被选过
        # print('S003')
        plan_select_state = PlanSelectionStatus.no_select_plan
        hour_or_num = plan_rate.count(0)
        unit = GlobalVar.unit_num
        num_in_detail = plan_rate.count(0)

    elif len([i for i in plan_rate if 0 < i < CONSTANTS.min_plan_rate]):  # 某个方案被选小于10%
        # print('S005')
        plan_select_state = PlanSelectionStatus.plan_irrational
        index = plan_rate.index(min([i for i in plan_rate if 0 < i < CONSTANTS.min_plan_rate]))
        hour_or_num = math.ceil(int(output_df[index + 2 + 1]) * 3 / 60)
        unit = GlobalVar.unit_hour
        num_in_detail = index + 1
    elif tmp_rate == 0:
        # print('S001')
        plan_select_state = PlanSelectionStatus.no_select_all_plan
        # index = plan_rate.index(min([i for i in plan_rate if 0 < i < CONSTANTS.min_plan_rate]))
        hour_or_num = 0
        unit = GlobalVar.unit_num
        # num_in_detail = index + 1

    # 固定方案不合理(很多异常点)
    fix_list = []
    temp_fix = []
    ab_num = 0
    fix_plan_unfit_flag = 0
    fix_cycle_num = 0
    for m in range(len(df_plans)):  # 获取固定方案时间段
        if str(df_plans.iloc[m]['FSTR_PLANTYPE'])[0] == 'F':
            temp_fix.append(df_plans.iloc[m]['FSTR_PLANSTARTTIME'])
            temp_fix.append(df_plans.iloc[m]['FSTR_PLANENDTIME'])
            fix_list.append(temp_fix)
            temp_fix = []
    for m in range(len(fix_list)):  # 遍历固定list
        s = dt.datetime.strptime(fix_list[m][0], "%H:%M:%S")
        e = dt.datetime.strptime(fix_list[m][1], "%H:%M:%S")
        delta = (e - s).seconds
        fix_cycle_num = math.ceil(int(delta) / 3 / 60)
        for n in range(len(df_abnormal_cycle)):
            ab_time = dt.datetime.strftime(df_abnormal_cycle.iloc[n]['CycleStartTime'], "%H:%M:%S")
            if fix_list[m][0] <= ab_time <= fix_list[m][1]:
                ab_num += 1
        if ab_num:
            if ab_num / fix_cycle_num * 100 > CONSTANTS.fix_plan_unfit_per:
                fix_plan_unfit_flag = 1
                break
        ab_num = 0
    if fix_plan_unfit_flag and tmp_rate:
        # print('S008')
        plan_select_state = PlanSelectionStatus.fix_plan_unfit
        hour_or_num = round(ab_num / fix_cycle_num * 100, 2)
        unit = GlobalVar.unit_percent
        num_in_detail = ''
    # 固定方案不合理(很多异常点)

    # 建议高峰期设置固定方案
    morning_peak_start = CONSTANTS.morning_peak_start
    morning_peak_end = CONSTANTS.morning_peak_end
    afternoon_peak_start = CONSTANTS.afternoon_peak_start
    afternoon_peak_end = CONSTANTS.afternoon_peak_end
    mor_ab_num = 0
    aft_ab_num = 0
    peak_need_fix = 0
    for m in range(len(df_abnormal_cycle)):
        temp_time = dt.datetime.strftime(df_abnormal_cycle.iloc[m]['CycleStartTime'], "%H:%M:%S")
        if morning_peak_start <= temp_time <= morning_peak_end:
            mor_ab_num += 1
        elif afternoon_peak_start <= temp_time <= afternoon_peak_end:
            aft_ab_num += 1
    if mor_ab_num / CONSTANTS.peak_num * 100 >= CONSTANTS.peak_ab_per or aft_ab_num / CONSTANTS.peak_num * 100 >= CONSTANTS.peak_ab_per:
        peak_need_fix = 1
    if peak_need_fix and tmp_rate:
        # print('S009')
        plan_select_state = PlanSelectionStatus.peak_no_fix
        hour_or_num = math.floor((mor_ab_num + aft_ab_num) * 3 / 60)
        unit = GlobalVar.unit_hour
        num_in_detail = ''
    # 建议高峰期设置固定方案

    # 某个方案老是选
    if max(plan_rate) > CONSTANTS.max_plan_rate:
        # print('S004')
        plan_select_state = PlanSelectionStatus.kept_one_plan
        index = plan_rate.index(max(plan_rate))
        hour_or_num = math.ceil(int(output_df[index + 2 + 1]) * 3 / 60)
        unit = GlobalVar.unit_hour
        num_in_detail = index + 1

        # 锁定某方案
        if max(plan_rate) == 100:
            # print('S007')
            plan_select_state = PlanSelectionStatus.one_plan_lock
            hour_or_num = 24
            unit = GlobalVar.unit_hour
            index = plan_rate.index(max(plan_rate))
            num_in_detail = index + 1
    # 某个方案老是选
    result = [str_date, str_int, 'Split', plan_select_state.pro_code, plan_select_state.pro_name, plan_select_state.pro_detail,
              str(hour_or_num), str(num_in_detail), str(unit), plan_select_state.pro_title, plan_select_state.pro_reference]
    # print(result)

    test_type = 'Split'
    try:  # 数据库连接超时
        # db = cx_Oracle.connect(GlobalVar.OracleUser)
        cr = db.cursor()
        sql_write = "insert into " + GlobalVar.IssueIntTest + \
                    "(FSTR_DATE, FSTR_INTERSECTID, TEST_TYPE， STATE_CODE, STATE_NAME, STATE_DETAIL, SHOW_HOURORNUM, " \
                    "SHOW_NUMINDETAIL, SHOW_UNIT, SHOW_TITLE, SHOW_REFERENCE) " \
                    "values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)"
        try:
            # cr.prepare(sql_write)
            cr.execute(sql_write, result)
            db.commit()
            # print(GlobalVar.Oracle_Insert)
        except cx_Oracle.IntegrityError:
            cr1 = db.cursor()
            sql_delete = "delete from " + GlobalVar.IssueIntTest + " where FSTR_DATE = '" + str_date + \
                         "' and FSTR_INTERSECTID = '" + str_int + "' and TEST_TYPE = '" + test_type + "'"
            cr1.execute(sql_delete)
            # print(GlobalVar.Oracle_Delete)
            # cr.prepare(sql_write)
            cr.execute(sql_write, result)
            db.commit()
        cr.close()
    # except cx_Oracle.DatabaseError as e:
    #     print(GlobalVar.OracleError_Connection)
    #     print(e)
    except Exception as e:
        logger.error('int' + str_int + str(e))

    return plan_select_state


# def plot(str_date, str_int, ADP_p, FIX_p, TMP_p, df_cycle_dev, series_adaptive_plan_selection,
#          fixed_plan_selection, adaptive_plan_list, fixed_plan_list, df_abnormal_cycle, plan_select_state):
#
#     title = 'INT_' + str_int + '_' + str_date + 'Split'
#     state = plan_select_state.pro_name
#     matplotlib.rcParams['figure.figsize'] = (12, 12)
#     matplotlib.style.use('ggplot')
#     plt.rcParams['font.sans-serif'] = ['SimHei']  # 中文字体配置
#     plt.rcParams['axes.unicode_minus'] = False  # 坐标轴负号格式配置
#
#     ADP_t, FIX_t, TMP_t = ADP_p, FIX_p, TMP_p
#     tmp_time = []
#     tmp_plt = []
#     for m in range(len(TMP_t)):
#         if TMP_t[m] != '-':
#             tmp_time.append(df_cycle_dev['CycleStartTime'][m])
#             tmp_plt.append(TMP_t[m])
#     while '-' in ADP_t:
#         ADP_t.remove('-')
#     while '-' in FIX_t:
#         FIX_t.remove('-')
#     while '-' in TMP_p:
#         TMP_p.remove('-')
#
#     fig = plt.figure(figsize=(10, 10))
#
#     # 图1 方案-流量
#     ax1 = fig.add_subplot(311)
#     ax1.plot(series_adaptive_plan_selection['CycleStartTime'], ADP_p, '.', 10, 'steelblue')
#     ax1.plot(fixed_plan_selection['CycleStartTime'], FIX_p, '.', 10, 'palevioletred')  # 画散点图
#     ax1.plot(tmp_time, tmp_plt, '.', 10, 'slategray')  # 画散点图
#     ax1.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))  # 设置时间标签显示格式
#     datetime_0 = dt.datetime.strptime("00:00:00", "%H:%M:%S")
#     datatime_1day = dt.timedelta(days=1)
#     datetime_24 = datetime_0 + datatime_1day
#     plt.xlim([datetime_0, datetime_24])
#     plt.xticks(pd.date_range(datetime_0, datetime_24, freq='2H'), rotation=0)  # 时间间隔
#     plt.title(title + '_' + state)
#     # plt.legend(['ADP', 'FIX', 'TMP'], loc='upper right', ncol=1)
#
#     # 图2 自适应和固定方案选择时间分布
#     ax4 = fig.add_subplot(312)
#     adaptive_plan_list_length = len(adaptive_plan_list)
#     fixed_plan_list_length = len(fixed_plan_list)
#     ax4.plot(series_adaptive_plan_selection['CycleStartTime'],
#              series_adaptive_plan_selection['IdxOfPlan'], '.',
#              color='mediumslateblue', alpha=0.5, markersize=8)
#     ax4.plot(fixed_plan_selection['CycleStartTime'],
#              adaptive_plan_list_length + fixed_plan_selection['IdxOfPlan'], '.',
#              color='cadetblue', alpha=0.5, markersize=8)
#     ax4.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))  # 设置时间标签显示格式
#     plt.xlim([datetime_0, datetime_24])
#     plt.xticks(pd.date_range(datetime_0, datetime_24, freq='2H'), rotation=0)  # 时间间隔
#     yrange = adaptive_plan_list_length + fixed_plan_list_length + 1
#     ylabels = [''] * 1  # 先填充一个空字符
#     # ADP_list = []
#     # FIX_list = []
#     for l in range(adaptive_plan_list_length):
#         cur_adaptive_plan = adaptive_plan_list[l]
#         cur_adptplanid = cur_adaptive_plan.planid
#         # ADP_list.append(cur_adptplanid)
#         ylabels.append(cur_adptplanid)
#     for l in range(fixed_plan_list_length):
#         cur_fixed_plan = fixed_plan_list[l]
#         cur_fixedplanid = cur_fixed_plan.planid
#         # FIX_list.append(cur_fixedplanid)
#         ylabels.append(cur_fixedplanid)
#     plt.ylim([0.5, adaptive_plan_list_length + fixed_plan_list_length + 1])
#     plt.yticks(np.arange(yrange), ylabels)  # 从0到自适应方案的个数,0坐标空着不画，1坐标对应ADP1
#     plt.xlabel('时间')
#     plt.ylabel('被选自适应方案')
#     plt.subplots_adjust(left=0.16, wspace=0.35, hspace=0.38, bottom=0.08, top=0.93)
#
#     # 图3 绿信比相对偏差
#     ax2 = fig.add_subplot(313)
#
#     ax2.plot(df_cycle_dev['CycleStartTime'], df_cycle_dev['Dev'], linewidth=1.5, color='steelblue')
#     ax2.plot(df_abnormal_cycle['CycleStartTime'], df_abnormal_cycle['Dev'], '.', 10, 'palevioletred')  # 画散点图
#
#     ax2.xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))  # 设置时间标签显示格式
#     datetime_0 = dt.datetime.strptime("00:00:00", "%H:%M:%S")
#     datatime_1day = dt.timedelta(days=1)
#     datetime_24 = datetime_0 + datatime_1day
#     plt.xlim([datetime_0, datetime_24])
#     plt.xticks(pd.date_range(datetime_0, datetime_24, freq='2H'), rotation=0)  # 时间间隔
#     plt.ylim([0, 80])
#     plt.legend(['相对偏差', '异常点'], loc='upper right', ncol=1)
#     plt.xlabel('时间')
#     plt.ylabel('绿信比相对偏差')
#
#     cur_path_string = os.path.abspath('.')
#     path = 'E:\PycharmProjects'
#     foldername = 'Fig'
#     # 判断输出文件夹是否存在
#     if not os.path.exists(path + "/Fig"):  # Figs文件夹不存在
#         os.makedirs(path + "/Fig")
#     plt.savefig(path + '/' + foldername + '/{}.jpg'.format(title.replace(r'/', ' ')))
#     # plt.show()
#     plt.close(fig)
#     return


def call_postgre(str_nodeid, conn, logger):
    match_plan_records = pd.DataFrame({})
    # PLAN_TABLENAME = 'split_plans_overviews'
    try:
        # conn = psycopg2.connect(database=GlobalVar.PgUser_getdata_db, user=GlobalVar.PgUser_getdata_us,  # 72
        #                         password=GlobalVar.PgUser_getdata_pw,
        #                         host=GlobalVar.PgUser_getdata_ht, port=GlobalVar.PgUser_getdata_pt)
        cr = conn.cursor()
        sql = "select m.node_id, m.split_id, m.phase_name, m.split, m.split_type, m.starttime, m.endtime from " + \
              GlobalVar.PostgresqlSplitPlan + " m where m.node_id = '" + str_nodeid + \
              "' order by split_type, split_id, phase_name"  # 试点
        # print(sql)
        cr.execute(sql)
        rs1 = cr.fetchall()
        match_plan_records = pd.DataFrame(rs1)
        match_plan_records.columns = ['node_id', 'split_id', 'phase_name', 'split', 'split_type', 'starttime', 'endtime']  # 试点
        # print(match_plan_records)
        # conn.close()
        cr.close()
    # except psycopg2.ProgrammingError as e:
    #     print(GlobalVar.PostgreError_TableName)
    #     print(e)
    # except ValueError as e:
    #     print(GlobalVar.PostgreError_DateName)
    #     print(e)
    # except psycopg2.OperationalError as e:  # 数据库连接超时
    #     print(GlobalVar.PostgresqlError_Connection)
    #     print(e)
    except Exception as e:
        logger.error(e)
    return match_plan_records


def get_split_plan_from_pg(str_int, str_nodeid, conn, logger):
    match_plan_records = call_postgre(str_nodeid, conn, logger)
    # print(match_plan_records)
    phase_standard = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    all_item = []
    if not match_plan_records.empty:
        match_plan_records['split_type'] = match_plan_records['split_type'].fillna(0)
        # print(match_plan_records)
        group_split_type = match_plan_records.groupby(['split_type'])
        for split_type in group_split_type.groups:
            # print(split_type)
            one_type_data = group_split_type.get_group(split_type)
            # print(one_type_data)
            # print(str(split_type))
            if str(split_type) == '1' or str(split_type) == '0':  # 自适应
                # print(one_type_data)
                group_plan = one_type_data.groupby(['split_id'])
                plan_nums = 0

                for plans in group_plan.groups:
                    temp_item = []
                    plan_nums += 1
                    temp_item.append(str_int)
                    temp_item.append('ADAPTIVE')
                    temp_item.append('ADP' + str(plan_nums))
                    temp_item.append('')
                    temp_item.append('')
                    one_plan = group_plan.get_group(plans)
                    for n in range(len(phase_standard)):

                        match_flag = 0
                        for m in range(len(one_plan)):
                            if str(one_plan.iloc[m]['phase_name']) == phase_standard[n]:
                                match_flag = 1
                                break
                        if match_flag:
                            if np.isnan(one_plan.iloc[m]['split']) or int(one_plan.iloc[m]['split']) == 0:
                                temp_item.append(0)
                            elif one_plan.iloc[m]['split']:

                                temp_item.append(int(one_plan.iloc[m]['split']))
                            # print(phase_standard[n])
                        else:
                            temp_item.append(0)
                            # print(phase_standard[n])
                            # print('null')
                    temp_item.append('')
                    all_item.append(temp_item)
            elif str(split_type) == '2':  # 固定
                # print(one_type_data)
                group_plan = one_type_data.groupby(['split_id'])
                plan_nums = 0

                for plans in group_plan.groups:
                    temp_item = []
                    plan_nums += 1
                    temp_item.append(str_int)
                    temp_item.append('FIXED')
                    temp_item.append('FIX' + str(plan_nums))
                    # temp_item.append(one_plan.iloc[plans]['starttime'])
                    # temp_item.append(one_plan.iloc[plans]['endtime'])
                    one_plan = group_plan.get_group(plans)
                    # print(one_plan)
                    temp_item.append(one_plan.iloc[0]['starttime'])
                    temp_item.append(one_plan.iloc[0]['endtime'])
                    for n in range(len(phase_standard)):
                        match_flag = 0
                        for m in range(len(one_plan)):
                            if str(one_plan.iloc[m]['phase_name']) == phase_standard[n]:
                                match_flag = 1
                                break
                        if match_flag:
                            if np.isnan(one_plan.iloc[m]['split']) or int(one_plan.iloc[m]['split']) == 0:
                                temp_item.append(0)
                            elif one_plan.iloc[m]['split']:
                                temp_item.append(int(one_plan.iloc[m]['split']))
                            # print(phase_standard[n])
                        else:
                            temp_item.append(0)
                            # print(phase_standard[n])
                            # print('null')
                    temp_item.append('')
                    all_item.append(temp_item)

    new_plan_df = pd.DataFrame(all_item, columns=['FSTR_INTERSECTID', 'FSTR_PLANTYPE', 'FSTR_PLANID', 'FSTR_PLANSTARTTIME', 'FSTR_PLANENDTIME', 'PHASE_A', 'PHASE_B', 'PHASE_C', 'PHASE_D', 'PHASE_E', 'PHASE_F', 'PHASE_G', 'FSTR_DAYTOUSE'])
    # print(new_plan_df)
    return new_plan_df


def main_PlanSelection(match_records, str_date, str_int, str_nodeid, str_intname, db, conn, logger):
    plan=None
    # df_plans, df_splitselection = db_split_selection(str_date, str_int, db)  # 获取所需时间段内所有数据
    df_splitselection = call_oracle(str_date, str_int, db, logger)
    new_df_plans = get_split_plan_from_pg(str_int, str_nodeid, conn, logger)
    # print(new_df_plans)

    output_count = []  # 保存某个路口的方案选择情况

    IntInformation = {"Date": str_date, "IntCode": str_int, "IntName": str_intname}
    ADP = []
    FIX = []
    TMP = []
    ADP_new = []
    FIX_new = []
    TMP_new = []
    ADP_p = []
    FIX_p = []
    TMP_p = []
    concat_df = pd.DataFrame({})
    CycleTime = []
    CycleLength = []
    PlanCode = []
    PlanRate = []
    PlanPie_outside = []
    PlanPie_inside = []
    offset = []
    ab_offset = []
    output_df = []

    plan_select_state = PlanSelectionStatus.no_selection_pro
    hour_or_num = 0
    num_in_detail = ''
    unit = ''


    if not df_splitselection.empty and not match_records.empty:
        if not new_df_plans.empty:  # 排除没有静态方案的路口
            get_max_ADP_FIX_num(new_df_plans)  # 获取静态方案中ADP与FIX的个数
            # 各种类型静态方案拆分
            whole_plan_list, fixed_plan_list, adaptive_plan_list, offline_plan_list = plan_objects_preparation(
                new_df_plans)
            # 方案选择数据分析
            df_cycle_dev, df_abnormal_cycle, series_adaptive_plan_selection, fixed_plan_selection = \
                plan_selection_analyzing(df_splitselection, fixed_plan_list, adaptive_plan_list)
            print(series_adaptive_plan_selection,'1')
            print(fixed_plan_selection,'2')
            print(df_abnormal_cycle,'3')#实际运行的方案与预设的方案偏差来确定是哪套方案
            print(df_cycle_dev,'4')
            nowTime=dt.datetime.now().strftime('%H:%M')
            print(nowTime)
            a=series_adaptive_plan_selection[(series_adaptive_plan_selection.CycleStartTime==3).isin([nowTime])].index.tolist()
            if a!=[]:
                plan_number='adaptive'
                plan=plan_number
                return plan
            else:
                a = fixed_plan_selection[(fixed_plan_selection.CycleStartTime == 3).isin([nowTime])].index.tolist()
                if a!=[]:
                    plan_number='fixed'
                else:
                    plan_number='other'
            #input()
                plan=plan_number
                return plan



            # print(df_cycle_dev)
            for m in range(len(df_cycle_dev)):
                CycleTime.append(dt.datetime.strftime(df_cycle_dev['CycleStartTime'][m], "%H:%M:%S")[:-3])
            if len(series_adaptive_plan_selection) == 0:  # 没有选中任何自适应方案
                plan_select_state = PlanSelectionStatus.no_select_all_plan  # 方案选择存在问题：没有选中任何自适应方案
                hour_or_num = 0
                unit = GlobalVar.unit_num
                num_in_detail = ''
            if len(series_adaptive_plan_selection) + len(fixed_plan_selection) > 0:  # 自适应与固定 至少一个选中
                # 画图以及选择情况统计
                output_df = plot_plan_selection_analyze(str_date, str_int, df_cycle_dev, df_abnormal_cycle,
                                                        series_adaptive_plan_selection, fixed_plan_selection,
                                                        adaptive_plan_list, fixed_plan_list, df_splitselection,
                                                        output_count)

                # 获取画图所需的各种ADP,FIX,TMP
                # print(series_adaptive_plan_selection)
                ADP_new, FIX_new, TMP_new, ADP, FIX, TMP, ADP_p, FIX_p, TMP_p, concat_df = \
                    all_cycle_select_df(match_records, df_splitselection, series_adaptive_plan_selection,
                                        fixed_plan_selection)
            # else:

                # print(concat_df)

                # 方案选择问题检测
                # plan_select_state = plan_selection_problem(str_date, str_int, concat_df, output_df, df_abnormal_cycle,
                #                                            df_splitselection, new_df_plans, plan_select_state,
                #                                            hour_or_num, unit, num_in_detail, db)

                # ########################画图瞧一瞧
                # if if_test_mode:
                #     print('要画图啦')
                #     plot(str_date, str_int, ADP_p, FIX_p, TMP_p, df_cycle_dev, series_adaptive_plan_selection,
                #          fixed_plan_selection, adaptive_plan_list, fixed_plan_list, df_abnormal_cycle, plan_select_state)
                # ########################画图瞧一瞧

                # 方案选择绿
                for m in range(int(output_df[2])):
                    # PlanCode.append(m + 1)
                    planlabel = '自适应方案' + str(m + 1)
                    plannum = output_df[2 + m + 1]
                    tempplanpie = {"value": plannum, "name": planlabel}
                    PlanPie_outside.append(tempplanpie)
                # for m in range(int(output_df[2 + int(output_df[2]) + 2])):
                #     planlabel = '固定方案' + str(m + 1)
                #     plannum = output_df[2 + int(output_df[2]) + 2 + m + 1]
                #     tempplanpie = {"value": plannum, "name": planlabel}
                #     PlanPie_outside.append(tempplanpie)
                # alter
                planlabel = '固定方案'
                plannum = output_df[2 + int(output_df[2]) + 1]
                tempplanpie = {"value": plannum, "name": planlabel}
                PlanPie_outside.append(tempplanpie)
                PlanPie_outside.append({"value": output_df[-1], "name": '临时方案'})
                PlanPie_inside.append({"value": output_df[-1], "name": '临时方案'})
                PlanPie_inside.append({"value": output_df[1], "name": '自适应方案'})
                PlanPie_inside.append({"value": output_df[2 + int(output_df[2]) + 1], "name": '固定方案'})
            else:  # 自适应 固定 一个都没有
                concat_df = []
                for x in range(len(CycleTime)):
                    TMP.append(CONSTANTS.MAX_ADP_NUM + CONSTANTS.MAX_FIX_NUM + 1)
                vo_df = avg_volume_per_cycle(match_records)
                for x in range(len(vo_df)):
                    TMP_new.append(str(vo_df.iloc[x]['Volume']))
                PlanPie_inside = [{"value": len(df_splitselection), "name": '临时方案'}]
                PlanPie_outside = [{"value": len(df_splitselection), "name": '临时方案'}]

            # 方案选择问题检测
            plan_select_state = plan_selection_problem(str_date, str_int, concat_df, output_df, df_abnormal_cycle,
                                                       df_splitselection, new_df_plans, plan_select_state,
                                                       hour_or_num, unit, num_in_detail, db, logger)
            # 偏移量画图
            for m in range(len(df_cycle_dev)):
                offset.append(df_cycle_dev['Dev'][m])
                if df_cycle_dev['Dev'][m] >= CONSTANTS.MAX_DEV:
                    ab_offset.append(df_cycle_dev['Dev'][m])
                else:
                    ab_offset.append('-')

            # 转换成Json格式
            VoPlanScatter = {"Data": [{"Value": ADP_new, "Name": GlobalVar.PSlabelADP},
                                      {"Value": FIX_new, "Name": GlobalVar.PSlabelFIX},
                                      {"Value": TMP_new, "Name": GlobalVar.PSlabelTMP}],
                             "Describe": GlobalVar.Des_VoPlan}
            PlanDistribution = {"ADP": ADP, "FIX": FIX, "TMP": TMP, "ADPNum": CONSTANTS.MAX_ADP_NUM,
                                "FIXNum": CONSTANTS.MAX_FIX_NUM, "Describe": GlobalVar.Des_PlanDistribution}
            SelectRate = {"PlanPie_outside": PlanPie_outside, "PlanPie_inside": PlanPie_inside,
                          "Describe": GlobalVar.Des_SelectRate}
            SplitOffset = {"Offset": {"Data": offset, "Name": '相对偏差'}, "Abnormal": {"Data": ab_offset, "Name": '异常点'},
                           "Describe": GlobalVar.Des_SplitOffset}

            SplitPlanPlot = {"IntInformation": IntInformation, "Time": CycleTime, "VolumePlan": VoPlanScatter,
                             "PlanDistribution": PlanDistribution, "SelectRate": SelectRate, "SplitOffset": SplitOffset}
            all_dict = {"appCode": SplitPlanPlot}
            json_ps_dict = json.dumps(all_dict, ensure_ascii=False)
            # print(json_ps_dict)
            data_type = '9'
            # write_postgresql(str_int, str_date, data_type, json_ps_dict, conn, logger)  # 9: 绿信比方案选择画图
            CONSTANTS.MAX_ADP_NUM = 0
            CONSTANTS.MAX_FIX_NUM = 0

            # print(concat_df[200:])
            # adp_state(str_date, str_int, concat_df, db, logger, len(df_cycle_dev))

        else:
            print('WARNING:无静态方案')
            plan_number='无静态方案'
            plan=plan_number
            return plan

    else:
        print(GlobalVar.Error_NoData + str('（无绿信比运行记录）'))  # 无数据
        plan_number = '无静态方案'
        plan = plan_number
        return plan

def main(data):
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(console)
    db = cx_Oracle.connect(GlobalVar.OracleUser)
    conn = psycopg2.connect(database=GlobalVar.PgUser_getdata_db, user=GlobalVar.PgUser_getdata_us,
                            password=GlobalVar.PgUser_getdata_pw, host=GlobalVar.PgUser_getdata_ht,
                            port=GlobalVar.PgUser_getdata_pt)  # 连接pg
    cr = db.cursor()
    int_num = '192'
    sql1 = "select FSTR_INTERSECTID, FINT_DETECTORID, FSTR_CYCLE_STARTTIME, FSTR_PHASE_STARTTIME," \
           "FSTR_PHASE, FINT_CYCLE_LENGTH, FINT_DS , FINT_ACTUALVOLUME , " \
           "to_char(FSTR_DATE, 'yyyy-mm-dd') from {1} where FSTR_DATE = to_date('{0}', 'yyyy-mm-dd')" \
           "and FSTR_INTERSECTID = '{2}' order by TO_NUMBER(FSTR_INTERSECTID), FSTR_CYCLE_STARTTIME" \
        .format('2018-10-21', GlobalVar.StrRecordTableName, int_num)
    # print(sql1)
    cr.execute(sql1)
    rs1 = cr.fetchall()
    match_records = pd.DataFrame(rs1)
    match_records.columns = ['FSTR_INTERSECTID', 'FINT_DETECTORID', 'FSTR_CYCLE_STARTTIME',
                             'FSTR_PHASE_STARTTIME', 'FSTR_PHASE', 'FINT_CYCLE_LENGTH',
                             'FINT_DS', 'FINT_ACTUALVOLUME', 'FSTR_DATE']

    plan=main_PlanSelection(match_records, '2018-10-21', int_num, '{0}', '文一路路', db, conn, logger).format(data)
    return plan
if __name__ == '__main__':
    main('UTNDM000488')


