#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# about Oracle
# OracleUser = 'DINGCY/dingcy@192.168.20.62/orcl'  # oracle连接信息 办公室62
OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'  # oracle连接信息 办公室56
# OracleUser = 'SIG_OPT_ADMIN/admin@172.20.251.6/orcl'  # oracle连接信息 支队服务器
# OracleUser = 'enjoyor/admin@33.83.100.139/orcl'  # oracle连接信息 支队服务器
# OracleUser = 'ZW/Zw19930103@172.20.239.20/orcl'  # oracle连接信息 江干服务器

# remote_mode
# OracleUser = 'DINGCY/dingcy@60.190.226.163:1522/orcl'  # oracle连接信息

# DetectorStateTableName = 'HZ_SCATS_DETECTOR_STATE'  # 检测器质量判断结果表名
# # IntAlarmTableName = 'XUJIA.GS_ALI_STATUSALARM'  # 阿里报警记录
# IssueIntTest = 'HZ_SCATS_PLAN_SET_TEST'  # 保存问题路口检测结果
# StrRecordTableName = 'GS_SCATS_OUTPUT'  # 经过解析的战略运行记录表名 拱墅
# IntRankTableName = 'GS_DYNA_SUM_CG_PERIOD_27'
# IntRankInitTableName = 'GS_DYNA_AVG_CG_PERIOD_27'
# # StrRecordTableName = 'JG_SCATS_OUTPUT'  # 经过解析的战略运行记录表名 江干
# PlanSelectionTableName = 'PE_DYNA_SPLITSELECTION'  # 江干
# SplitRecord = 'GS_DYNA_SGN_SPLITRECORD'
# VolumeRate = 'GS_DYNA_3CYCLE_VOLUMERATE'

# server
# DetectorStateTableName = 'HZ_SCATS_DETECTOR_STATE'  # 检测器质量判断结果表名
# IssueIntTest = 'HZ_SCATS_PLAN_SET_TEST'  # 保存问题路口检测结果
# StrRecordTableName = 'HZ_SCATS_OUTPUT'  # 经过解析的战略运行记录表名 拱墅
# IntRankTableName = 'HZ_DYNA_SUM_CG_PERIOD'
# IntRankInitTableName = 'HZ_DYNA_AVG_CG_PERIOD'
# PlanSelectionTableName = 'PE_DYNA_SPLITSELECTION'  #

# 江干服务器172.20.239.24或支队服务器172.20.251.6
DetectorStateTableName = 'HZ_SCATS_DETECTOR_STATE'  # 检测器质量判断结果表名
IssueIntTest = 'HZ_SCATS_PLAN_SET_TEST'  # 保存问题路口检测结果
StrRecordTableName = 'HZ_SCATS_OUTPUT'  # 经过解析的战略运行记录表名
IntRankTableName = 'HZ_DYNA_SUM_CG_PERIOD'
IntRankInitTableName = 'HZ_DYNA_AVG_CG_PERIOD'
PlanSelectionTableName = 'HZ_DYNA_PLANSELECTION'
SplitRecord = 'HZ_DYNA_SGN_SPLITRECORD'
VolumeRate = 'HZ_DYNA_3CYCLE_VOLUMERATE'

#  '''
# server:
# HZ_DYNA_SPLITSELECTION:
# select SITEID_T FSTR_INTERSECTID, RECVDATE FSTR_DATE, RECVTIME FSTR_CYCLE_STARTTIME,
# ACTUALCYCLETIME FSTR_CYCLE_LENGTH, A PHASE_A, B PHASE_B, C PHASE_C, D PHASE_D, E PHASE_E, F PHASE_F, G PHASE_G
# from ZHOUWEI.RUN_STR_INFORMATION1
# '''
# StatisticPlanTableName = 'PE_SCATS_STATICSPLITPLANS'
# IntRankTableName = 'JG_DYNA_SUM_CG_PERIOD_91'
# IntRankInitTableName = 'JG_DYNA_AVG_CG_PERIOD_91'
# SERVER
# OracleUser = 'ZW/Zw19930103@172.20.239.20/orcl'  # oracle连接信息
# StrRecordTableName = 'HZ_SCATS_OUTPUT'  # 经过解析的战略运行记录表名
# IntNameTableName = 'INTERSECT_INFORMATION'  # 路口列表表名
# DetectorStateTableName = 'HZ_SCATS_DETECTOR_STATE'  # 检测器质量判断结果表名
# PlanSelectionTableName = 'HZ_DYNA_PLANSELECTION'
# StatisticPlanTableName = 'PEGS_SCATS_STATICSPLITPLANS'

# about PostgreSQL
# 办公室
PgUser_intname_db = "superpower"
PgUser_intname_us = "postgres"  # dataer
PgUser_intname_pw = "postgres"
PgUser_intname_ht = "33.83.100.144"
PgUser_intname_pt = "5432"
PgUser_getdata_db = "superpower"
PgUser_getdata_us = "postgres"  # dataer
PgUser_getdata_pw = "postgres"
PgUser_getdata_ht = "33.83.100.144"
PgUser_getdata_pt = "5432"

# 172.20.239.24江干
# PgUser_intname_db = "superpower"
# PgUser_intname_us = "postgres"
# PgUser_intname_pw = "postgres"
# PgUser_intname_ht = "172.20.239.24"
# PgUser_intname_pt = "5432"
# PgUser_getdata_db = "superpower"
# PgUser_getdata_us = "postgres"
# PgUser_getdata_pw = "postgres"
# PgUser_getdata_ht = "172.20.239.24"
# PgUser_getdata_pt = "5432"


# 阿里服务器
# PgUser_intname_db = "superpower"
# PgUser_intname_us = "postgres"
# PgUser_intname_pw = "postgres"
# PgUser_intname_ht = "33.83.100.144"
#
# PgUser_intname_pt = "5432"
# PgUser_getdata_db = "superpower"
# PgUser_getdata_us = "postgres"
# PgUser_getdata_pw = "postgres"
# PgUser_getdata_ht = "33.83.100.144"
# PgUser_getdata_pt = "5432"
# # 支队服务器
# PgUser_intname_db = "superpower"
# PgUser_intname_us = "postgres"
# PgUser_intname_pw = "postgres"
# PgUser_intname_ht = "172.20.251.98"
#
# PgUser_intname_pt = "5432"
# PgUser_getdata_db = "superpower"
# PgUser_getdata_us = "postgres"
# PgUser_getdata_pw = "postgres"
# PgUser_getdata_ht = "172.20.251.98"
# PgUser_getdata_pt = "5432"

# remote_mode
# PgUser_intname_db = "superpower"
# PgUser_intname_us = "postgres"
# PgUser_intname_pw = "postgres"
# PgUser_intname_ht = "60.190.226.165"
# PgUser_intname_pt = "5432"
# PgUser_getdata_db = "superpower"
# PgUser_getdata_us = "postgres"
# PgUser_getdata_pw = "postgres"
# PgUser_getdata_ht = "60.190.226.165"
# PgUser_getdata_pt = "5432"

PgUser_test_db = "postgres"
PgUser_test_us = "postgres"
PgUser_test_pw = "19861015dh"
PgUser_test_ht = "localhost"
PgUser_test_pt = "5432"
PostgresqlTableName_test = 'json_data_test'
PgUser_db = "superpower"
PgUser_us = "postgres"
PgUser_pw = "postgres"
PgUser_ht = "33.83.100.144"  # "localhost"
# PgUser_ht = "172.20.251.98"  # 支队服务器
# PgUser_ht = "33.83.100.144"  # 支队服务器

PgUser_pt = "5432"
PostgresqlTableName = 'transfer_json_data'
PostgresqlFunctionTableName = 'function_upnode_overviews'
PostgresqlUpNodeTableName = 'up_node_flow_overviews'
PostgresqlInterInfoTableName = 'inter_info'
PostgresqlMapPointTableName = 'system_diagnostics_point_history'
PostgresqlAlarmRecord = 'alarm_record'
newPostgresqlAlarmRecord = 'disposal_alarm_data'
PostgresqlSplitPlan = 'split_plans_overviews'
PostgresqlAdjustLog = 'adjust_log'
pg_lane_table = 'pe_tobj_lane'
pg_node_table = 'pe_tobj_node'
pg_road_table = 'pe_tobj_roadsect'
pg_transect_table = 'pe_tobj_transect'

# Detector State
non_error_code, non_error_name, non_error_detail = 'NORMSTA', '检测器正常', '检测器工作正常'
major_error_code, major_error_name, major_error_detail = 'ERR0001', '检测器损坏', '无数据'
major_error_overflow_code, major_error_overflow_name, major_error_overflow_detail = 'ERR0002', '检测器损坏', '流量异常'
major_error_oversaturated_code, major_error_oversaturated_name, major_error_oversaturated_detail = 'ERR0003', '检测器损坏', '饱和度异常'
major_error_overflowandsaturated_code, major_error_overflowandsaturated_name, major_error_overflowandsaturated_detail = 'ERR0004', '检测器损坏', '流量及饱和度异常'
major_error_noise_code, major_error_noise_name, major_error_noise_detail = 'ERR0005', '数据异常', '大量数据噪点'
major_error_data_loss_code, major_error_data_loss_name, major_error_data_loss_detail = 'ERR0006', '数据异常', '数据丢失严重'
minor_error_noise_code, minor_error_noise_name, minor_error_noise_detail = 'WRN0010', '数据噪点', '少量数据噪点'
major_error_communication_failed_code, major_error_communication_failed_name, major_error_communication_failed_detail = 'ERR0000', '通讯异常', '通讯异常'
# Detector Rank
state_well_code, state_well_name, state_well_detail = 'R001', '良好', '路口检测器质量较好，检测器基本无损坏，不影响自适应方案选择。'
state_normal_code, state_normal_name, state_normal_detail = 'R002', '一般', '路口检测器质量尚可，部分检测器损坏，可能导致自适应方案选择不准确。'
state_bad_code, state_bad_name, state_bad_detail = 'R003', '较差', '路口检测器质量较差，部分方向检测器完全损坏，导致自适应方案选择不准确。'
state_unknow_code, state_unknow_name, state_unknow_detail = 'R000', '未知', ' '
state_norm = '正常'
state_error = '损坏'
state_wrn = '异常'
# Split Plan Selection Problem
no_selection_pro_code, no_selection_pro_name, no_selection_pro_detail, no_selection_pro_title, no_selection_pro_reference = 'S000', '配时方案设置较合理', '', '', ''
no_select_all_plan_code, no_select_all_plan_name, no_select_all_plan_detail, no_select_all_plan_title, no_select_all_plan_reference = 'S001', '没有选中任何自适应方案', '未选择任何自适应方案，请检查方案设置。', '配时方案不合理', '配时方案分布%配时方案被选率'
tmp_rate_high_code, tmp_rate_high_name, tmp_rate_high_detail, tmp_rate_high_title, tmp_rate_high_reference = 'S002', '自适应方案被选率低/临时方案较多', '配时方案设置不合理，不足以适应复杂流量变化。', '配时方案不合理', '配时方案分布%配时方案被选率'
no_select_plan_code, no_select_plan_name, no_select_plan_detail, no_select_plan_title, no_select_plan_reference = 'S003', '某几个自适应方案未被选中', '%个自适应方案未被选择，请检查方案是否合理。', '配时方案不合理', '配时方案分布'
kept_one_plan_code, kept_one_plan_name, kept_one_plan_detail, kept_one_plan_title, kept_one_plan_reference = 'S004', '某一自适应方案被选率过高', '较长时间选择自适应方案%，请检查其余方案是否合理。', '配时方案不合理', '配时方案分布%配时方案被选率'
plan_irrational_code, plan_irrational_name, plan_irrational_detail, plan_irrational_title, plan_irrational_reference = 'S005', '某一自适应方案被选率过低', '较短时间选择自适应方案%，请检查该方案是否合理。', '配时方案不合理', '配时方案分布%配时方案被选率'
tmp_plan_lock_code, tmp_plan_lock_name, tmp_plan_lock_detail, tmp_plan_lock_title, tmp_plan_lock_reference = 'S006', '锁定临时方案', '较长时间锁定临时方案，请检查设置是否合理', '配时方案不合理', '配时方案分布%配时方案被选率'
one_plan_lock_code, one_plan_lock_name, one_plan_lock_detail, one_plan_lock_title, one_plan_lock_reference = 'S007', '锁定自适应方案', '较长时间锁定自适应方案%，请检查设置是否合理', '配时方案不合理', '配时方案分布%配时方案被选率'
fix_plan_unfit_code, fix_plan_unfit_name, fix_plan_unfit_detail, fix_plan_unfit_title, fix_plan_unfit_reference = 'S008', '固定方案不合理（偏移量大）', '固定方案实际运行偏差较大，请检查固定方案设置是否合理', '配时方案不合理', '绿信比偏差%配时方案分布'
peak_no_fix_code, peak_no_fix_name, peak_no_fix_detail, peak_no_fix_title, peak_no_fix_reference = 'S009', '建议高峰期设置固定方案', '流量高峰期选择临时方案，建议设置固定方案', '配时方案不合理', '流量-方案数据%配时方案分布'
unit_hour = '小时'
unit_num = '个'
unit_vehicle = '车辆/小时'
unit_percent = '百分比'
# Cycle Plan Problem
no_cycle_pro_code, no_cycle_pro_name, no_cycle_pro_detail, no_cycle_pro_title, no_cycle_pro_reference = 'C000', '周期方案设置较合理', '', '', ''
cycle_lock_code, cycle_lock_name, cycle_lock_detail, cycle_lock_title, cycle_lock_reference = 'C001', '周期锁定时间较长', '周期长时间锁定，请确认是否合理。', '周期方案不合理', '流量-周期数据'
low_volume_high_cycle_code, low_volume_high_cycle_name, low_volume_high_cycle_detail, low_volume_high_cycle_title, low_volume_high_cycle_reference = 'C002', '低流量周期值过高', '低流量情况下周期值设置过高，红灯等待时间过长。', '周期方案不合理', '流量-周期数据'
# Trans Volume Problem
no_volume_pro_code, no_volume_pro_name, no_volume_pro_detail, no_volume_pro_title, no_volume_pro_reference = 'V000', '交通状况良好', '', '', ''
heavy_volume_inrushhour_code, heavy_volume_inrushhour_name, heavy_volume_inrushhour_detail, heavy_volume_inrushhour_title, heavy_volume_inrushhour_reference = 'V001', '高峰期流量过大', '%高峰车流量过大，难以简单通过配时方案调整解决。', '交通压力过大', '入口道流量%上游出口道流量'

# plot describe in IssueIntersect
Des_VoPlan = '结合流量情况和配时方案选择，观察流量与配时方案选择的关系。'
Des_PlanDistribution = '结合被选配时方案与时间的关系，观察配时方案选择在一天中的分布情况。'
Des_SelectRate = '结合不同配时方案被选概率，从整体上观察方案设置是否合理。'
Des_SplitOffset = '结合配时方案记录与实际运行方案，观察二者之间存在的偏差，并将偏差值过大的点在图中标注出来。'
Des_CyclePlan = '结合流量情况和周期长度，观察周期设置与流量的关系。'
Des_EnterIntVolume = '结合路口每个入口道的流量，观察路口交通情况。'
Des_EnterIntDS = '结合路口每个入口道的饱和度，观察路口交通情况。'
Des_UpperIntVolume = '结合路口每个入口道的上游出口道的流量，观察上下游路口交通关系。'


# about Illustration
OracleError_TableName = 'Oracle_ERROR: 表名输入错误或无数据'
OracleError_DateName = 'Oracle_ERROR: 日期输入错误或无数据'
OracleError_IntName = 'Oracle_ERROR: 路口输入错误或无数据'
OracleError_Connection = 'Oracle_ERROR: 数据库连接超时'
Oracle_Insert = 'Oracle: 已插入分析结果'
Oracle_Delete = 'Oracle: 数据重复，已删除原有数据并插入新数据'
PostgresqlError_Connection = 'Postgre_ERROR: 数据库连接超时或用户信息错误，插入数据失败'
Postgresql_Insert = 'Postgresql: 已插入数据至Postgresql'
Postgresql_Delete = 'Postgresql: 数据重复，已删除原有数据并插入新数据'
PostgreError_TableName = 'Postgre_ERROR: 表名输入错误或无数据'
PostgreError_DateName = 'Postgre_ERROR: 日期输入错误或无数据'
Error_NoData = 'WARNING: 未获取到可用数据'
SectionOne = 'Section1: 检测器质量 判断 结束'
SectionTwo = 'Section2: 检测器质量 画图数据 结束'
SectionThree = 'Section3: 今日检测器结果统计 结束'
SectionFour = 'Section4: 历史检测器结果统计 结束'
SectionFive = 'Section5: 流量与饱和度统计 结束'
SectionSix = 'Section6: 方案选择分析 结束'
SectionSeven = 'Section7: 路口检测器情况评价 结束'
SectionEight = 'Section8: 战略路口检测器情况排名 & 首页面总体情况 & 首页面地图情况（方) 结束'
SectionNine = 'Section9: 绿信比方案选择情况评价与方案选择画图数据 结束'
SectionTen = 'Section10: 周期方案选择情况评价与周期选择画图数据 结束'
SectionEleven = 'Section11: 路口交通流量情况评价与入口道画图数据 结束'
SectionTwelve = 'Section12: 路口问题明细 结束'
SectionThirteen = 'Section13: 问题路口列表 结束'
SectionTwentyone = 'Section21: 路口分级画图 结束'
SectionTwentytwo = 'Section22: 路口分级初始化 结束'
SectionTwentythree = 'Section23: 路口分级建议 结束'

IntInforFetched = '路口列表已获取'

# 'No Eligible Data'
# 'Section1: Detector State Test Finished.'
# 'Section2: Detector State Plot Finished.'
# 'Section3: Current Detector State Detail Count Finished.'
# 'Section4: History Detector State Detail Count Finished.'
# 'Section5: Volume and DS Count Finished.'
# 'Section6: Plan Selection Finished.'
# 'Intersect Information List Fetched'
PSlabelADP = '自适应绿信比方案'
PSlabelFIX = '固定绿信比方案'
PSlabelTMP = '临时绿信比方案'

# xjc——数据库内容

oracle_database = {}
oracle_database['user'] = 'enjoyor'
oracle_database['password'] = 'admin'
oracle_database['host'] = "33.83.100.139"
pg_database = {}


"""
pg_database['database'] = 'postgis_23_sample'
pg_database['user'] = 'postgres'
pg_database['password'] = 'xiejc'
pg_database['host'] = 'localhost'
pg_database['port'] = '5432'
"""

# pg_database['database'] = "superpower"
# pg_database['user'] = "postgres"
# pg_database['password'] = "postgres"
# pg_database['host'] = "192.168.22.72"
# pg_database['port'] = "5432"

pg_database['database'] = "superpower"
pg_database['user'] = "postgres"
pg_database['password'] = "postgres"
pg_database['host'] = "33.83.100.144"
pg_database['port'] = "5432"

"""
pg_database['database'] = "postgres"
pg_database['user'] = "postgres"
pg_database['password'] = "postgres"
pg_database['host'] = "192.168.20.51"
pg_database['port'] = "5432"
"""

# ORACLE 中数据表格
HZ_SCATS_OUTPUT = 'HZ_SCATS_OUTPUT'
HZ_ROSECT_MODEL_TP = 'HZ_ROSECT_MODEL_TP'
HZ_ROSECT_MODEL_HISTORY = 'HZ_ROSECT_MODEL_HISTORY'
SCATS_DATA_LANE_CAP_TP = 'SCATS_DATA_LANE_CAP_TP'
SCATS_DATA_SELECT = 'SCATS_DATA_SELECT'
SCATS_LC_DATA = 'SCATS_LC_DATA'
SCATS_SL_SUM = 'SCATS_SL_SUM'
SL_SCATS_LOOP_STATUS = 'SL_SCATS_LOOP_STATUS'
SL_SCATS_LOOP_INF = 'SL_SCATS_LOOP_INF'

# procedure 过程

LANE_CAP_CAL = 'LANE_CAP_CAL'   # 计算车道流率
SCATS_DATA_CHOSE = 'SCATS_DATA_CHOSE'   # 数据选择过程
CUR_CLR_CAL = 'CUR_CLR_CAL4'     # CUR 和CLR计算及车道聚合数据
SUM_SCATS_SL = 'SUM_SCATS_SL'   # 车道数据聚合
SCATS_LOOP_STATUS = 'SCATS_LOOP_STATUS'     # 线圈质量判断

# PG中数据表格

sl_scats_rdsect_nsl_result = 'sl_scats_rdsect_nsl_result'
sl_select_rdsectid = 'sl_select_rdsectid'
sl_scats_area_nsl_result = 'sl_scats_area_nsl_result'
sl_route_nsl_result = 'sl_route_nsl_result'
sl_route_rdsectid = 'sl_route_rdsectid'
sl_scats_loop_inf = 'sl_scats_loop_inf'
sl_intersection_nsl_result = 'sl_intersection_nsl_result'
pe_tobj_roadsect = 'pe_tobj_roadsect'
sl_scats_sl_sum = 'sl_scats_sl_sum'
sl_scats_data_repair = 'sl_scats_data_repair'
sl_scats_key_lane = 'sl_scats_key_lane'
sl_area_rdsectid = 'sl_area_rdsectid'
sl_scats_coor_route_result = 'sl_scats_coor_route_result'
transfer_json_data = 'transfer_json_data'
sl_scats_area_nsl_result_realtime = 'sl_scats_area_nsl_result_realtime'
sl_scats_loop_inf_back = 'sl_scats_loop_inf_back'
"""
sl_area_nsl_result = 'sl_scats_area_nsl_result'
sl_area_rdsectid = 'sl_area_rdsectid'
sl_select_rdsectid = 'sl_select_rdsectid'
sl_route_nsl_result = 'route_nsl_result'
sl_route_rdsectid = 'route_rdsectid'
sl_scats_loop_inf = 'sl_scats_loop_inf'
sl_intersection_nsl_result = 'sl_intersection_nsl_result'
pe_tobj_roadsect = 'pe_tobj_roadsect'
sl_scats_sl_sum = 'sl_scats_sl_sum'
sl_scats_data_repair = 'sl_scats_data_repair'
sl_scats_key_lane = 'sl_scats_key_lane'

"""


# PG中函数

sl_scats_loop_inf_pro = 'sl_scats_loop_inf_pro'     #scats线圈信息查询过程
sl_scats_data_repair_pro = 'sl_scats_data_repair_pro'
sl_scats_nsl_result_pro = 'sl_scats_nsl_result_pro'
sl_intersection_nsl_cal = 'sl_intersection_nsl_cal'
sl_scats_key_lane_pro = 'sl_scats_key_lane_pro'
sl_scats_coor_route_result_pro = 'sl_scats_coor_route_result_pro'
sl_area_rdsectid_match = 'sl_area_rdsectid_match'
sl_core_area_rdsectid_select = 'sl_core_area_rdsectid_select'
sl_coor_route_rdsectid_select = 'sl_coor_route_rdsectid_select'
sl_scats_nsl_result_realtime_pro = 'sl_scats_nsl_result_realtime_pro'


# 运行参数
time_interval = 900
left_volume_rat = 0.7
left_blc_rat = 0.7
time_delay = 780
