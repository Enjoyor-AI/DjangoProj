import os
import numpy as np
import cx_Oracle
import pandas as pd
import GlobalVar

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class LoopError(object):
    def __init__(self, error_code, error_name, detail_error=None):
        self.error_code = error_code
        self.error_name = error_name
        self.detail_error = detail_error


class LoopStatus:
    non_error                        = LoopError(GlobalVar.non_error_code, GlobalVar.non_error_name, GlobalVar.non_error_detail)
    major_error                      = LoopError(GlobalVar.major_error_code, GlobalVar.major_error_name, GlobalVar.major_error_detail)
    major_error_overflow             = LoopError(GlobalVar.major_error_overflow_code, GlobalVar.major_error_overflow_name, GlobalVar.major_error_overflow_detail)
    major_error_oversaturated        = LoopError(GlobalVar.major_error_oversaturated_code, GlobalVar.major_error_oversaturated_name, GlobalVar.major_error_oversaturated_detail)
    major_error_overflowandsaturated = LoopError(GlobalVar.major_error_overflowandsaturated_code, GlobalVar.major_error_overflowandsaturated_name, GlobalVar.major_error_overflowandsaturated_detail)
    major_error_noise                = LoopError(GlobalVar.major_error_noise_code, GlobalVar.major_error_noise_name, GlobalVar.major_error_noise_detail)
    major_error_data_loss            = LoopError(GlobalVar.major_error_data_loss_code, GlobalVar.major_error_data_loss_name, GlobalVar.major_error_data_loss_detail)
    minor_error_noise                = LoopError(GlobalVar.minor_error_noise_code, GlobalVar.minor_error_noise_name, GlobalVar.minor_error_noise_detail)

    major_error_communication_failed = LoopError(GlobalVar.major_error_communication_failed_code, GlobalVar.major_error_communication_failed_name, GlobalVar.major_error_communication_failed_detail)


class Online:
    def __init__(self):
        self.a1 = 0
        self.a2 = 1
        self.max_noise = 6
        self.min_noise = 2
        self.maximum_median_volume = 70
        self.maximum_median_ds = 130
    """
    功能：通过箱型图模型筛选异常数据点
    输入：饱和度（ds）和流量（q）的比值的集合（列表）
    输出：上下阈值（2个返回值）
    """
    def Box_Diagram(self, data: list):
        # data = data.tolist()
        # Q1为数据占25%的数据值范围
        Q1 = np.percentile(data, 25)
        # Q3为数据占75%的数据范围
        Q3 = np.percentile(data, 75)
        IQR = Q3 - Q1
        # 异常值的范围
        outlier_step = 1.5 * IQR
        min_rate = Q1 - outlier_step
        max_rate = Q3 + outlier_step
        return max_rate, min_rate

    """
    功能：筛选有问题的数据点
    输入：采样间隔内的饱和度（ds），流量（q）和时间（time）的集合（列表）
    输出：缺失数据点的集合，异常数据点的集合,正常数据点的集合（列表）
    """
    def Online_Select(self, data_df):
        # road = road.tolist()
        # lane = lane.tolist()
        ds = data_df['FINT_DS'].tolist()
        q = data_df['FINT_ACTUALVOLUME'].tolist()
        # time = pd.to_datetime(time.tolist())
        time = data_df['FSTR_CYCLE_STARTTIME'].tolist()
        # date = date.tolist()
        error_list = []
        error_zero_list = []
        normal_list = []
        ratio_list = []
        for n in range(len(data_df)):
            if int(q[n]) + int(ds[n]) == 0:    # 数据点缺失判断
                # if (time.hour[n] > 7 and time[n].hour < 10) or (time[n].hour > 17 and time[n].hour < 19):
                if (int(time[n][:2]) > 7 and int(time[n][3:5]) < 10) or (int(time[n][:2]) > 17 and int(time[n][3:5]) < 19):
                    if sum(q) < self.a1:
                        # error1 = [road[n], lane[n], ds[n], q[n], time[n], date[n]]
                        error_zero_list.append(data_df.iloc[n].tolist())
                        error1 = []
                    else:
                        # normal1 = [road[n], lane[n], ds[n], q[n], time[n], date[n]]
                        normal_list.append(data_df.iloc[n].tolist())
                        normal1 = []
                else:
                    if sum(q) < self.a2:
                        # error2 = [road[n], lane[n], ds[n], q[n], time[n], date[n]]
                        error_zero_list.append(data_df.iloc[n].tolist())
                        error2 = []
                    else:
                        # normal2 = [road[n], lane[n], ds[n], q[n], time[n], date[n]]
                        normal_list.append(data_df.iloc[n].tolist())
                        normal2 = []
            else:    # 数据异常点判断
                ratio = ds[n]/q[n]
                ratio_list.append(ratio)
                max_rate, min_rate = self.Box_Diagram(ratio_list)
                if min_rate < ratio < max_rate:
                    # normal3 = [road[n], lane[n], ds[n], q[n], time[n], date[n]]
                    normal_list.append(data_df.iloc[n].tolist())
                    normal3 = []
                else:
                    # error3 = [road[n], lane[n], ds[n], q[n], time[n], date[n]]
                    error_list.append(data_df.iloc[n].tolist())
                    normal3 = []
        # print("缺失数据点的集合长度：", len(error_zero_list))
        # print("异常数据点的集合长度：", len(error_list))
        # print("正常数据点的集合长度：", len(normal_list))
        # print(normal_list)
        # print(pd.DataFrame(normal_list))
        return error_zero_list, error_list, normal_list

    """
    功能：采样间隔内的数据质量标注
    输入：采样间隔内的饱和度（ds），流量（q）和时间（time）的集合（列表），缺失数据点的集合，异常数据点的集合
    输出：数据质量标签的集合
    """
    def Data_Quality_Labeling(self, data_series,error_zero_list,error_list):
        output_state = []
        median_volume = data_series.median()["FINT_ACTUALVOLUME"]
        median_ds = data_series.median()["FINT_DS"]
        data = data_series.values.tolist()
        # print(data)
        # print("原始数据的长度：", len(data))
        # print("数据缺失点的交集长度：", len(error_zero_list))
        # print("数据异常点的交集长度：", len(error_list))
        if len(error_zero_list) == len(data):        #数据缺失
            output_state = LoopStatus.major_error
            # print("数据质量类型：数据缺失")
        elif len(error_zero_list)/len(data) >= 0.6:  #数据丢失严重
            output_state = LoopStatus.major_error_data_loss
            # print("数据质量类型：数据丢失严重")
        elif len(error_zero_list)/len(data) > 0.2 and len(error_zero_list)/len(data) < 0.6: #通讯异常
            output_state = LoopStatus.major_error_communication_failed
            # print("数据质量类型：通讯异常")
        elif len(error_list) >= self.max_noise:    #大量噪点
            output_state = LoopStatus.major_error_noise
            # print("数据质量类型：大量噪点")
        elif len(error_list) >= self.min_noise:    #少量噪点判断
            output_state = LoopStatus.minor_error_noise
            # print("数据质量类型：少量噪点")
        elif int(median_ds) > self.maximum_median_ds and int(median_volume) > self.maximum_median_volume:     #流量和饱和度异常
            output_state = LoopStatus.major_error_overflowandsaturated
            # print("数据质量类型：流量和饱和度异常")
        elif int(median_volume) > self.maximum_median_volume:     #流量异常
            output_state = LoopStatus.major_error_overflow
            # print("数据质量类型：流量异常")
        elif int(median_ds) > self.maximum_median_ds:   #饱和度异常
            output_state = LoopStatus.major_error_oversaturated
            # print("数据质量类型：饱和度异常")
        else:
            output_state = LoopStatus.non_error
            # print("数据质量类型：数据正常")
            # pass
        return output_state


if __name__ == "__main__":
    oracleuser = "SIG_OPT_ADMIN/admin@192.168.20.56/orcl"
    try:
        conn = cx_Oracle.connect(oracleuser)
    except Exception as e1:
        print("数据库连接："+e1)

    cr = conn.cursor()
    # sql ="SELECT FSTR_INTERSECTID,FINT_DETECTORID,FINT_DS,FINT_ACTUALVOLUME,FSTR_CYCLE_STARTTIME,FSTR_DATE FROM SIG_OPT_ADMIN.HZ_SCATS_OUTPUT " \
    #      "WHERE FSTR_DATE = to_date( '2018-06-18','yyyy-mm-dd') AND FSTR_CYCLE_STARTTIME < '00:30:00' ORDER BY FSTR_CYCLE_STARTTIME ASC"
    sql = "SELECT * " \
          "FROM HZ_SCATS_OUTPUT WHERE FSTR_DATE = to_date( '2018-06-18','yyyy-mm-dd') " \
          "AND FSTR_CYCLE_STARTTIME < '00:30:00' ORDER BY FSTR_CYCLE_STARTTIME ASC"
    try:
        cr.execute(sql)
    except Exception as e2:
        print(e2)
        pass
    result = cr.fetchall()
    # print(result)
    conn.commit()
    cr.close()
    conn.close()
    result = pd.DataFrame(result)
    # result.columns = ['FSTR_INTERSECTID', 'FINT_DETECTORID', 'FINT_DS', 'FINT_ACTUALVOLUME', 'FSTR_CYCLE_STARTTIME', 'FSTR_DATE']

    # cr.execute(sql)
    # result = cr.fetchall()
    # result = pd.DataFrame(result)
    result.columns = ['FSTR_INTERSECTID', 'FINT_SA', 'FINT_DETECTORID', 'FSTR_CYCLE_STARTTIME',
                      'FSTR_PHASE_STARTTIME', 'FSTR_PHASE', 'FINT_PHASE_LENGTH', 'FINT_CYCLE_LENGTH', 'FINT_DS',
                      'FINT_ACTUALVOLUME', 'FSTR_DATE', 'FSTR_WEEKDAY', 'FSTR_CONFIGVERSION']

    df = result.groupby(["FSTR_INTERSECTID", "FINT_DETECTORID"])

    for group in df.groups:
        grouped_data = df.get_group(group)
        # print(grouped_data)
        ol_restore = Online()
        error_zero_list, error_list, normal_list = ol_restore.Online_Select(grouped_data)
        # print(error_list,normal_list)
        outputsate = ol_restore.Data_Quality_Labeling(grouped_data, error_zero_list, error_list)
        print(outputsate)
        # input()
