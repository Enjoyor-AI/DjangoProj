from proj.config import  database
import re,os
from datetime import datetime,timedelta
import psycopg2

def get_configure_info(starttime,endtime):
    dict={}
    #starttime='2018-12-10'   #时间现在写死，改成定时任务只需要把这两个时间注释掉就好
    #endtime='2018-12-14'
    sql1 = """select
                 A.*,
                 (case  
                       when A.region like '%SC%' then '上城大队' 
                         when A.region like '%JG%' then '江干大队' 
                             when A.region like '%XH%' then '西湖大队' 
                             when A.region like '%JQ%' then '景区大队' 
                             when A.region like '%GS%' then '拱墅大队' 
                             when A.region like '%XC%' then '下城大队' 
                             when A.region like '%CS%' then '测试区域' 
                             else '未知区域'
                    end) as region_name,
                 B.user_name
            from
                (select oper,opertime,opertype,region,userid from record_data_parsing where opertime between '{0}' and '{1}'
                 and oper like '%SA=%') A
            left join
                 sms_user as B
            on
                 A.userid=B.company_id""".format(starttime, endtime)  # sql语句，输出形式为：oper,opertime,opertype,region,userid,region_name,name

    sql2="""select
    A.*,
	B.region_name,
	C.node_name,
	D.sys_code as scatsid
from 
(
select
   A.channel_id,
	 A.node_id,
	 B.region_code
from
   (select channel_id,node_id from pe_tobj_channel)as A, 
	 (select region_code,node_id from region_inter)as B
where A.node_id=B.node_id
)as A,
(select * from region_classify)as B,
(select * from pe_tobj_node)as C,
(select * from pe_tobj_node_info)as D
where 
   B.region_code=A.region_code and
	 C.node_id=A.node_id and
	 D.node_id=A.node_id
	 """  #输出形式为channel_id,node_id,region_code,region_name,node_name,scatsid
    pp=database.Postgres()
    data_all=pp.call_pg_data(sql1)
    channel=pp.call_pg_data(sql2,fram=True)
    pp.db_close()
    for i in data_all:
        list=[]
        scatsid=None
        node_name={}
        SA = None
        userid = i[4]
        name = i[6]
        time = i[1]
        oper = i[0]
        opertype=i[2]
        region_name=i[5]
        m = re.search(r'SA=([0-9]+)', oper, re.I)
        SA = m.group(1)   #对每条修改记录提取修改的通道号
        detail=channel[(channel.channel_id==SA)&(channel.region_name==region_name)] #查询该通道号以及属于这个区域的数据
        detail=detail.values.tolist()   #转成列表
        for i in detail:     #获取路口id以及路口名字
            if i[5]!=None:
                if scatsid==None:
                    scatsid=i[5]
                else:
                    scatsid=scatsid+','+i[5]
                node_name[i[5]]=i[4]
        # sql_scats="""select
        #                A.node_id,
        #                  A.channel_id,
        #                  B.scatsid
        #             from
        #             (
        #             select
        #                *
        #             from
        #                pe_tobj_channel
        #             where
        #                channel_id='{0}'
        #             ) as A
        #             left join
        #             (
        #             select
        #                A.*,
        #                  B.sys_code as scatsid
        #             from
        #             (
        #             select
        #                B.*
        #             from
        #             (
        #             select
        #                region_code
        #             from
        #                region_classify
        #             where
        #                region_name='{1}'
        #             ) as A,
        #             (
        #             select
        #                *
        #             from
        #                region_inter
        #             )as B
        #             where
        #                B.region_code=A.region_code
        #             ) as A
        #             left join pe_tobj_node_info as B
        #             on A.node_id=B.node_id
        #             )as B
        #             on A.node_id=B.node_id""".format(SA,region_name)
        # data_scats=pp.call_pg_data(sql_scats)  #输出形式为node_id,channel_id,scats_id
        # for i in data_scats:      #scats_id可能有一个或多个，进行整理
        #     if i[2]==None:
        #         pass
        #     else:
        #         if scatsid==None:
        #             scatsid=i[2]
        #         else:
        #             scatsid=scatsid+','+i[2]
        if node_name=={}:
            node_name=None
        else:
            node_name=str(node_name)[1:-2]
        list.append([userid,name,SA,time,oper,opertype,region_name,scatsid,node_name])
        if userid in dict.keys():    #通过字典把信息按人分组
            dict[userid].append(list)
        else:
            dict[userid]=[list]
    pp.db_close()
    pg=database.demo_pg_inf2
    conn=psycopg2.connect(database=pg['database'],user=pg['user'],password=pg['password'],host=pg['host'],port=pg['port'])
    cr=conn.cursor()
    sql_insert = "insert into scats_configure_update_log (userid,name,sa,time,oper,opertype,region_name,scatsid,node_name) " \
                 "values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for key in dict.keys():
        for i in dict[key]:
            cr.execute(sql_insert,i[0])
            conn.commit()
    cr.close()
    conn.close()

def main():
    endtime=datetime.now()
    starttime=endtime - timedelta(days=1)
    if endtime.hour==1:           #判断是否是1点，每天凌晨1点运行
        get_configure_info(starttime,endtime)
    else:
        pass

if __name__ == '__main__':
    main()