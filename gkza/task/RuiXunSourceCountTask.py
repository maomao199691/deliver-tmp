# -*- coding: utf-8 -*-
import gkza.util.HiveConnUtil as myhive
import gkza.util.MysqlUtil as mysl
import pandas as pd
from datetime import datetime,timedelta
import xlwt
import logging
from colorama import init, Fore, Style

# 初始化 colorama 库
init()

# 创建 logger
logger = logging.getLogger(__name__)

# 设置 logger 的级别
logger.setLevel(logging.INFO)

# 创建控制台处理程序
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 创建格式化器
formatter = logging.Formatter(Fore.BLUE + '%(asctime)s - %(levelname)s - %(message)s' + Style.RESET_ALL)

# 将格式化器应用于处理程序
console_handler.setFormatter(formatter)

# 将处理程序添加到 logger
logger.addHandler(console_handler)

def get_before_week(date_str, date_format='%Y-%m-%d'):
    try:
        date_obj = datetime.strptime(date_str, date_format)
        previous_date = date_obj - timedelta(days=1)
        week_before = date_obj - timedelta(days=7)

        new_date_str = int(previous_date.strftime('%Y%m%d') + '23')
        week_before_str = int(week_before.strftime('%Y%m%d') + '00')
        return (week_before_str, new_date_str)

    except ValueError:
        logger.error("日期格式不正确 ===> 2023-09-05")


def get_filter_data(table_name):

    mysql_conn = mysl.connect_to_mysql("172.16.8.46", "root", "123456", "data_handle")
    cursor = mysql_conn.cursor()

    source_sql = f'SELECT data_source,domain FROM ruixun_source WHERE table_name  = "{table_name}" and is_inc = "持续采集" and collect_way = "爬虫采集"'

    if 'info_collect' == table_name:
        source_sql = f'SELECT data_source,domain FROM ruixun_source WHERE table_name  = "info_collect" and is_inc = "持续采集" and collect_way = "爬虫采集" and domain != "mp.weixin.qq.com"'

    if 'official_account' == table_name:
        source_sql = f'SELECT data_source,domain FROM ruixun_source WHERE table_name  = "info_collect" and is_inc = "持续采集" and collect_way = "爬虫采集" and domain = "mp.weixin.qq.com"'

    if 'policies_collect' == table_name:
        source_sql = f'SELECT data_source,domain FROM ruixun_source WHERE table_name  = "{table_name}" and domain!="mp.weixin.qq.com" '

    cursor.execute(source_sql)

    df = cursor.fetchall()

    data_sources = set()
    data_domains = set()
    for row in df:
        data_source = row[0]
        data_domain = row[1]

        if pd.notnull(data_source):
            data_sources.add(data_source)

        if pd.notnull(data_domain):
            data_domains.add(data_domain)

    filter_data = (data_sources, data_domains)

    return filter_data

def get_filter_sql(table_name, source_result):
    #data_sources, data_domains = get_filter_data(table_name)

    # 新增数据源
    if len(source_result) > 0:

        if table_name in ['info_collect', 'report_collect', 'policies_collect', 'official_account']:
            # 查询mysql 获取需要过滤的数据源
            data_sources, data_domains = get_filter_data(table_name)
            domain_sql = ' where domain in (' + ','.join(f'"{d}"' for d in data_domains)

            inc_source = ''
            if 'official_account' == table_name:
                domain_sql = ' where source in (' + ','.join(f'"{d}"' for d in data_sources)
                for row in source_result:
                    inc_domain = row[1]
                    inc_source = inc_source + ',' + f'"{inc_domain}"'
            else:
                for row in source_result:
                    inc_domain = row[0]
                    inc_source = inc_source + ',' + f'"{inc_domain}"'

            return domain_sql + inc_source + ")"

        else:
            # 无过滤数据源
            return ""
    else:
        if table_name in ['info_collect', 'report_collect', 'policies_collect', 'official_account']:
            data_sources, data_domains = get_filter_data(table_name)
            domain_sql = ' where domain in (' + ','.join(f'"{d}"' for d in data_domains) + ')'

            if 'official_account' == table_name:
                domain_sql = ' where source in (' + ','.join(f'"{d}"' for d in data_sources) + ")"

            return domain_sql
        else:
            # 无过滤数据源
            return ""

class table_all():
    table_sql = dict(report_collect="""select source, tmpA.domain, collect_way, continue, all_data, week_data,tmpB.title,tmpB.pub_time,tmpB.collect_url from
                        (select source,domain,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,domain,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from report_collect filter_sql
                            ) tmp group by source, domain) tmpA LEFT JOIN (
                        select title,pub_time,collect_url,domain from (
                        select row_number() over (partition by domain ORDER BY collect_time desc )as num,`if`(length(title_en)>0,title_en,title_zh) as title,pub_time,collect_url,domain from report_collect
                        filter_sql) tmp
                        where num=1) tmpB on tmpA.domain=tmpB.domain""",
                     policies_collect="""select source, tmpA.domain, collect_way, continue, all_data, week_data,tmpB.title,tmpB.pub_time,tmpB.collect_url from
                        (select source,domain,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,domain,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from policies_collect filter_sql) tmp group by source, domain) tmpA LEFT JOIN (
                        select title,pub_time,collect_url,domain from (
                        select row_number() over (partition by domain ORDER BY acq_time desc )as num,art_title as title,pub_time,collect_url,domain from policies_collect
                         filter_sql) tmp
                        where num=1) tmpB on tmpA.domain=tmpB.domain""",
                     org_collect="""select tmpA.source,tmpA.domain, collect_way, continue, all_data, week_data,tmpB.title,tmpB.collect_time,tmpB.collect_url from
                        (select source,domain,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,domain,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from org_collect filter_sql
                            ) tmp group by source,domain) tmpA LEFT JOIN (
                        select title,collect_time,collect_url,source,domain from (
                        select row_number() over (partition by source ORDER BY collect_time desc )as num,name as title,collect_time,collect_url,source,domain from org_collect filter_sql
                        ) tmp
                        where num=1) tmpB on tmpA.domain=tmpB.domain""",
                     ent_collect="""select tmpA.source, collect_way, continue, all_data, week_data,tmpB.title,tmpB.collect_time,tmpB.collect_url from
                        (select source,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from ent_collect filter_sql
                            ) tmp group by source) tmpA LEFT JOIN (
                        select title,collect_time,collect_url,source from (
                        select row_number() over (partition by source ORDER BY collect_time desc )as num,name as title,collect_time,collect_url,source from ent_collect
                        filter_sql) tmp
                        where num=1) tmpB on tmpA.source=tmpB.source""",
                     talent_collect="""select tmpA.source, collect_way, continue, all_data, week_data,tmpB.title,tmpB.collect_time,tmpB.collect_url from
                        (select source,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from talent_collect filter_sql
                            ) tmp group by source) tmpA LEFT JOIN (
                        select title,collect_time,collect_url,source from (
                        select row_number() over (partition by source ORDER BY collect_time desc )as num,name as title,collect_time,collect_url,source from talent_collect filter_sql
                        ) tmp
                        where num=1) tmpB on tmpA.source=tmpB.source""",
                     project_collect="""select tmpA.source, collect_way, continue, all_data, week_data,tmpB.title,tmpB.collect_time,tmpB.collect_url from
                        (select source,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from project_collect filter_sql
                            ) tmp group by source) tmpA LEFT JOIN (
                        select title,collect_time,collect_url,source from (
                        select row_number() over (partition by source ORDER BY collect_time desc )as num,title as title,collect_time,collect_url,source from project_collect filter_sql
                        ) tmp
                        where num=1) tmpB on tmpA.source=tmpB.source""",
                     census_collect="""select tmpA.source, collect_way, continue, all_data, week_data,tmpB.title,tmpB.collect_time,tmpB.collect_url from
                        (select source,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from census_collect filter_sql
                            ) tmp group by source) tmpA LEFT JOIN (
                        select title,collect_time,collect_url,source from (
                        select row_number() over (partition by source ORDER BY collect_time desc )as num,title as title,collect_time,collect_url,source from census_collect filter_sql
                        ) tmp
                        where num=1) tmpB on tmpA.source=tmpB.source""",
                     market_collect="""select tmpA.source, collect_way, continue, all_data, week_data,tmpB.title,tmpB.collect_time,tmpB.collect_url from
                        (select source,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from market_collect filter_sql
                            ) tmp group by source) tmpA LEFT JOIN (
                        select title,collect_time,collect_url,source from (
                        select row_number() over (partition by source ORDER BY collect_time desc )as num,title as title,collect_time,collect_url,source from market_collect filter_sql
                        ) tmp
                        where num=1) tmpB on tmpA.source=tmpB.source""",
                     bid_collect="""select tmpA.source, collect_way, continue, all_data, week_data,tmpB.title,tmpB.collect_time,tmpB.collect_url from
                        (select source,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from bid_collect filter_sql
                            ) tmp group by source) tmpA LEFT JOIN (
                        select title,collect_time,collect_url,source from (
                        select row_number() over (partition by source ORDER BY collect_time desc )as num,title as title,collect_time,collect_url,source from bid_collect filter_sql
                        ) tmp
                        where num=1) tmpB on tmpA.source=tmpB.source""",
                     credential_collect="""select tmpA.source,tmpA.domain, collect_way, continue, all_data, week_data,tmpB.title,tmpB.collect_time,tmpB.collect_url from
                        (select source,domain,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,domain,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from credential_collect filter_sql
                            ) tmp group by source,domain) tmpA LEFT JOIN (
                        select title,collect_time,collect_url,source,domain from (
                        select row_number() over (partition by domain ORDER BY collect_time desc )as num,name as title,collect_time,collect_url,source,domain from credential_collect filter_sql
                        ) tmp
                        where num=1) tmpB on tmpA.domain=tmpB.domain""",
                     terms_collect="""select tmpA.source, collect_way, continue, all_data, week_data,tmpB.title,tmpB.collect_time,tmpB.collect_url from
                        (select source,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from terms_collect filter_sql
                            ) tmp group by source) tmpA LEFT JOIN (
                        select title,collect_time,collect_url,source from (
                        select row_number() over (partition by source ORDER BY collect_time desc )as num,name as title,collect_time,collect_url,source from terms_collect filter_sql
                        ) tmp
                        where num=1) tmpB on tmpA.source=tmpB.source""",
                     info_collect="""select source, tmpA.domain, collect_way, continue, all_data, week_data,tmpB.title,tmpB.pub_time,tmpB.collect_url from
                        (select source,domain,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,domain,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from info_collect filter_sql
                            ) tmp group by source, domain) tmpA LEFT JOIN (
                        select title,pub_time,collect_url,domain from (
                        select row_number() over (partition by domain ORDER BY acq_time desc )as num,art_title as title,pub_time,collect_url,domain from info_collect filter_sql) tmp
                        where num=1) tmpB on tmpA.domain=tmpB.domain""",
                     official_account="""select tmpA.source, collect_way, continue, all_data, week_data,tmpB.title,tmpB.pub_time,tmpB.collect_url from
                        (select source,index(collect_list(collect_way),0) as collect_way,index(collect_list(continue),0) as continue,sum(cnt) as all_data,sum(week) as week_data  from (
                        select source,
                               case collect_way when 1 then 'rss' when 2 then '爬虫采集' when 6 then '爬虫采集' else '整理导入' end as collect_way,
                               case collect_way when 1 then '持续采集' when 2 then '持续采集' when 6 then '持续采集' else '非持续' end as continue,
                               1 as cnt,
                               if(cdate between week_before and new_date, 1, 0) as week
                        from info_collect filter_sql
                            ) tmp group by source) tmpA LEFT JOIN (
                        select title,pub_time,collect_url,source from (
                        select row_number() over (partition by source ORDER BY acq_time desc )as num,art_title as title,pub_time,collect_url,source from info_collect filter_sql) tmp
                        where num=1) tmpB on tmpA.source=tmpB.source""")


    table_names = {"report_collect": "报告", "policies_collect": "政策", "org_collect": "研发机构", "ent_collect": "企业",
                   "talent_collect": "人才", "project_collect": "项目", "census_collect": "统计", "market_collect": "市场",
                   "bid_collect": "招标","credential_collect":"资质奖项" ,"terms_collect":"术语词典","info_collect": "资讯",
                   "official_account": "公众号"}

    @staticmethod
    def get_hive_sql(table_name, source_inc_result, week_before, new_date):

        # mysql查询数据
        filter_sql = get_filter_sql(table_name, source_inc_result)
        hive_sql = table_all.table_sql[table_name]
        hive_sql = hive_sql.replace("filter_sql", filter_sql).replace("week_before", str(week_before)).replace(
            "new_date", str(new_date))
        return hive_sql


    @staticmethod
    def get_sheet_name(table_name):
        return table_all.table_names[table_name]

    @staticmethod
    def get_inc_source(table_name, week_before, new_date):
        if "info_collect" == table_name:
            return f"""select tmpA.domain,tmpA.source from (
                        select domain, source from (
                        select domain,source from info_collect where cdate < {week_before} and domain !='mp.weixin.qq.com' group by domain, source) tmpa)tmpA
                        right join (
                        select domain, source from (
                        select domain,source from info_collect where cdate between {week_before} and {new_date} and domain !='mp.weixin.qq.com' group by domain, source)tmpb) tmpB on tmpA.source=tmpB.source
                        where tmpA.source IS NULL"""
        elif "official_account" ==  table_name:
            return f"""select tmpA.domain,tmpA.source from (
                                    select domain, source from (
                                    select domain,source from info_collect where cdate < {week_before} and domain='mp.weixin.qq.com' group by domain, source) tmpa)tmpA
                                    right join (
                                    select domain, source from (
                                    select domain,source from info_collect where cdate between {week_before} and {new_date} and domain='mp.weixin.qq.com' group by domain, source)tmpb) tmpB on tmpA.source=tmpB.source
                                    where tmpA.source IS NULL"""
        else:
            return f"""select tmpA.domain,tmpA.source from (
                    select domain, source from (
                    select domain,source from {table_name} where cdate < {week_before} group by domain, source) tmpa)tmpA
                    right join (
                    select domain, source from (
                    select domain,source from {table_name} where cdate between {week_before} and {new_date} group by domain, source)tmpb) tmpB on tmpA.domain=tmpB.domain
                    where tmpA.domain IS NULL"""


# 写出excel
def write_excel(book, table_name, week_before, new_date, result_data, source_inc_result , all_index, sheet_0):

    sheet_name = table_all.get_sheet_name(table_name)

    sheet = book.add_sheet(sheet_name, cell_overwrite_ok=True)

    col = ('数据源','域名','采集方式','是否持续','总数',f'{week_before}~{new_date}增长','标题','发布时间','采集地址')

    col1 = ('数据源','采集方式','是否持续','总数',f'{week_before}~{new_date}增长','标题','发布时间','采集地址')

    filter_tables = ['policies_collect', 'report_collect', 'org_collect', 'info_collect', 'credential_collect']

    if table_name in filter_tables:
        for i in range(0, len(col)):
            sheet.write(0, i, col[i])
    else:
        for i in range(0, len(col1)):
            sheet.write(0, i, col1[i])

    count_all = 0
    count_inc = 0
    row_index = 1
    if table_name in filter_tables:
        for row in result_data:
            for j in range(0, len(col)):

                value = row[j]
                if (j == 4):
                    count_all += int(value)
                if (j == 5):
                    count_inc += int(value)
                sheet.write(row_index, j, value)
            row_index += 1
    else:
        for row in result_data:
            for j in range(0, len(col1)):
                value = row[j]
                if (j == 3):
                    count_all += int(value)
                if (j == 4):
                    count_inc += int(value)
                sheet.write(row_index, j, value)
            row_index += 1

    inc_domains = []
    inc_sources = []
    if len(source_inc_result) > 0:
        for row in source_inc_result:
            row_domain = row[0]
            row_source = row[1]
            if row_domain is not None:
                inc_domains += row_domain
            if row_source is not None:
                inc_sources += row_source

    sheet_row = [sheet_name, count_all, count_inc, len(inc_domains), str(inc_domains), str(inc_sources)]
    for i in range(0, len(sheet_row)):
        sheet_0.write(all_index, i, sheet_row[i])

    all_index += 1
    logger.info(f"Excel-{sheet_name} 添加成功!")


if __name__ == '__main__':

    date = input("请输入计算日期: ")

    week_before, new_date = get_before_week(date)

    excel_path = input("请输入excel生成路径: ")
    # excel_path = "E:\data\锐寻数据总结1.xlsx"

    hive_conn = myhive.HiveConnector(host='hadoop8', port=10000, username='root', password='hive',
                                     database='special_database')
    hive_conn.connect()

    all_index = 1
    # table_names = ['org_collect', 'report_collect', 'policies_collect', 'ent_collect', 'talent_collect', 'project_collect',
    #              'census_collect', 'market_collect', 'bid_collect', 'credential_collect', 'terms_collect', 'info_collect',
    #              'official_account']

    table_names = ['info_collect']

    book = xlwt.Workbook(encoding='utf-8', style_compression=0)
    sheet_0 = book.add_sheet("总览", cell_overwrite_ok=True)

    col_0 = ('类型', '总览', f'{week_before}~{new_date}数据增长', f'{week_before}~{new_date}数据源增长', '新增域名', '新增数据源')

    for i in range(0, len(col_0)):
        sheet_0.write(0, i, col_0[i])


    for table_name in table_names:
        # 先查询是否增加数据源
        source_inc_sql = table_all.get_inc_source(table_name, week_before, new_date)

        # 查询新增数据源
        logger.info(f'hive 查询 {table_name} 新增数据源中·····')
        source_inc_result = hive_conn.execute_query(source_inc_sql)
        logger.info(f'hive 查询 {table_name} 新增数据源完毕!!!')

        hive_sql = table_all.get_hive_sql(table_name, source_inc_result, week_before, new_date)

        logger.info(f"hive 查询中 {table_name} 数据中·····")
        result = hive_conn.execute_query(hive_sql)
        logger.info(f"hive 查询完毕 {table_name} 数据完毕！！！")

        write_excel(book, table_name, week_before, new_date, result, source_inc_result, all_index, sheet_0)
        all_index += 1

    book.save(excel_path)
    hive_conn.close_conn()