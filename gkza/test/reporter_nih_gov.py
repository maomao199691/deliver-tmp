import json

import requests
import scrapy
import time
import scrapy
from National_Institutes_of_Health.crawl_utils import constant
from National_Institutes_of_Health.crawl_utils import md5_util
from National_Institutes_of_Health.crawl_utils import redis_util
from National_Institutes_of_Health.items import NationalInstitutesOfHealthItem
from National_Institutes_of_Health.crawl_utils import data_util

source = '美国国立卫生研究院项目'
source_type = 'Grants & Funding_RePORT_Advanced Search_Search'
data_type = '15'


class ReporterNihGovSpider(scrapy.Spider):
    name = 'reporter_nih_gov'
    allowed_domains = ['reporter.nih.gov']
    start_url = "https://reporter.nih.gov/services/Projects/search/"
    custom_settings = {
        'LOG_FILE': constant.FILE_PATH_ENV_LOG + '{}_{}.log'.format(source, source_type)
    }
    clinical_studies_url = 'https://reporter.nih.gov/services/Projects/ClinicalStudies?projectId={}'
    publications_url = 'https://reporter.nih.gov/services/Projects/Publications?projectId={}'
    similar_projects_url = "https://reporter.nih.gov/services/Projects/SimilarProjects"
    history_url = "https://reporter.nih.gov/services/Projects/ProjectHistory"
    news_and_more_url = 'https://reporter.nih.gov/services/Projects/NewsResults'
    patents_url = 'https://reporter.nih.gov/services/Projects/Patents?projectId={}'
    sub_projects_url = "https://reporter.nih.gov/services/Projects/ProjectSubProjects"
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://reporter.nih.gov',
        # 'Referer': 'https://reporter.nih.gov/search/cb8HtnM0wEeNEoX37ez4Fg/project-details/10816033',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        # 'Cookie': 'dtCookie=v_4_srv_1_sn_E396E18D1CDA3348DCA50226D405CEF3_perc_100000_ol_0_mul_1_app-3A611b5bc1cdd55364_1; AWSALB=cRFgK6I9FDCmcg6/cujAoFEkfq4BLDXkq9CfNv088828Axo6gVOHA92CFHcLbQBnvueCjMOkPKIBwPKlXr08wN/kg5QQqQoy2/CKcbmwUBRyaTKmuRMkd+xnpPpT; AWSALBCORS=cRFgK6I9FDCmcg6/cujAoFEkfq4BLDXkq9CfNv088828Axo6gVOHA92CFHcLbQBnvueCjMOkPKIBwPKlXr08wN/kg5QQqQoy2/CKcbmwUBRyaTKmuRMkd+xnpPpT'
    }
    num = 100
    payload = "{'search_type':'Advanced','offset': %s,'limit':50,'facet_filters':{},'criteria':{'fiscal_years':'ap'},'is_shared':false,'search_id':'cb8HtnM0wEeNEoX37ez4Fg'}"

    def __init__(self):
        self.redis_con = redis_util.redis_client()
        self.now_day = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        super().__init__()

    def start_requests(self):
        # start_url = "https://reporter.nih.gov/services/Projects/search/"
        # payload = "{'search_type':'Advanced','offset': %s,'limit':50,'facet_filters':{},'criteria':{'fiscal_years':'ap'},'is_shared':false,'search_id':'cb8HtnM0wEeNEoX37ez4Fg'}"
        start_payload = self.payload % self.num
        yield scrapy.Request(self.start_url, method='POST', headers=self.headers, body=start_payload,
                             callback=self.parse_list)

    def parse_list(self, response):
        page_list = response.json()['results']
        data = dict()
        for one in page_list:
            appl_id = one['appl_id']
            data['appl_id'] = appl_id
            # 项目名称
            data['title'] = one['project_title']
            # 项目编号
            data['project_num'] = one['full_project_num']
            data['link'] = f'https://reporter.nih.gov/search/cb8HtnM0wEeNEoX37ez4Fg/project-details/{appl_id}'
            url = f'https://reporter.nih.gov/services/Projects/ProjectInfo?projectId={appl_id}&searchId=cb8HtnM0wEeNEoX37ez4Fg'
            # 判断是否爬过
            uuid = md5_util.get_md5(url)
            # uuid_key = f"uuid_{source}_{source_type}"
            # judge = self.redis_con.sismember(uuid_key, uuid)
            # if judge is True:
            #     continue
            # self.redis_con.sadd(uuid_key, uuid)
            data['uuid'] = uuid
            yield scrapy.Request(url, cb_kwargs=data, callback=self.parce_details)
        if page_list:
            self.num += 50
            next_payload = self.payload % self.num
            yield scrapy.Request(self.start_url, method='POST', headers=self.headers, body=next_payload,
                                 callback=self.parse_list)

    def get_history(self, appl_id):
        history = list()
        url = self.history_url
        payload = "{'project_id': %s,'sort_field':'FiscalYear','sort_order':'desc','offset':0}" % appl_id
        response = requests.request("POST", url, headers=self.headers, data=payload)
        history_detail = response.json()['results']
        if history_detail:
            for i in history_detail:
                data = dict()
                data['title'] = i['project_title']
                data['project_number'] = i['full_project_num']
                project_leader = i['pi_info']
                if project_leader:
                    data['project_leader'] = project_leader[0]['name']
                data['organization'] = i['org_name']
                data['fiscal_year'] = i['fiscal_year']
                data['admin_ic'] = i['admin_icd_long_abbr']
                funding_ic = i['funding']
                if funding_ic:
                    data['funding_ic'] = funding_ic[0]['ic']
                data['fy_total_cost_by_ic'] = i['total_award_amount']
                history.append(data)
            return history

    def get_similar_projects(self, appl_id):
        similar_projects = list()
        url = self.similar_projects_url
        payload = "{'project_id': %s,'sort_field':'match_score','sort_order':'desc','offset':0}" % appl_id
        response = requests.request("POST", url, headers=self.headers, data=payload)
        similar_projects_detail = response.json()['results']
        if similar_projects_detail:
            for i in similar_projects_detail:
                data = dict()
                data['title'] = i['project_title']
                data['match_score'] = i['match_score']
                data['project_number'] = i['full_project_num']
                project_leader = i['pi_info']
                if project_leader:
                    data['project_leader'] = project_leader[0]['name']
                data['organization'] = i['org_name']
                data['fiscal_year'] = i['fiscal_year']
                data['admin_ic'] = i['admin_icd_long_abbr']
                funding_ic = i['funding']
                if funding_ic:
                    data['funding_ic'] = funding_ic[0]['ic']
                data['fy_total_cost_by_ic'] = i['total_award_amount']
                similar_projects.append(data)
            return similar_projects

    def get_publications(self, appl_id):
        publications = list()
        url = self.publications_url.format(appl_id)
        response = requests.request("GET", url, headers=self.headers)
        publications_detail = response.json()['results']
        if publications_detail:
            for i in publications_detail:
                data = dict()
                similar_publications = list()
                citedby = list()
                data['title'] = i['pub_title']
                journal_title = i['journal_title'] if i['journal_title'] else ''
                journal_volume = i['journal_volume'] if i['journal_volume'] else ''
                journal_issue = i['journal_issue'] if i['journal_issue'] else ''
                page_number = i['page_number'] if i['page_number'] else ''
                pub_date = i['pub_date'] if i['pub_date'] else ''
                data[
                    'journal'] = journal_title + pub_date + ";" + journal_volume + "(" + journal_issue + ")" + page_number
                data['author'] = i['author_list']
                data['publication_year'] = i['pub_year']
                related_pub_med_links = i['related_pub_med_links']
                similar_publications.append(related_pub_med_links)
                related_pub_med_google = i['related_pub_med_google']
                similar_publications.append(related_pub_med_google)
                data['similar_publications'] = similar_publications
                article_pub_med_central = i['article_pub_med_central']
                citedby.append(article_pub_med_central)
                article_citing_google = i['article_citing_google']
                citedby.append(article_citing_google)
                data['citedby'] = citedby
                data['icite_rcr'] = i['relative_citation_ratio']
                publications.append(data)
            return publications

    def get_clinical_studies(self, appl_id):
        clinical_studies = list()
        url = self.clinical_studies_url.format(appl_id)
        response = requests.request("GET", url, headers=self.headers)
        clinical_studies_detail = response.json()['results']
        if clinical_studies_detail:
            for i in clinical_studies_detail:
                data = dict()
                data['Core_NIH_Project_Number'] = i['core_project_num']
                data['ClinicalTrials.gov_ID'] = i['nct_id']
                data['study'] = i['study_title']
                data['study_status'] = i['study_status']
                clinical_studies.append(data)
            return clinical_studies

    def get_news(self, appl_id):
        payload = "{'project_id': %s,'Newstype':'PressRelease','grant_Num_Or_Full_Proj_Num':'NS107671','limit':50}" % appl_id
        news_and_more = list()
        url = self.news_and_more_url
        response = requests.request("POST", url, headers=self.headers, data=payload)
        news_detail = response.json()['results']
        if news_detail:
            for i in news_detail:
                data = dict()
                data['news'] = i['description']
                release_date = i['date_added']
                data['release_date'] = release_date.replace('T', ' ').replace('Z', '') if release_date else ''
                data['journal_article'] = i['journl_link']
                data['pubMed_abstract'] = i['pub_med_link']
                news_and_more.append(data)
            return news_and_more

    def get_patents(self, appl_id):
        patents = list()
        url = self.patents_url.format(appl_id)
        response = requests.request("GET", url, headers=self.headers)
        patents_detail = response.json()['results']
        if patents_detail:
            for i in patents_detail:
                data = dict()
                data['patent_number'] = i['patent_id']
                data['patent_title'] = i['patent_title']
                data['patent_owner'] = i['patent_org_name']
                data['primary_agency'] = i['primary_agency']
                patents.append(data)
            return patents

    def get_related_nih_research_matters(self, appl_id):
        related_nih_research_matters = list()
        payload = "{'project_id': %s,'Newstype':'ResearchMatters','grant_Num_Or_Full_Proj_Num':'EY001792','limit':50}" % appl_id
        url = self.news_and_more_url
        response = requests.request("POST", url, headers=self.headers, data=payload)
        research_matters_detail = response.json()['results']
        if research_matters_detail:
            for i in research_matters_detail:
                data = dict()
                data['news'] = i['description']
                data['pubMed_abstract'] = i['pub_med_link']
                data['release_date'] = i['date_added'].replace('T', ' ').replace('Z', '')
                related_nih_research_matters.append(data)
            return related_nih_research_matters

    def get_sub_projects(self, appl_id):
        sub_projects = list()
        payload = "{'project_id': %s,'sort_field':'','sort_order':'asc','offset':0}" % appl_id
        url = self.sub_projects_url
        response = requests.request("POST", url, headers=self.headers, data=payload)
        sub_projects_detail = response.json()['results']
        if sub_projects_detail:
            for i in sub_projects_detail:
                data = dict()
                data['title'] = i['project_title']
                data['project_number'] = i['full_project_num']
                data['sub'] = i['subproject_id']
                data['organization'] = i['org_name']
                data['fiscal_year'] = i['fiscal_year']
                data['fy_total_cost_by_ic'] = i['total_award_amount']
                funding = i['funding']
                if funding:
                    data['admin_ic'] = funding[0]['ic']
                sub_projects.append(data)
            return sub_projects

    def parce_details(self, response, **kwargs):
        ext = dict()
        detail = response.json()
        # 项目条款
        ext['pref_terms'] = detail['pref_terms']
        # 公共卫生相关性声明
        public_health_relevance_statement = detail['phr_text']
        ext['public_health_relevance_statement'] = public_health_relevance_statement.replace('\n',
                                                                                             ' ') if public_health_relevance_statement else ''
        # 项目描述
        abstract_text = detail['abstract_text']
        description = abstract_text.replace('\n', ' ') if abstract_text else ''
        # 支出类别
        spending_category = detail['spending_categories']
        if spending_category:
            ext['nih_spending_category'] = ';'.join(spending_category)

        # 获奖者_组织
        ext['awardee_organization'] = detail['org_name']
        # 项目负责人
        undertake_leader = list()
        contact_pi = dict()
        contact_pi['name'] = detail['contact_pi']['full_name']
        contact_pi['title'] = detail['contact_pi']['title']
        contact_pi['contact'] = detail['contact_pi']['email']
        undertake_leader.append(contact_pi)
        # 其他
        pis = detail['others_pis']
        others_pis_name = ''
        for i in pis:
            others_pis_name += i['full_name'] + ','
            ext['others_pis-name'] = others_pis_name

        # 项目官员
        ext['program_official-name'] = detail['po_name']
        # 项目承担机构
        undertake_org = list()
        organization = dict()
        organization['org_name'] = detail['organization']['org_name']
        organization['city'] = detail['organization']['org_city']
        organization['country'] = detail['organization']['org_country']
        organization['org_type'] = detail['org_type_name']
        organization['cong_dist'] = detail['cong_dist']
        undertake_org.append(organization)
        # 其他信息

        ext['other_information-foa'] = detail['full_foa']
        other_information_study_section = detail['full_study_section']
        if other_information_study_section:
            ext['other_information-study_section'] = other_information_study_section['name']
        ext['other_information-fiscal_year'] = detail['fiscal_year']
        ext['other_information-cfda_code'] = detail['cfda_code']
        ext['other_information-DUNS_number'] = detail['organization']['primary_duns']
        ext['other_information-UEI'] = detail['organization']['primary_uei']
        other_information_budget_start_date = detail['budget_start']
        ext['other_information-Budget_Start_Date'] = other_information_budget_start_date.replace('T', ' ').replace('Z', '') if other_information_budget_start_date else ''
        other_information_budget_end_date = detail['budget_end']
        ext['other_information-Budget_End_Date'] = other_information_budget_end_date.replace('T', ' ').replace('Z',
                                                                                                                   '') if other_information_budget_end_date else ''
        # 项目融资信息
        ext['project_funding_information-total_funding'] = detail['award_amount']
        ext['project_funding_information-direct_costs'] = detail['direct_cost_amt']
        ext['project_funding_information-indirect_costs'] = detail['indirect_cost_amt']
        ext['project_funding_information-agency_ic_fundings'] = detail['agency_ic_fundings']
        categorical_spending = detail['categorical_spending']
        if categorical_spending:
            ext['nih_categorical_spending_funding_ic'] = categorical_spending[0]['funding_ic']
            ext['nih_categorical_spending_fy_total_cost_by_ic'] = categorical_spending[0]['total_cost']
            ext['nih_categorical_spending'] = ';'.join(categorical_spending[0]['spending_categories'])

        # 开始时间
        project_start_date = detail['project_start_date'].replace('T', ' ').replace('Z', '')
        start_date = int(data_util.time_timestamp(project_start_date))
        # 结束时间
        project_end_date = detail['project_end_date'].replace('T', ' ').replace('Z', '')
        end_date = int(data_util.time_timestamp(project_end_date))
        ext['similar_projects'] = self.get_similar_projects(kwargs['appl_id'])
        ext['get_history'] = self.get_history(kwargs['appl_id'])
        ext['clinical_studies'] = self.get_clinical_studies(kwargs['appl_id'])
        ext['publications'] = self.get_publications(kwargs['appl_id'])
        ext['news_and_more'] = self.get_news(kwargs['appl_id'])
        ext['patents'] = self.get_patents(kwargs['appl_id'])
        ext['related_nih_research_matters'] = self.get_related_nih_research_matters(kwargs['appl_id'])
        ext['sub_projects'] = self.get_sub_projects(kwargs['appl_id'])

        item = NationalInstitutesOfHealthItem()
        item['source'] = source
        item['source_type'] = source_type
        item['data_type'] = data_type
        item['description'] = description
        item['undertake_leader'] = undertake_leader
        item['undertake_org'] = undertake_org
        item['start_date'] = start_date
        item['end_date'] = end_date
        item['ext'] = json.dumps(ext, ensure_ascii=False)
        item['title'] = kwargs['title']
        item['project_num'] = kwargs['project_num']
        item['link'] = kwargs['link']
        item["uuid"] = kwargs['uuid']
        item["project"] = f'{source}_{source_type}'
        item["source_content"] = response.text.replace('\n', '').replace('\r', '').replace('<br>', '').replace('<br />',
                                                                                                               '').replace(
            '"', '').replace('\\', '').strip()
        yield item


from scrapy.cmdline import execute

if __name__ == '__main__':
    execute(["scrapy", "crawl", "reporter_nih_gov"])
