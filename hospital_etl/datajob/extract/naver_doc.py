from datetime import datetime
import json
from bs4 import BeautifulSoup
from infra.util import execute_rest_api,cal_std_day
from infra.hdfs_client import get_client
from infra.logger import get_logger


class DoctorInfoExtarctor:
    FILE_DIR = '/naver_doc/'
    FILE_NAME = 'doctor_info_' + cal_std_day(0) + '.json'
    HEADERS = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"}
    URL = ['https://kin.naver.com/people/expert/index.naver?type=DOCTOR','https://kin.naver.com/people/expert/index.naver?type=DOCTOR&page=' ,
            'https://kin.naver.com/userinfo/expert/index.naver?u=']

    @classmethod
    def extract_data(cls):
        # 의사의 페이지수 구하기
        # 의사수
        doc_param = cls.__get_registered_doc_num()
        res = cls.__get_doc_info(doc_param)
        cls.__wright_to_hdfs(res)






    @classmethod
    def __wright_to_hdfs(cls, res):
        return get_client().write(cls.FILE_DIR+cls.FILE_NAME,json.dumps(res,ensure_ascii=False),encoding='utf-8')


    @classmethod
    def __get_doc_info(cls, doc_param):
        cols = ['doc_id','doc_name','is_pro','doc_pos','naver_grade']
        data = []
        for param in doc_param:
            rows = []
            url = cls.URL[2] + param
            bs4 = BeautifulSoup(execute_rest_api('get',url,cls.HEADERS,{}), 'html.parser')
            #의사 id => key
            rows.append(param)
            print(url)
            #의사이름
            try :
                rows.append(bs4.find('div',{'class':'profile_section2'}).find('dt').text)
                # 의사 분야
                try : 
                    doc_dpt=bs4.find('div',{'class':'profile_section2'}).find('span',{'class':'p_doctor'}).text.split(' ')
                    # 전문의 여부 
                    if doc_dpt[1] == '전문의':
                        rows.append(1)
                    else :
                        rows.append(0)
                except : 
                    doc_dpt=bs4.find('div',{'class':'profile_section2'}).find('span',{'class':'p_doctor'}).text
                    if doc_dpt == '전문의':
                        rows.append(1)
                    else :
                        rows.append(0)
                # 의사 근무지 및 직책
                try:
                    doc_pos = bs4.find('div',{'class':'profile_section2'}).find('p',{'class':'position'}).text
                    if doc_pos[-1] == ' ' :
                        rows.append(doc_pos[:-1])
                    else :
                        rows.append(doc_pos)
                except :
                    rows.append(None)
                try :
                    # 지식인 등급
                    rows.append(bs4.find('div',{'class':'profile_section3'}).findAll('dd')[2].text)
                    tmp = dict(zip(cols,rows))
                    data.append(tmp)
                except:
                    rows.append(None)
                    tmp = dict(zip(cols,rows))
                    data.append(tmp)

            except :
                rows.append(None)
                rows.append(None)
                rows.append(None)
                rows.append(None)
                
        res = {
            'meta':{
                'desc':'지식인 의사 정보',
                'cols':{
                    'doc_id':'의사_파라미터',
                    'doc_name': '의사이름',
                    'is_pro': '전문의 여부 1:전문의 2:전문의 아니거나 치과의사',
                    'doc_pos': '의사근무병원, 직책',
                    'naver_grade' : '네이버지식인 등급'
                },
                'std_day':cal_std_day(0),
            },
            'data':data
        }
        
        return res

    @classmethod
    def __get_registered_doc_num(cls):
        bs4 = BeautifulSoup(execute_rest_api('get',cls.URL[0],cls.HEADERS,{}), 'html.parser')
        doc_count = int(bs4.find('div',{'class':'list_summary'}).find('strong').text.replace('명',''))
        page_count = round(doc_count/10)

        doc_param = []
        for i in range(1,page_count+1) :
            url = cls.URL[1] + str(i)
            bs4 = BeautifulSoup(execute_rest_api('get',url,cls.HEADERS,{}), 'html.parser')
            lis = bs4.find('ul',{'class':'pro_list'}).findAll('li')
            for li in lis :
                # 의사의 고유번호
                doc_param.append(li.find('dt').a['href'].split('=')[1])
        return doc_param
