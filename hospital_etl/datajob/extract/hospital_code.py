from datetime import datetime
import json
from bs4 import BeautifulSoup
from infra.spark_session import get_spark_session
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client
from infra.logger import get_logger
import pandas as pd
from pandas import DataFrame
import requests


class HospitalCode:
    KEY='ecfbc592d2a441f9b2170a51efd15e15'
    FILE_DIR = '/hospital_code/'
    FILE_NAME = 'hospital_code'+ cal_std_day(0)
    BASE_URL = 'https://openapi.gg.go.kr/AsembyStus?'
    PARAMS = {'KEY' : KEY,'Type':'json','pIndex':'1','pSize':'5'}
    #HEADERS = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}


    @classmethod
    def extract_data(cls):
        response = requests.get(cls.BASE_URL, cls.PARAMS)
        bs = BeautifulSoup(response.content, 'lxml')
        bs_str=bs.findAll('p')[0].text
        bs_dict = json.loads(bs_str.replace("'", "\"")) # json 모듈이 큰 따옴표만 인식하기 때문에 작은따옴표를 큰따옴표로 변환
        #for문 반복을 위한 row의 개수 구하기
        len_row=len(bs_dict['AsembyStus'][1:][0]['row'])
        row=[]
        

        for i in range(len_row):
            c=bs_dict['AsembyStus'][1:][0]['row'][i]
            row.append(c)
            # data.append([sp_price[k].text.strip(), tmp_date, sp_area[k].text, sp_localcode[k].text])
            cls.__write_to_csv(row)
        
    @classmethod
    def __write_to_csv(cls,row):
        df=pd.DataFrame(row)
        file_name = cls.FILE_DIR + cls.FILE_NAME +'.csv'
        with get_client().write(file_name, overwrite=True, encoding='utf-8') as writer:
            df.to_csv(writer, index=False)
    


