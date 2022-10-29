from datetime import datetime
import json
from bs4 import BeautifulSoup
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client
from infra.logger import get_logger
import requests


class QusQtyExtractor :

    FILE_DIR = '/qus_qty/'
    FILE_NAME = 'qus_qty_'+ cal_std_day(0)+'.json'
    URL = 'https://kin.naver.com/qna/expertAnswerList.naver?dirId=701'
    HEADERS = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}

    @classmethod
    def extract_data(cls):
        cols = ['cnt_qus','cnt_ans']
        data = []
        rows = []
        res = requests.get(cls.URL, headers = cls.HEADERS)
        bs = BeautifulSoup(res.text, 'html.parser')
        dds = bs.find('div',{'id':'content'}).find('div',{'class':'ques_cnt'}).findAll('dd')
        # 오늘의 질문 + 모바일 질문
        cnt_qus = int(dds[0].text.replace(',','')) + int(dds[1].text.replace(',',''))
        # 오늘의 답변개수
        cnt_ans = int(dds[2].text.replace(',',''))
        rows.append(cnt_qus)
        rows.append(cnt_ans)
        tmp = dict(zip(cols,rows))
        data.append(tmp)
        res = {
            'meta':{
                'desc':'지식인 건강태그 금일 질문 개수',
                'cols':{
                        'cnt_qus':'질문개수 : 웹 + 모바일 질문','cnt_ans':'답변개수'
                },
                'std_day':cal_std_day(0)
            },
            'data':data
        }
        
        get_client().write(cls.FILE_DIR+cls.FILE_NAME,json.dumps(res,ensure_ascii=False),encoding='utf-8')

        