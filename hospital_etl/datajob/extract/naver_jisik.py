from datetime import datetime
import json
from bs4 import BeautifulSoup
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client
from infra.logger import get_logger
import requests


class NaverJisikExtractor :

    FILE_DIR = '/naver_jisik/'
    FILE_NAME = 'qus_ans_'+ cal_std_day(0)+'.json'
    BASE_URL = ['https://kin.naver.com/qna/expertAnswerList.naver?dirId=701&queryTime=','https://kin.naver.com/']
    PARAM = "%2014%3A51%3A20&page="
    HEADERS = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}

    @classmethod
    def extract_data(cls):
        cols = ['qus_title','qus_dir','qus_doc_id','qus_doc','qus_time','ans_time','qus_content','qus_answer']
        data = []
        for i in range(1,100):
            
            url = cls.BASE_URL[0] + cal_std_day(0) + cls.PARAM + str(i)
            res = requests.get(url, headers = cls.HEADERS)
            bs = BeautifulSoup(res.text, 'html.parser')
            # 질문 리스트
            trs = bs.find('tbody',{'id':'au_board_list'}).findAll('tr')
            for tr in trs :
                rows =[] 
                if tr.find('td',{'class':'t_num'}).text[:3] != '202' :
                    # 질문 URL
                    qus_url = tr.find('td',{'class':'title'}).find('a')['href']
                    # 질문 제목 
                    rows.append(tr.find('td',{'class':'title'}).find('a').text)
                    # 분야
                    rows.append(tr.find('td',{'class':'field'}).find('a').text)
                    # 답변의사 아이디
                    rows.append(tr.find('td',{'class':'questioner'}).find('a')['href'].split('=')[1])
                    # 의사이름
                    rows.append(tr.find('td',{'class':'questioner'}).find('a').text)
                    q_url = cls.BASE_URL[1] + qus_url
                    q_res = requests.get(q_url, headers = cls.HEADERS)
                    q_bs = BeautifulSoup(q_res.text, 'html.parser')
                    # 질문시간

                    qus_time = q_bs.find('div',{'class':'c-userinfo__left'}).find('span',{'class':'c-userinfo__info'}).text.replace('작성일','').replace('끌올 ','')
                    if '분' in qus_time :
                        rows.append(int(qus_time.replace('분 전','')))
                    elif '시간' in qus_time :
                        rows.append(int(qus_time.replace('시간 전',''))*60)
                    else :
                        rows.append(0)
                    
                    # 답변시간
                    ans_time = q_bs.find('div',{'id':'answerArea'}).find('p',{'class':'c-heading-answer__content-date'}).text.replace('작성일','')
    
                    if '분' in ans_time :
                        rows.append(int(ans_time.replace('분 전','')))
                    elif '시간' in ans_time :
                        rows.append(int(ans_time.replace('시간 전',''))*60)
                    else :
                        rows.append(0)
                    # 질문내용
                    try :
                        rows.append(q_bs.find('div',{'class':'c-heading__content'}).text.replace('\n','').replace('\t',''))
                    except :
                        rows.append('No Content')
                    temp_ls = []
                    ps = q_bs.find('div',{'class':'se-main-container'})
                    ps_1 = q_bs.find('div',{'class':'_endContentsText c-heading-answer__content-user'})
                    try:
                        for p in ps.findAll('p'):
                            p_content = p.find('span').text
                            if p_content == ' ' :
                                continue
                            else :
                                temp_ls.append(p_content.replace('\u200b','').replace('\xa0',''))
                        rows.append(' '.join(temp_ls))
                        tmp = dict(zip(cols,rows))
                        data.append(tmp)
                    except :
                        for p in ps_1.findAll('p'):
                            p_content = p.text
                            if p_content == ' ' :
                                continue
                            else :
                                temp_ls.append(p_content.replace('\u200b','').replace('\xa0',''))
                        
                        rows.append(' '.join(temp_ls))
                        tmp = dict(zip(cols,rows))
                        data.append(tmp)


        res = {
            'meta':{
                'desc':'지식인 주관적 텍스트 추출',
                'cols':{
                    'qus_title':'질문제목','qus_dir':'카테고리','qus_doc_id':'의사_고유번호','qus_doc':'의사이름',
                    'qus_time':'질문시간', 'ans_time':'답변시간','qus_content':'질문내용','qus_answer':'답변'
                },
                'std_day':cal_std_day(0)
            },
            'data':data
        }
                    
        get_client().write(cls.FILE_DIR+cls.FILE_NAME,json.dumps(res,ensure_ascii=False),encoding='utf-8')