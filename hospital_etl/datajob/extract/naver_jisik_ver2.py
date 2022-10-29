from datetime import datetime
import json
from bs4 import BeautifulSoup
from infra.spark_session import get_spark_session
from infra.jdbc import DataWarehouse,find_data
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client
from infra.logger import get_logger
import requests



class NaverJisikExtractor2 :

    doc_list = find_data(DataWarehouse, 'DOC_DATA').toPandas()
    FILE_DIR = '/naver_jisik/'
    FILE_NAME = 'qus_ans_'+ cal_std_day(0)+'.json'
    BASE_URL = ['https://kin.naver.com/userinfo/expert/answerList.naver?u=','https://kin.naver.com/']
    HEADERS = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}

    @classmethod
    def extract_data(cls):
        cols = ['qus_title','qus_dir','qus_doc_id','qus_doc','qus_time','ans_time','qus_content','qus_answer']
        data = []
        
        # 데이터 크롤링
        res = cls.__crawl_data(cols, data)
    
        cls.__write_to_hdfs(res)




    @classmethod
    def __write_to_hdfs(cls, res):
        return get_client().write(cls.FILE_DIR+cls.FILE_NAME,json.dumps(res,ensure_ascii=False),encoding='utf-8')

    @classmethod
    def __crawl_data(cls, cols, data):
        for idx, param in enumerate(cls.doc_list['DOC_ID']):
            url = cls.BASE_URL[0] + param
            res = requests.get(url, headers = cls.HEADERS)
            bs = BeautifulSoup(res.text, 'html.parser')
            #오늘의 답변 개수
            # 전문가가 탈퇴 했을 경우
            try :
                qus_qty = int(bs.find('div',{'id':'content'}).findAll('dd')[2].text)
            except:
                qus_qty = 0
            if str(qus_qty) == '0' : 
                pass
            else :
                #오늘의 답변으로 보는 페이지개수
                qus_page =  int(qus_qty/20)+1
                for page in range(1,qus_page+1) :
                    url = url + '&page=' + str(page)
                    res = requests.get(url, headers = cls.HEADERS)
                    bs = BeautifulSoup(res.text, 'html.parser')

                    trs = bs.find('tbody',{'id':'au_board_list'}).findAll('tr')
                    for tr in trs :
                        rows =[] 
                        if tr.find('td',{'class':'t_num'}).text[:-1] == cal_std_day(0).replace('-','.') :
                            # 질문 URL
                            qus_url = tr.find('td',{'class':'title'}).find('a')['href']
                            # 질문 제목 
                            rows.append(tr.find('td',{'class':'title'}).find('a').text)
                            # 분야
                            rows.append(tr.find('td',{'class':'field'}).find('a').text)
                            # 답변의사 아이디
                            rows.append(param)
                            # 의사이름
                            rows.append(cls.doc_list['DOC_NAME'][idx])
                            q_url = cls.BASE_URL[1] + qus_url
                            q_res = requests.get(q_url, headers = cls.HEADERS)
                            q_bs = BeautifulSoup(q_res.text, 'html.parser')
                            # 질문시간
                            # 게시물이 비공개처리된경우
                            try :
                                print(q_url)
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
                                    rows.append(q_bs.find('div',{'class':'c-heading__content'}.text.replace('\n','').replace('\t','')))
                                except :
                                    rows.append('No Content')
                                print(rows)
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
                            except:
                                rows.append(0)
                                rows.append(0)
                                rows.append("비공개 처리된 게시물")


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
        
        return res