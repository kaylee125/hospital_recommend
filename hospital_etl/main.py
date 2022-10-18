#!/usr/bin/env python3
"""
Module Docstring
"""

__author__ = "Your Name"
__version__ = "0.1.0"
__license__ = "MIT"
import sys

from datajob.extract.naver_jisik import NaverJisikExtractor




def extract_execute():
    NaverJisikExtractor.extract_data()

# def extract_execute_monthly():

# def transform_execute():

# def transform_execute_monthly():


def main(extract_execute):
    works = {
        'extract':{
            'execute_daily':extract_execute
            , 'naver_jisik': NaverJisikExtractor.extract_data

        }
        , 'transform':{
            # 'execute_daily' : transform_execute

        }

    }
    
    return works

works = main(extract_execute)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    args = sys.argv

    print(args)

    # main.py 작업(extract, transform, datamart) 저장할 위치(테이블, 작업)
    # 매개변수 2개
    
    if len(args) != 3:
        raise Exception('2개의 전달인자가 필요합니다.')
    
    if args[1] not in works.keys() :
        raise Exception("첫번째 전달인자가 이상함 >> " +str(works.keys()))
    if args[2] not in works[args[1]].keys() :
        raise Exception("두번째 전달인자가 이상함 >> " +str(works[args[1]].keys()))
        # print(work)
        # <bound method CoronaVaccineExtractor.extract_data of <class 'datajob.etl.extract.corona_vaccine.CoronaVaccineExtractor'>>
    
    work = works[args[1]][args[2]]
    work()