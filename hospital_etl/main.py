#!/usr/bin/env python3
"""
Module Docstring
"""

__author__ = "Your Name"
__version__ = "0.1.0"
__license__ = "MIT"
import sys
from datajob.datamart.exact_dpt_answer import ExactDptAnswer
from datajob.datamart.rec_dpt import RecDermaSave, RecENTSave, RecInternalMedicineSave, RecNeuroSave, RecORTSave, RecObstetricsSave, RecOphthalSave, RecUrologySave

from datajob.extract.naver_jisik import NaverJisikExtractor
from datajob.transform.find_no_symt_code_tf import FindNoSymtCode
from datajob.transform.subjective_tf import SubjectiveTextTransformer
from datajob.transform.objectiv_tf import ObjectiveTextTransformer
from datajob.datamart.objective_each import ObjSymptom
from datajob.extract.naver_doc import DoctorInfoExtarctor
from datajob.transform.naver_doc_tf import DocInfoTransformer
from datajob.extract.naver_jisik_ver2 import NaverJisikExtractor2
from datajob.datamart.qty_eachgwa import QtyEachDpt
from datajob.datamart.answer_delay import AnswerDelay
from datajob.transform.test1 import SubjectiveTextVer
from datajob.transform.write_obj_symt import WriteObjSymtTransfomer
from infra.rawdata_upload import FileUpload




def extract_execute():
    NaverJisikExtractor.extract_data()

# def extract_execute_monthly():

def transform_execute():
    SubjectiveTextTransformer.transform()
    ObjectiveTextTransformer.transform()
    
# def transform_execute_monthly():

def local_upload():
    FileUpload.upload()

def main(extract_execute):
    works = {
        'upload' : {
            'hdfs_upload' : FileUpload.upload
        }
        , 'extract':{
            'execute_daily':extract_execute
            , 'naver_jisik': NaverJisikExtractor.extract_data
            , 'naver_doc' : DoctorInfoExtarctor.extract_data
            , 'naver_jisik_ver2': NaverJisikExtractor2.extract_data

        }
        , 'transform':{
            'subjective_tf': SubjectiveTextTransformer.transform
            , 'objective_tf' : ObjectiveTextTransformer.transform
            , 'naver_doc_tf' : DocInfoTransformer.transform
            , 'find_no_symt_code_tf': FindNoSymtCode.transform
            , 'write_obj_symt': WriteObjSymtTransfomer.transform
            , 'test1': SubjectiveTextVer.transform
        }
        , 'save':{
            'obj_sypmtom' : ObjSymptom.save
            , 'qty_eachgwa' : QtyEachDpt.save
            , 'answer_delay' : AnswerDelay.save
            , 'exact_dpt_answer' : ExactDptAnswer.save
            , 'int_med_rec' : RecInternalMedicineSave.save
            , 'ort_rec' : RecORTSave.save
            , 'obstetrics_rec' : RecObstetricsSave.save
            , 'derma_rec' : RecDermaSave.save
            , 'ent_rec' :RecENTSave.save
            , 'neuro_rec' : RecNeuroSave.save
            , 'urology_rec' : RecUrologySave.save
            , 'ophthal_rec' : RecOphthalSave.save
            
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