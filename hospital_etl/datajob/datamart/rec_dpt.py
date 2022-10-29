from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil
import pandas as pd
import time

class RecInternalMedicineSave:
    """
    내과
    """
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']
    df_sub = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY') == cal_std_day(0))


    @classmethod
    def save(cls):
        df_qus = cls.df_sub.select(col('QUS_DOC'),col('QUS_CONTENT'),col('QUS_ANSWER').cast('string')). \
                where(col('QUS_CONTENT') != 'No Content')
        pd_qus = df_qus.toPandas()
        
        for idx, qus in enumerate(pd_qus['QUS_ANSWER']):
            if pd_qus['QUS_DOC'][idx] in qus :
                pd_qus['QUS_ANSWER'][idx] = qus.split(pd_qus['QUS_DOC'][idx])[1]

        df_qus = get_spark_session().createDataFrame(pd_qus.drop(columns='QUS_DOC'))
        df_qus =  df_qus.filter(df_qus.QUS_ANSWER.like('%' + cls.dpt_list[0] + '%'))
        
        save_data(DataMart, df_qus.limit(int(df_qus.count())-1), 'SUB_INT_MED')

class RecORTSave:
    """
    정형외과
    """
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']
    df_sub = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY') == cal_std_day(0))



    @classmethod
    def save(cls):
        df_qus = cls.df_sub.select(col('QUS_DOC'),col('QUS_CONTENT'),col('QUS_ANSWER').cast('string')). \
                where(col('QUS_CONTENT') != 'No Content')
        pd_qus = df_qus.toPandas()
        
        for idx, qus in enumerate(pd_qus['QUS_ANSWER']):
            if pd_qus['QUS_DOC'][idx] in qus :
                pd_qus['QUS_ANSWER'][idx] = qus.split(pd_qus['QUS_DOC'][idx])[1]
                
        df_qus = get_spark_session().createDataFrame(pd_qus.drop(columns='QUS_DOC'))
        df_qus =  df_qus.filter(df_qus.QUS_ANSWER.like('%' + cls.dpt_list[1] + '%'))
        save_data(DataMart, df_qus.limit(int(df_qus.count())-1), 'SUB_ORT')


class RecObstetricsSave:
    """
    산부인과
    """
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']
    df_sub = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY') == cal_std_day(0))



    @classmethod
    def save(cls):
        df_qus = cls.df_sub.select(col('QUS_DOC'),col('QUS_CONTENT'),col('QUS_ANSWER').cast('string')). \
                where(col('QUS_CONTENT') != 'No Content')
        pd_qus = df_qus.toPandas()
        
        for idx, qus in enumerate(pd_qus['QUS_ANSWER']):
            if pd_qus['QUS_DOC'][idx] in qus :
                pd_qus['QUS_ANSWER'][idx] = qus.split(pd_qus['QUS_DOC'][idx])[1]
                
        df_qus = get_spark_session().createDataFrame(pd_qus.drop(columns='QUS_DOC'))
        df_qus =  df_qus.filter(df_qus.QUS_ANSWER.like('%' + cls.dpt_list[2] + '%'))
        save_data(DataMart, df_qus.limit(int(df_qus.count())-1), 'SUB_OBSTETRICS')

class RecDermaSave:
    """
    피부과
    """
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']
    df_sub = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY') == cal_std_day(0))



    @classmethod
    def save(cls):
        df_qus = cls.df_sub.select(col('QUS_DOC'),col('QUS_CONTENT'),col('QUS_ANSWER').cast('string')). \
                where(col('QUS_CONTENT') != 'No Content')
        pd_qus = df_qus.toPandas()
        
        for idx, qus in enumerate(pd_qus['QUS_ANSWER']):
            if pd_qus['QUS_DOC'][idx] in qus :
                pd_qus['QUS_ANSWER'][idx] = qus.split(pd_qus['QUS_DOC'][idx])[1]
                
        df_qus = get_spark_session().createDataFrame(pd_qus.drop(columns='QUS_DOC'))
        df_qus =  df_qus.filter(df_qus.QUS_ANSWER.like('%' + cls.dpt_list[3] + '%'))
        save_data(DataMart, df_qus.limit(int(df_qus.count())-1), 'SUB_DERMA')

class RecENTSave:
    """
    이비인후과
    """
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']
    df_sub = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY') == cal_std_day(0))



    @classmethod
    def save(cls):
        df_qus = cls.df_sub.select(col('QUS_DOC'),col('QUS_CONTENT'),col('QUS_ANSWER').cast('string')). \
                where(col('QUS_CONTENT') != 'No Content')
        pd_qus = df_qus.toPandas()
        
        for idx, qus in enumerate(pd_qus['QUS_ANSWER']):
            if pd_qus['QUS_DOC'][idx] in qus :
                pd_qus['QUS_ANSWER'][idx] = qus.split(pd_qus['QUS_DOC'][idx])[1]
                
        df_qus = get_spark_session().createDataFrame(pd_qus.drop(columns='QUS_DOC'))
        df_qus =  df_qus.filter(df_qus.QUS_ANSWER.like('%' + cls.dpt_list[4] + '%'))
        save_data(DataMart, df_qus.limit(int(df_qus.count())-1), 'SUB_ENT')

class RecNeuroSave:
    """
    신경외과
    """
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']
    df_sub = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY') == cal_std_day(0))



    @classmethod
    def save(cls):
        df_qus = cls.df_sub.select(col('QUS_DOC'),col('QUS_CONTENT'),col('QUS_ANSWER').cast('string')). \
                where(col('QUS_CONTENT') != 'No Content')
        pd_qus = df_qus.toPandas()
        
        for idx, qus in enumerate(pd_qus['QUS_ANSWER']):
            if pd_qus['QUS_DOC'][idx] in qus :
                pd_qus['QUS_ANSWER'][idx] = qus.split(pd_qus['QUS_DOC'][idx])[1]
                
        df_qus = get_spark_session().createDataFrame(pd_qus.drop(columns='QUS_DOC'))
        df_qus =  df_qus.filter(df_qus.QUS_ANSWER.like('%' + cls.dpt_list[5] + '%'))
        save_data(DataMart, df_qus.limit(int(df_qus.count())-1), 'SUB_NEURO')

class RecUrologySave:
    """
    비뇨기과 | 비뇨의학과
    """
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']
    df_sub = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY') == cal_std_day(0))



    @classmethod
    def save(cls):
        df_qus = cls.df_sub.select(col('QUS_DOC'),col('QUS_CONTENT'),col('QUS_ANSWER').cast('string')). \
                where(col('QUS_CONTENT') != 'No Content')
        pd_qus = df_qus.toPandas()
        
        for idx, qus in enumerate(pd_qus['QUS_ANSWER']):
            if pd_qus['QUS_DOC'][idx] in qus :
                pd_qus['QUS_ANSWER'][idx] = qus.split(pd_qus['QUS_DOC'][idx])[1]
                
        df_qus = get_spark_session().createDataFrame(pd_qus.drop(columns='QUS_DOC'))
        df_qus =  df_qus.filter((df_qus.QUS_ANSWER.like('%' + cls.dpt_list[6] + '%') | (df_qus.QUS_ANSWER.like('%비뇨의학과%'))))
        save_data(DataMart, df_qus.limit(int(df_qus.count())-1), 'SUB_UROLOGY')

class RecOphthalSave:
    """
    안과
    """
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']
    df_sub = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY') == cal_std_day(0))



    @classmethod
    def save(cls):
        df_qus = cls.df_sub.select(col('QUS_DOC'),col('QUS_CONTENT'),col('QUS_ANSWER').cast('string')). \
                where(col('QUS_CONTENT') != 'No Content')

        pd_qus = df_qus.toPandas()
        
        for idx, qus in enumerate(pd_qus['QUS_ANSWER']):
            if pd_qus['QUS_DOC'][idx] in qus :
                pd_qus['QUS_ANSWER'][idx] = qus.split(pd_qus['QUS_DOC'][idx])[1]
                
        df_qus = get_spark_session().createDataFrame(pd_qus.drop(columns='QUS_DOC'))
        df_qus =  df_qus.filter(df_qus.QUS_ANSWER.like('%' + cls.dpt_list[7] + '%'))
        save_data(DataMart, df_qus.limit(int(df_qus.count())-1), 'SUB_OPHTHAL')