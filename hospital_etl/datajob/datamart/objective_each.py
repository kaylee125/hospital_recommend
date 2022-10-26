from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from pyspark.sql.functions import col, ceil
import pandas as pd

class ObjSymptom:
    obj_data = find_data(DataWarehouse, 'OBJ_DATA')
    dis_symt = find_data(DataWarehouse, 'DISEASE_CODE')
    hos_dpt = find_data(DataWarehouse, 'HOSPITAL_DEPARTMENT')
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']

    @classmethod
    def save(cls) :    
        obj_dupl = cls.obj_data.dropDuplicates(['MAIN_SICK'])
        df_obj_symt = obj_dupl.join(cls.dis_symt,obj_dupl.MAIN_SICK == cls.dis_symt.DIS_CODE)
        df_obj_symt = df_obj_symt.join(cls.hos_dpt, df_obj_symt.DSBJT_CD == cls.hos_dpt.GUBUN_NUM). \
            select(col('GUBUN_DIR'),col('DIS_SYMT'))

        for dpt in cls.dpt_list :
            df_dpt = df_obj_symt.where(df_obj_symt.GUBUN_DIR == dpt).dropDuplicates(['DIS_SYMT'])
            df_dpt.show()
            symts = []
            for row in df_dpt.rdd.toLocalIterator():
                symts.append(row.DIS_SYMT)
            symt = ' '.join((symts))
            data_dict = {'dpt':dpt,'symt':symt}
            obj_symt = get_spark_session().createDataFrame(data=[data_dict])
            save_data(DataMart, obj_symt, 'OBJ_SYMT')
 



