from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, ceil
import pandas as pd

class QtyEachDpt:
    sub_data = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY').isin(cal_std_day(0)))
    dpt_list = ['내과','정형외과','산부인과','피부과','이비인후과','신경외과','비뇨기과','안과']
    
    @classmethod
    def save(cls):  
        df_sub_data = cls.sub_data.toPandas()

        # 자기소개 부분의 **과전문의라 소개하여 특정과가 잘못들어가는 일을 방지
        for idx, ans in enumerate(list(df_sub_data['QUS_ANSWER'])):
            doc_name = df_sub_data['QUS_DOC'][idx]
            if doc_name in ans :
                df_sub_data['QUS_ANSWER'][idx] = ans.split(doc_name)[1]
        dpts = []
        dpt_qty = []
        std_day = []
        for dpt in cls.dpt_list:
            dpts.append(dpt)
            dpt_qty.append(len(df_sub_data[(df_sub_data['QUS_ANSWER'].str.contains(dpt))]['QUS_ANSWER']))
            std_day.append(cal_std_day(0))

        df_qty = pd.DataFrame({'DPT':dpts, 'QUS_QTY':dpt_qty, 'STD_DAY':std_day})
        rec_rank = get_spark_session().createDataFrame(df_qty)
        save_data(DataMart,rec_rank,'REC_RANK')


        
