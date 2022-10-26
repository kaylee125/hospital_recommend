from infra.jdbc import DataWarehouse, save_data, find_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col
from pyspark.sql import Row

class DocInfoTransformer:
    FILE_DIR = '/naver_doc/'
    FILE_NAME = 'doctor_info_' + cal_std_day(0) + '.json'
    bf_doc=find_data(DataWarehouse, 'DOC_DATA')
    @classmethod
    def transform(cls):

        data = []
        path = cls.FILE_DIR + cls.FILE_NAME     
        doc_info_json = get_spark_session().read.json(path,encoding='UTF-8')
        for r1 in doc_info_json.select(doc_info_json.data, doc_info_json.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                temp = r2.asDict()
                temp['std_day'] = r1['meta.std_day'] 
                data.append(Row(**temp))
        doc_info_data = get_spark_session().createDataFrame(data)
        doc_info = doc_info_data.select(
            doc_info_data.doc_id.alias('DOC_ID_1').cast('string'),
            doc_info_data.doc_name.alias('DOC_NAME_1').cast('string'),
            doc_info_data.is_pro.alias('DOC_IS_PRO_1').cast('string'),
            doc_info_data.doc_pos.alias('DOC_POS_1').cast('string'),
            doc_info_data.naver_grade.alias('NAVER_GRADE_1').cast('string'),
            doc_info_data.std_day.alias('STD_DAY_1')
        ).where(col('DOC_ID_1').isNotNull())
        
        # 컬럼 이름 뒤에 _1 있는게 최신데이터
        df_join = doc_info.join(cls.bf_doc,doc_info.DOC_ID_1 == cls.bf_doc.DOC_ID, how='outer')
        df_join.printSchema()


        # 의사가 추가되는 경우, 의사정보가 변경 되는 경우
        # 1. 의사가 추가되는 경우
        df_new_doc = df_join.where(col('DOC_ID').isNull())
        df_new_doc = df_new_doc.select(
            df_new_doc.DOC_ID_1.alias('DOC_ID').cast('string'),
            df_new_doc.DOC_NAME_1.alias('DOC_NAME').cast('string'),
            df_new_doc.DOC_IS_PRO_1.alias('DOC_IS_PRO').cast('string'),
            df_new_doc.DOC_POS_1.alias('DOC_POS').cast('string'),
            df_new_doc.NAVER_GRADE_1.alias('NAVER_GRADE').cast('string'),
            df_new_doc.STD_DAY_1.alias('STD_DAY')
        ).where(col('DOC_NAME') != '양우영')


        if df_new_doc.count() > 0 :
            save_data(DataWarehouse, df_new_doc.limit(int(df_new_doc.count())-1), 'DOC_DATA')