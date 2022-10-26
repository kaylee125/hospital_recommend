from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col
from pyspark.sql import Row

class SubjectiveTextTransformer:

    @classmethod
    def transform(cls):
        file_dir= '/naver_jisik/'
        file_name = 'qus_ans_' + cal_std_day(0)+'.json'

        data = []
        path = file_dir + file_name     
        sub_text_json = get_spark_session().read.json(path,encoding='UTF-8')
        for r1 in sub_text_json.select(sub_text_json.data, sub_text_json.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                temp = r2.asDict()
                temp['std_day'] = r1['meta.std_day'] 
                data.append(Row(**temp))
        sub_text_data = get_spark_session().createDataFrame(data)
        sub_text = sub_text_data.select(
            sub_text_data.qus_title.alias('QUS_TITLE').cast('string'),
            sub_text_data.qus_dir.alias('QUS_DIR').cast('string'),
            sub_text_data.qus_doc_id.alias('QUS_DOC_ID').cast('string'),
            sub_text_data.qus_doc.alias('QUS_DOC').cast('string'),
            sub_text_data.qus_time.alias('QUS_TIME').cast('int'),
            sub_text_data.ans_time.alias('ANS_TIME').cast('int'),
            sub_text_data.qus_content.alias('QUS_CONTENT').cast('string'),
            sub_text_data.qus_answer.alias('QUS_ANSWER').cast('string'),
            sub_text_data.std_day.alias('STD_DAY').cast('string'),
        )

        save_data(DataWarehouse, sub_text.limit(int(sub_text.count())-1), 'SUB_DATA')