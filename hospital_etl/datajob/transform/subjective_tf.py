from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col
from pyspark.sql import Row

class SubjectiveTextTransformer:


    @classmethod
    def transform(cls):
        base_path= '/naver_jisik/'
        params = 'qus_ans_' + cal_std_day(1)+'.json'
        data = []
        for param in params:
            path = base_path + param     
            sub_text_json = get_spark_session().read.json(path,encoding='UTF-8')
            for r1 in sub_text_json.select(sub_text_json.data, sub_text_json.meta.std_day).toLocalIterator():
                for r2 in r1.data:
                    temp = r2.asDict()
                    temp['std_day'] = r1['meta.std_day'] 
                    data.append(Row(**temp))
        sub_text_data = get_spark_session().createDataFrame(data)
        sub_text = sub_text_data.select(
            sub_text_data.product_name.alias('PRODUCT_NAME').cast('string'),
            sub_text_data.unit.alias('UNIT').cast('string'),
            sub_text_data.std_day.alias('STD_DAY').cast('string'),
            sub_text_data.fluctuation_rate.alias('FLUCTUATION_RATE').cast('float'),
            sub_text_data.is_rise.alias('IS_RISE').cast('int'),
            sub_text_data.price.alias('PRICE').cast('float'),
            sub_text_data.product_line.alias('PRODUCT_LINE').cast('string'),
        ) 
        save_data(DataWarehouse, fu_market, 'FUTURES_MARKET')