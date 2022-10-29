from infra.util import cal_std_day
from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, avg, date_add, current_date

class AnswerDelay:
    sub_data = find_data(DataWarehouse, 'SUB_DATA').where(col('STD_DAY').isin(cal_std_day(0)))
    
    @classmethod
    def save(cls):  
        df_sub = cls.sub_data.where((cls.sub_data.QUS_TIME > 0) & 
                                    (cls.sub_data.ANS_TIME > 0) &
                                        (cls.sub_data.QUS_TIME.isNotNull()) &
                                        (cls.sub_data.ANS_TIME.isNotNull()))    
                                        
        df_delay = df_sub.select((avg(df_sub.QUS_TIME)-avg(df_sub.ANS_TIME)).alias('DELAY_M'))
        df_delay = df_delay.withColumn('STD_DAY',date_add(current_date(), -0))
        df_delay = df_delay.select(
            df_delay.DELAY_M.cast('float'),
            df_delay.STD_DAY.cast('string')
                
        )

        save_data(DataMart, df_delay, 'ANS_DELAY')
    