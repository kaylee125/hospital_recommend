import imp
from infra.jdbc import DataWarehouse, save_data, overwrite_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col,length
from pyspark.sql import Row, Column


    
class ObjectiveTextTransformer:
    FILE_DIR = '/rawdata/'
    FILE_NAME = ['dis_code.csv','gubun_dir.csv','df_obj_short.csv','age_group.csv','clasfi_dis_code.csv','form.csv','sido.csv']
    TABLES = ['DISEASE_CODE','HOSPITAL_DEPARTMENT','OBJ_DATA','AGE_GROUP','CLASIFI_DIS_CODE','FORM','SIDO']
    @classmethod
    def transform(cls) :
        df_code = get_spark_session().read.csv(cls.FILE_DIR+cls.FILE_NAME[0],encoding='utf-8',header=True).drop('_c0','Unnamed: 0','dis_id')
        df_gubun = get_spark_session().read.csv(cls.FILE_DIR+cls.FILE_NAME[1],encoding='utf-8',header=True).drop('_c0','Unnamed: 0')
        df_objecitve = get_spark_session().read.csv(cls.FILE_DIR+cls.FILE_NAME[2],encoding='utf-8',header=True).drop('_c0','Unnamed: 0')
        df_age_group = get_spark_session().read.csv(cls.FILE_DIR+cls.FILE_NAME[3],encoding='utf-8',header=True).drop('_c0','Unnamed: 0')
        df_clasfi_dis_code = get_spark_session().read.csv(cls.FILE_DIR+cls.FILE_NAME[4],encoding='utf-8',header=True).drop('_c0','Unnamed: 0')
        df_form = get_spark_session().read.csv(cls.FILE_DIR+cls.FILE_NAME[5],encoding='utf-8',header=True).drop('_c0','Unnamed: 0')
        df_sido = get_spark_session().read.csv(cls.FILE_DIR+cls.FILE_NAME[6],encoding='utf-8',header=True).drop('_c0','Unnamed: 0')
        
        
        code = df_code.select(
            df_code.dis_name.alias('DIS_NAME'),
            df_code.dis_dir.alias('DIS_DIR'), 
            df_code.dis_code.alias('DIS_CODE'), 
            df_code.dis_symt.alias('DIS_SYMT').cast('string')
        ).where(col('DIS_CODE').isNotNull())
        
        gubun = df_gubun.select(
            df_gubun.gubun_num.alias('GUBUN_NUM'),
            df_gubun.gubun_dir.alias('GUBUN_DIR')
        )

        objective = df_objecitve.select(
            df_objecitve.기준년도.alias('STND_Y'),
            col('가입자 일련번호').alias('IDV_ID'),
            df_objecitve.진료내역일련번호.alias('KEY_SEQ').cast('int'),
            df_objecitve.성별코드.alias('SEX'),
            df_objecitve.연령대코드.alias('AGE_GROUP'),
            df_objecitve.시도코드.alias('SIDO'),
            df_objecitve.요양개시일자.alias('RECU_FR_DT'),
            df_objecitve.서식코드.alias('FORM_CD'),
            df_objecitve.진료과목코드.alias('DSBJT_CD'),
            df_objecitve.주상병코드.alias('MAIN_SICK'),
            df_objecitve.부상병코드.alias('SUB_SICK'),
            df_objecitve.요양일수.alias('VSCN'),
            df_objecitve.입내원일수.alias('RECN'),
            df_objecitve.심결가산율.alias('EDEC_ADD_RT'),
            df_objecitve.심결요양급여비용총액.alias('EDEC_TRAMT'),
            df_objecitve.심결본인부담금.alias('EDEC_SBRDN_AMT'),
            df_objecitve.심결보험자부담금.alias('EDEC_JBRDN_AMT'),
            df_objecitve.총처방일수.alias('TOT_PRES_D_CNT'),
            col('데이터 기준일자').alias('DATA_STD_DT')
        ).where(col('AGE_GROUP').isNotNull()). \
            where(col('SIDO').isNotNull()). \
                where(col('FORM_CD').isNotNull()). \
                    where(col('MAIN_SICK').isNotNull()). \
                        where(col('KEY_SEQ').isNotNull())                            

        age_group = df_age_group.select(
            df_age_group.age_id.alias('AGE_ID'),
            df_age_group.age_group.alias('AGE_GROUP')
        )

        clasfi_dis_code = df_clasfi_dis_code.select(
            df_clasfi_dis_code.dis_id.alias('DIS_ID'),
            df_clasfi_dis_code.dis_name.alias('DIS_NAME')
        ).where(col('DIS_ID').isNotNull())

        form = df_form.select(
            df_form.form_id.alias('FORM_ID'),
            df_form.form_name.alias('FORM_NAME')
        )
        sido = df_sido.select(
            df_sido.sido_id.alias('SIDO_ID'),
            df_sido.sido_name.alias('SIDO_NAME')
        )
        
        spark_df = [code,gubun,objective,age_group,clasfi_dis_code,form,sido]

        # save_data(DataWarehouse,spark_df[0], cls.TABLES[0])
        save_data(DataWarehouse,spark_df[1], cls.TABLES[1])
        # save_data(DataWarehouse,spark_df[2].limit(int(spark_df[2].count())-1), cls.TABLES[2])
        # save_data(DataWarehouse,spark_df[3], cls.TABLES[3])
        # save_data(DataWarehouse,spark_df[4], cls.TABLES[4])
        # save_data(DataWarehouse,spark_df[5], cls.TABLES[5])
        # save_data(DataWarehouse,spark_df[6], cls.TABLES[6])
 