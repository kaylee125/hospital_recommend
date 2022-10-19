from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from pyspark.sql.functions import col, ceil

class ObjSymptom:
    
    age_group= find_data(DataWarehouse, 'AGE_GROUP')
    dis_code = find_data(DataWarehouse, 'CLASIFI_DIS_CODE')
    hos_dpt = find_data(DataWarehouse, 'HOSPITAL_DEPARTMENT')
    sido = find_data(DataWarehouse, 'SIDO')
    obj_data = find_data(DataWarehouse, 'OBJ_DATA')

    @classmethod
    def save(cls):
        obj_data = cls.obj_data.to_pandas_on_spark()
        print(obj_data.head(5))
        # age_group = cls.age_group
        # obj_data.join(age_group, obj_data.AGE_GROUP == age_group.AGE_ID).show()
        # cls.age_group.show()
        # cls.obj_data.show()