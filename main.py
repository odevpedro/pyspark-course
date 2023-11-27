import pyspark
from pyspark.sql.types import IntegerType



def print_hi(name):
    
    print(f'Hi, {name}')  
    print(f'Hello, {name}')  
    my_list = [1,2,3]
    spark = SparkSession.builder.appName("my first spark app").enableHiveSupport().getOrCreate()
    df = spark.createDataFrame(my_list,IntegerType())
    df.show()
    df.createOrReplaceTempView("temptable1")
    spark.sql("create table as select * from temptable1")


if __name__ == '__main__':
    print_hi('Python Pycharm')

