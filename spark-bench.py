import timeit
import time
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, desc, col

txschema = StructType([
    StructField("treat_name", StringType(), False),
    StructField("price", LongType(), False)
])

ptschema = StructType([
    StructField("uuid4", StringType(), False),
    StructField("patient_name", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("sex", StringType(), False)
])

dcschema = StructType([
    StructField("clinic_name", StringType(), False),
    StructField("address", StringType(), False),
    StructField("telephone", StringType(), False)
])


if __name__ == "__main__":
    iteration = 1
    for i in range(0, iteration):
        spark = SparkSession.builder.appName("Data Load Benchmark").getOrCreate()
        df = spark.read.format("parquet").option("header", "true").load("./output/parquet")
        start_time = timeit.default_timer()
        df = df.withColumn("content", from_json("content", txschema))
        df = df.withColumn("patient", from_json("patient", ptschema))
        df = df.withColumn("dental_clinic", from_json("dental_clinic", dcschema))
        df = df.select("date", "payment", "content.*", "patient.*", "dentist", "dental_clinic.*")
        df.show() # 모든 데이터 parse 완료
        end_time = timeit.default_timer() # filter(advanced)
        df = df.sort(desc("patient_name"), "date") # SQL을 이용한 정렬
        df.show()
        df.groupBy("patient_name").sum("price").show() # filter
        df.printSchema()
        print(end_time - start_time)
        end_time = timeit.default_timer() # filter(advanced)
        # remove data
        # add data

