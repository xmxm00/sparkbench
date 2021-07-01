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
    dataType = "json"
		spark = SparkSession.builder.appName("ETL Using SparkSQL example").getOrCreate()
		df = spark.read.format(dataType).option("header", "true").load("data." + dataType)
		df.show() # 원래 데이터 형
		df.printSchema()
		df = df.withColumn("content", from_json("content", txschema))
		df = df.withColumn("patient", from_json("patient", ptschema))
		df = df.withColumn("dental clinic", from_json("dental clinic", dcschema))
		df.show() # json 문자열 parse
		df = df.select("date", "payment", "content.*", "patient.*", "dentist", "dental clinic.*") # json 데이터 읽기
		df = df.sort(desc("patient_name"), "date") # 환자별 시간순서 정렬
		df.show()
		df.groupBy("patient_name").sum("price").show()
		df.printSchema()
