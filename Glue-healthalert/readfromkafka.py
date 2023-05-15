
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#Importing Necessary libraries.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


#Initializing Spark session.

spark = SparkSession.builder.appName("patient_vitals").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


#Defining schema for the incoming json data.
#Also, adding message_time column in schema which indicates the event time ( time when message is arrived in kafka queue) and pulling this column from kafka 
#Topic itself where we have created this column while pushing data into that topic using producer application.

schema = StructType([StructField("heartBeat", IntegerType()),
                     StructField("bp", IntegerType()),
                     StructField("customerId", IntegerType()),
                     StructField("message_time",TimestampType())])


#Reading data from kafka topic using readstream, specifying elastic IP in bootstrap server and port as 9092.

kafkaData = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "23.23.133.232:9092") \
        .option("subscribe", "vitals") \
        .option("startingOffsets","Latest") \
        .load()

#Converting json string into dataframe format using from_json() method.

kafkastream_DF = kafkaData.select(from_json(col("value").cast("string"), schema).alias("data"))
        
kafkastream_DF1 = kafkastream_DF.select("data.heartBeat","data.bp","data.customerId","data.message_time")

#Writing the data in hdfs in parquet form in the path /user/hadoop/PatientInfo.
def foreachBatchw(batchDF,batchId):
    batchDF.write.format("parquet").mode("append").save("s3://writetos3-15052023/parquetfiles/") 


query = kafkastream_DF1.writeStream.foreachBatch(foreachBatchw) \
        .option("checkpointLocation", "s3://writetos3-15052023/checkpointloc/metadata") \
        .start()

#Stream \
#        .outputMode("append") \
#        .option("truncate","false") \
#        .format("parquet") \
#        .option("path","/user/hive/warehouse/patients_vital1_info") \
#        .option("checkpointLocation", "/user/hive/warehouse/patients_vital1_infoCheckPoint") \
#        .start()

#Awaiting the user termination.

query.awaitTermination()

job.commit()