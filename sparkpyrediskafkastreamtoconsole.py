from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

kafkaRedisSchema = StructType (
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("Score", StringType())
            ]))
        )
    ]
)

kafkaJSONSchema = StructType (
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("email", StringType()),
        StructField("birthYear", StringType())
    ]
)

kafkaEventschema = StructType (
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

spark = SparkSession.builder.appName("kafkaRedis").getOrCreate()

spark.sparkContext.setLogLevel('WARN')

kafkaRedisDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","redis-server")\
    .option("startingOffsets","earliest")\
    .load() 

kafkaRedisDF = kafkaRedisDF.selectExpr("cast(value as string) value")

kafkaRedisDF.withColumn("value", from_json("value", kafkaRedisSchema))\
            .select(col('value.existType'), col('value.Ch'),\
                    col('value.Incr'), col('value.zSetEntries'))\
            .createOrReplaceTempView("RedisSortedSet")

zSetEntriesEncodedStreamingDF=spark.sql("select zSetEntries[0].element as encodedCustomer from RedisSortedSet")

zSetDecodedEntriesStreamingDF=zSetEntriesEncodedStreamingDF.withColumn("customer", unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string"))

customerSchema = StructType (
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]
)
zSetDecodedEntriesStreamingDF.withColumn("customer", from_json("customer", customerSchema))\
                             .select(col('customer.*'))\
                             .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql("select * from CustomerRecords where email is not null AND birthDay is not null")

emailAndBirthDayStreamingDF = emailAndBirthDayStreamingDF.withColumn('birthYear', split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0))

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(col('email'), col('birthYear'))

emailAndBirthYearStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct 