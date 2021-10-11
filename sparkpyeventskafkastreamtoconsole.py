from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

spark = SparkSession.builder.appName("kafkaEvents").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

kafkaEventsDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","stedi-events")\
    .option("startingOffsets","earliest")\
    .load() 

kafkaEventsDF = kafkaEventsDF.selectExpr("cast(value as string) value")

kafkaEventschema = StructType (
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)
kafkaEventsDF.withColumn("value", from_json("value", kafkaEventschema))\
             .select(col('value.customer'), col('value.score'), col('value.riskDate'))\
             .createOrReplaceTempView("CustomerRisk")

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

customerRiskStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 