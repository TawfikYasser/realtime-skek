# Consumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from pyspark.sql.functions import from_json, col, when, udf
import sys
import os
import openai
from textblob import TextBlob

# Get the current directory of this script
current_dir = os.path.dirname(os.path.abspath(__file__))
# Append the parent directory to the Python path
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)

# Now you can import config.py
from config import config
import time

def sentiment_analysis(comment) -> str:
    if comment:
        # Instead of openai api because quota was exceeded
        sentiment = TextBlob(comment).sentiment
        polarity = sentiment.polarity
        if polarity == 0:
            return 'neutral'
        elif polarity > 0:
            return 'positive'
        else:
            return 'negative' 
    return "Empty"

def start_straming(spark):
    topic = "customers_review"
    # Read the data from the socket (server)
    while True:
        try:
            stream_df = (spark.readStream.format("socket")
            .option("host", "0.0.0.0")
            .option("port", 9991)
            .load())

            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])

            stream_df = stream_df.select(from_json(col('value'), schema).alias('data')).select(('data.*'))


            # Console
            # query = stream_df.writeStream.outputMode("append").format("console").start()
            # query.awaitTermination()  

            # Produce to Kafka Cluster on Confluent Cloud


            # Applying sentiment analysis using openai through a udf
            sentiment_analysis_udf = udf(sentiment_analysis, StringType())

            stream_df = stream_df.withColumn('feedback',
                                             when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                                             .otherwise(None)
                                             )
            
            # Preparing data for kafka, sending it
            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")
            query =(kafka_df.writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", config.config["kafka"]["bootstrap.servers"])
                    .option("kafka.security.protocol", config.config["kafka"]["security.protocol"])
                    .option("kafka.sasl.mechanism", config.config["kafka"]["sasl.mechanisms"])
                    .option("kafka.sasl.jaas.config",
                            'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                            'password="{password}";'.format(
                                username=config.config["kafka"]["sasl.username"],
                                password=config.config["kafka"]["sasl.password"]
                            ))
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .option("topic", topic)
                    .start()
                    )
            query.awaitTermination()

        except Exception as e:
            print(f'Exception encountered {e}. Retrying  in 10 seconds.')
            time.sleep(10)

if __name__=='__main__':
    spark_conn = (SparkSession.builder.appName("SocketStreamConsumer").getOrCreate())
    start_straming(spark_conn)