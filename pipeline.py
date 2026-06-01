from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("Pipeline_Kafka_Spark_Cassandra") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("regiao",     StringType(), True),
    StructField("vendedor",   StringType(), True),
    StructField("produto",    StringType(), True),
    StructField("quantidade", StringType(), True),
    StructField("preco",      StringType(), True),
    StructField("categoria",  StringType(), True),
    StructField("status",     StringType(), True)
])

df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topico_vendas") \
    .option("startingOffsets", "latest") \
    .load()

df_estruturado = df_kafka \
    .select(col("value").cast("string").alias("json_bruto")) \
    .select(from_json(col("json_bruto"), schema).alias("dados")) \
    .select("dados.*")

def salvar_no_cassandra(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="vendas", keyspace="minha_loja") \
            .save()

query = df_estruturado \
    .writeStream \
    .foreachBatch(salvar_no_cassandra) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint_kafka_cassandra") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
