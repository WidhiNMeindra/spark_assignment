from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, from_json
import logging
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("Kafka Streaming Job") \
    .getOrCreate()

# Set log level to WARN or ERROR to suppress INFO logs
spark.sparkContext.setLogLevel("WARN")

# Mengonfigurasi log level untuk Spark menggunakan log4j
spark.sparkContext._jvm.org.apache.log4j.Logger.getLogger("org").setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.ERROR)
spark.sparkContext._jvm.org.apache.log4j.Logger.getLogger("akka").setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.ERROR)

# Mendefinisikan schema untuk event pembelian
schema = StructType([
    StructField("purchase_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Kafka consumer
purchase_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "purchases_topic") \
    .load()

# Convert Kafka value to string
purchase_events = purchase_df.selectExpr("CAST(value AS STRING)")

# Parse JSON ke DataFrame dengan schema yang telah ditentukan
purchase_events = purchase_events \
    .select(from_json(col("value"), schema).alias("purchase")) \
    .select("purchase.*")

# Convert timestamp ke tipe data timestamp
purchase_events = purchase_events \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# Agregasi jumlah pembelian per 30 detik
aggregated_df = purchase_events \
    .groupBy(window(col("timestamp"), "30 seconds")) \
    .agg({"amount": "sum"}) \
    .withColumnRenamed("sum(amount)", "total_amount")

# Output data ke konsol menggunakan foreachBatch dengan pembatasan
def foreach_batch_function(df, epoch_id):
    print(f"\n\nBatch ID: {epoch_id}")  # Menampilkan ID batch untuk memudahkan pelacakan
    df.show(truncate=False, n=10)  # Membatasi hanya 10 baris pertama dari setiap batch

# Write the output stream with complete mode
query = aggregated_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(foreach_batch_function) \
    .start()

# Tunggu hingga streaming job selesai
query.awaitTermination()
