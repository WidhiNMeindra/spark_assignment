from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, month, year, sum as _sum

def main():
    spark = SparkSession.builder \
        .appName("Retail ETL Batch") \
        .getOrCreate()

    # EXTRACT: Baca dataset retail
    df = spark.read.csv('/opt/airflow/dags/data/online-retail-dataset.csv', header=True, inferSchema=True)

    # CASTING & CLEANING
    # Bersihkan data yang tidak valid:
    df = df.filter(df.CustomerID.isNotNull())
    df = df.filter(df.Quantity > 0)
    df = df.filter(df.UnitPrice > 0)

    # Parsing InvoiceDate ke tanggal
    from pyspark.sql.functions import to_timestamp
    df = df.withColumn("InvoiceTimestamp", to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))

    # TRANSFORM: Contoh agregasi dan retention
    df = df.withColumn("InvoiceMonth", month("InvoiceTimestamp"))
    df = df.withColumn("InvoiceYear", year("InvoiceTimestamp"))

    # a) Retention (jumlah customer unik tiap bulan)
    retention = df.groupBy("InvoiceYear", "InvoiceMonth") \
                  .agg(countDistinct("CustomerID").alias("unique_customers"))

    # b) Penjualan total per bulan
    sales_per_month = df.groupBy("InvoiceYear", "InvoiceMonth") \
        .agg(
            _sum(col("Quantity") * col("UnitPrice")).alias("monthly_sales"),
            countDistinct("InvoiceNo").alias("total_invoices"),
            countDistinct("CustomerID").alias("unique_customers")
        )

    # LOAD: Simpan retention ke CSV dan PostgreSQL
    retention.write.csv('/opt/airflow/dags/output/retention_per_month.csv', header=True, mode="overwrite")
    retention.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://dataeng-postgres:5432/warehouse") \
        .option("dbtable", "retention_per_month") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    # LOAD: Simpan sales_per_month ke CSV dan PostgreSQL
    sales_per_month.write.csv('/opt/airflow/dags/output/sales_per_month.csv', header=True, mode="overwrite")
    sales_per_month.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://dataeng-postgres:5432/warehouse") \
        .option("dbtable", "sales_per_month") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    # Print beberapa hasil
    print("Retention per month:")
    retention.show(5)
    print("Sales per month:")
    sales_per_month.show(5)

    spark.stop()

if __name__ == "__main__":
    main()
