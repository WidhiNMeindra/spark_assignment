
## Dokumentasi Assignment Kafka
Assignment ini melibatkan pembuatan **Producer** dan **Consumer** untuk memproduksi dan mengkonsumsi data nasabah bank menggunakan **Apache Kafka**. Data yang diproduksi oleh **Producer** akan berisi informasi tentang nasabah bank, sedangkan **Consumer** akan membaca data tersebut dan melakukan perhitungan tertentu (misalnya, menghitung rata-rata saldo nasabah). Assignment ini juga menggunakan **ksqlDB** untuk query dan transformasi data secara real-time.

## Tools yang Digunakan
- **Docker**: Untuk menjalankan Kafka, ksqlDB, dan Kafka UI.
- **Kafka**: Untuk messaging dan streaming data.
- **ksqlDB**: Untuk melakukan query real-time pada data yang mengalir melalui Kafka.
- **Python**: Digunakan untuk membuat Kafka Producer dan Consumer.
- **Jupyter Notebook**: Digunakan untuk menulis dan menjalankan script Python yang terkait dengan Kafka Producer.

## Struktur Proyek
- **Producer**: Menghasilkan data nasabah bank secara acak dan mengirimkannya ke topik Kafka `nasabah_bank`.
- **Consumer**: Mengkonsumsi data dari topik Kafka dan melakukan perhitungan (misalnya, rata-rata saldo nasabah).
- **Kafka**: Menyimpan dan mengalirkan data dalam bentuk stream.
- **ksqlDB**: Untuk menjalankan query SQL pada stream data dan melakukan agregasi secara real-time.

# Hasil pekerjaan ada di folder screenshoot

---------------------------------------------------------------------------------------------------------------------------

## Dokumentasi ETL Batch Airflow & PySpark

# Alur Kerja DAG Airflow
DAG otomatisasi proses ETL dijadwalkan dengan Airflow.
Satu task utama: menjalankan script PySpark menggunakan BashOperator dan spark-submit.
Data hasil ETL disimpan ke CSV & PostgreSQL.

# Proses ETL
-Extract:
Membaca data retail dari CSV (online-retail-dataset.csv) dengan PySpark.

-Transform:
Menambah kolom bulan & tahun invoice.

Menghitung jumlah pelanggan unik setiap bulan (countDistinct(CustomerID)).

-Load:
Menulis hasil agregasi ke file CSV.

Menyimpan hasil ke tabel PostgreSQL menggunakan koneksi JDBC.

# Analisis Batch
-Analisis Retention:
Mengukur jumlah pelanggan unik per bulan (customer retention) sepanjang data.

# Output:
Tabel/CSV: InvoiceYear, InvoiceMonth, unique_customers.

# Cara Menjalankan
Pastikan semua container (Airflow, Spark, PostgreSQL) sudah jalan.

Pastikan script dan data sudah di path yang benar.

Trigger DAG dari Airflow Web UI.

Cek hasil di output file dan database.

# Catatan:
Pastikan koneksi database dan path file sudah benar.

Untuk kendala, cek log Airflow/Spark.

Gunakan driver JDBC saat koneksi ke PostgreSQL.

# Hasil pekerjaan ada di folder screenshoot
