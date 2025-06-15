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
