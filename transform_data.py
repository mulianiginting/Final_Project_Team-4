from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def read_transform_data():
    # Inisialisasi SparkSession
    spark = SparkSession.builder \
        .appName("Read and Transform Data") \
        .config("spark.jars", "/home/muliani/jars/postgresql-42.2.19.jar") \
        .getOrCreate()
    
    try:
        # Membaca data dari CSV
        provinsi_df = spark.read.csv('file:///home/muliani/dummy_data/location_reference.csv', header=True, inferSchema=True)
        produk_df = spark.read.csv('file:///home/muliani/dummy_data/product_reference.csv', header=True, inferSchema=True)
        transaksi_df = spark.read.csv('file:///home/muliani/dummy_data/111_processed.csv', header=True, inferSchema=True)

        # Transformasi data untuk provinsi_df
        provinsi_df = provinsi_df.select("nama_provinsi", "valuename")
        provinsi_df = provinsi_df.withColumnRenamed("valuename", "id")

        # Transformasi data untuk produk_df
        produk_df = produk_df.select("pid", "product_name").orderBy("pid")
        produk_df = produk_df.withColumnRenamed("pid", "id")  # Menyesuaikan nama kolom pid menjadi id

        # Transformasi data untuk transaksi_df
        transaksi_df = transaksi_df.select(
            col("id"),
            col("user_id"),
            col("product_id"),
            col("gross_amount"),
            col("discounts"),
            col("transaction_date")
        ).orderBy("transaction_date")

        # Menyimpan data ke Parquet (opsional)
        provinsi_df.write.mode('overwrite').parquet('file:///home/muliani/dummy_data/provinsi')
        produk_df.write.mode('overwrite').parquet('file:///home/muliani/dummy_data/produk')
        transaksi_df.write.mode('overwrite').parquet('file:///home/muliani/dummy_data/transaksi')

        # Menyimpan hasil ke PostgreSQL OLAP dengan nama tabel yang ditentukan
        olap_jdbc_url = "jdbc:postgresql://localhost:5432/finalproject_olap"
        olap_connection_properties = {
            "user": "postgres",
            "password": "muliani",
            "driver": "org.postgresql.Driver"
        }
        
        # Menulis ke PostgreSQL
        provinsi_df.write.jdbc(url=olap_jdbc_url, table="dim_provinsi", mode="overwrite", properties=olap_connection_properties)
        produk_df.write.jdbc(url=olap_jdbc_url, table="dim_produk", mode="overwrite", properties=olap_connection_properties)
        transaksi_df.write.jdbc(url=olap_jdbc_url, table="fact_transaksi", mode="overwrite", properties=olap_connection_properties)

        # Menandai tugas sebagai selesai jika berhasil
        print("Data transformation successful")
    
    except Exception as e:
        # Menangani kesalahan dan mencatatnya
        print(f"Error in data transformation: {str(e)}")

if __name__ == "__main__":
    read_transform_data()
