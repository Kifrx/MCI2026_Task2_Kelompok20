# 🛒 Building Market Basket Analysis Pipeline

> **Authors: MCI_Kelompok_20**
>
> 1. _Bima Novrifa Ananditya (5025241194)_
> 2. _Syah Amin Zikri (5025241197)_

Proyek ini mengekstrak data transaksi dari **http://96.9.212.102:8000/orders**, menemukan pola asosiasi produk menggunakan algoritma FP-Growth di PySpark MLlib, mengorkestrasinya via **Apache Airflow**, menyimpannya ke **ClickHouse**, dan memvisualisasikannya di **Metabase**, dimana semuanya dikemas dalam satu lingkungan Docker.

Arsitektur sistem ini mengadopsi pendekatan _Snapshot / Full Refresh_ untuk memastikan seluruh metrik analitik dan _dashboard_ selalu merepresentasikan kondisi data pesanan yang paling mutakhir.

---

## 🏗️ Arsitektur Sistem

```text
E-commerce API
   ↓ (100 pesanan terbaru / snapshot)
[Ingestion — Python requests]
   ↓ simpan latest_orders.parquet
[Data Lake — folder lokal]
   ↓ baca, flattening, & FP-Growth ML
[Processing — Apache Spark MLlib]
   ↓ truncate-insert (Full Refresh)
[Data Warehouse — ClickHouse]
   ↓ koneksi langsung
[Dashboard — Metabase]

↻ Seluruh siklus diatur oleh Apache Airflow
```

**Metrik yang dianalisis:**

- **Peak Order Times:** jam-jam tersibuk pelanggan melakukan checkout pesanan
- **Weekday vs Weekend Behavior:** perbandingan volume transaksi di hari kerja versus akhir pekan
- **Top Departments:** kategori departemen yang menyumbang volume penjualan terbesar
- **Most Reordered Products:** produk dengan tingkat repeat order paling tinggi
- **Customer Reorder Behavior:** seberapa lama jarak rata-rata hari pelanggan kembali berbelanja
- **Market Basket Analysis:** korelasi dan probabilitas antar produk yang sering dibeli secara bersamaan (berdasarkan nilai Lift & Confidence)

## 🛠️ Tech Stack

| Komponen       | Teknologi                         |
| -------------- | --------------------------------- |
| Orchestration  | Apache Airflow 2.9                |
| Processing     | Apache Spark / PySpark 3.5        |
| Data Warehouse | ClickHouse (column-oriented OLAP) |
| BI & Dashboard | Metabase                          |
| Infrastructure | Docker & Docker Compose           |
| Language       | Python 3.11                       |

## 📂 Struktur direktori

```

orders-pipeline/
│
├── dags/               # Folder utama Airflow
|    └── scripts/       # Kumpulan task script Python
│    |  ├── fetch_orders.py             # Ekstrasi: Narik data JSON dari API
│    |  └── process_orders_spark.py     # Transformation: PySpark & FP-Growth MLlib
|    └── orders_pipeline.py             # File DAG (Skenario penjadwalan/orkestrasi)
│
├── data_lake/          # Folder transit lokal (Storage)
│    └── order/         # Tempat nyimpen file temporary (latest_orders.parquet)
│
├── .gitignore          # File untuk mengecualikan data rahasia/sampah
├── docker-compose.yml  # Konfigurasi container (Airflow, ClickHouse, dll)
├── requirements.txt    # Dependencies (pyspark, clickhouse-driver, pandas)
└── README.md           # Dokumentasi project

```

## 🚀 Tutorial Penggunaan

Ikuti langkah-langkah di bawah ini untuk mengkloning repositori, menyiapkan lingkungan Docker, hingga menjalankan seluruh _pipeline_.

### 📋 Prasyarat (Prerequisites)

Pastikan perangkat lu sudah terinstal aplikasi berikut:

- [Git](https://git-scm.com/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

---

### Langkah-Langkah Eksekusi

#### 1. Kloning Repositori

Buka terminal di VS Code, lalu jalankan perintah berikut:

```bash
git clone https://github.com/Kifrx/MCI2026_Task2_Kelompok20.git

cd orders-pipeline
```

#### 2. Jalankan Docker

Build image, sebelum itu buka dahulu docker desktop.

```
docker-compose build
```

Instalasi database airflow

```
docker-compose up airflow-init
```

Jalankan seluruh pipeline

```
docker-compose up -d
```

> Tunggu 1–2 menit lalu buka http://localhost:8080

#### 3. Aktifkan Pipeline di Airflow

1. Buka http://localhost:8080 → login admin / admin
2. Temukan DAG wikipedia_realtime_stream, geser sakelar untuk mengaktifkan
3. Klik ▶️ Trigger DAG untuk memaksanya jalan sekarang

![alt text](/Assets/image.png)
Gambar di atas menunjukkan bahwa task-task sukses dijalankan.

![alt text](/Assets/Screenshot%202026-05-18%20030959.png)

Gambar diatas menampikan tab Graph, dimana dua kotak di atas saling terhubung dengan garis biru yang merupakan representasi visual dari logika urutan code. Kotak kiri adalah tugas menyedot data (fetch_orders_data) dan kotak kanan adalah tugas mengolah data dengan Spark (process_to_clickhouse). Di dalam kedua kotak tersebut terdapat indikator kotak kecil berwarna hijau bertuliskan success. Ini membuktikan bahwa dependency (ketergantungan) dibuat berjalan lancar: Tugas 1 berhasil mencari data, lalu estafet diserahkan ke Tugas 2, dan Tugas 2 berhasil mengolah serta memasukkannya ke ClickHouse.

![alt text](/Assets/Audit_log.png)

Dari gambar, dapt dilihat pesan yang dicetak: "Load data orders..." dan terdapat "INFO - ✅ Sukses menyimpan 100 baris ke /opt/\*\*\*/data_lake/order/order_20260517_200740.parquet". Di baris bawah, tertulis Command exited with return code 0. Maka eksekusi berhasil 100% terjalankan

---

### Analisis DAGs

#### 1. Task `fetch_orders.py`

```
import requests
import pandas as pd
import os
from datetime import datetime

def fetch_orders():
    print("Load data orders...")
    url = "http://96.9.212.102:8000/orders"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        orders_list = data.get('orders', [])

        if not orders_list:
            print("⚠️ Tidak ada data pesanan yang ditarik.")
            return

        df = pd.DataFrame(orders_list)

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f'/opt/airflow/data_lake/order/orders_{current_time}.parquet'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        df.to_parquet(output_path, index=False)
        print(f"✅ Sukses menyimpan {len(df)} baris ke {output_path}")

    except Exception as e:
        print(f"❌ Error saat memproses data: {e}")
        raise

if __name__ == "__main__":
    fetch_orders()

```

File ini berfungsi sebagai tahap Data Ingestion (penarikan data). Script ini mengambil data pesanan melalui API dari url:`http://96.9.212.102:8000/orders`.

Data yang ditarik berbentuk JSON, yang kemudian diubah menjadi Pandas DataFrame dan disimpan dengan format Parquet (bukan JSON) agar lebih efisien. Data tersebut disimpan di direktori data lake dengan format penamaan berbasis waktu: `output_path = f'/opt/airflow/data_lake/order/orders_{current_time}.parquet'`.

- Jika berhasil: Akan muncul log di sistem berupa `✅ Sukses menyimpan {len(df)} baris ke {output_path}`.

- Jika gagal (misal API mati atau timeout): Akan muncul log `❌ Error saat memproses data: {e}` dan proses akan dihentikan.

#### 2. Task `process_orders_spark.py`

```
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth
from clickhouse_driver import Client
import os
import glob

def run_spark_analytics():
    print("1. Menginisialisai SPARK...")
    spark = SparkSession.builder \
        .appName("Orders_Analytics_Final") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    try:
        print("2. Mmbaca parquet & flattening...")
        df_raw = spark.read.parquet("file:///opt/airflow/data_lake/order/")

        df_exploded = df_raw.withColumn("product_item", F.explode("products"))

        df_flat = df_exploded.select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("user_id").cast("int").alias("user_id"),
            F.col("order_number").cast("int").alias("order_number"),
            F.col("order_dow").cast("int").alias("order_dow"),
            F.col("order_hour_of_day").cast("int").alias("order_hour_of_day"),
            F.col("days_since_prior_order").cast("float").alias("days_since_prior_order"),
            F.col("product_item.product_id").cast("int").alias("product_id"),
            F.col("product_item.product_name").cast("string").alias("product_name"),
            F.col("product_item.department").cast("string").alias("department"),
            F.col("product_item.reordered").cast("int").alias("reordered")
        )

        print("3. Menjalankan FP-GROWTH...")
        df_basket = df_flat.groupBy("order_id").agg(
            F.collect_set("product_name").alias("items")
        )

        fpGrowth = FPGrowth(itemsCol="items", minSupport=0.02, minConfidence=0.1)
        model = fpGrowth.fit(df_basket)

        df_rules = model.associationRules
        df_rules_clean = df_rules.select(
            F.concat_ws(", ", F.col("antecedent")).alias("antecedent"),
            F.concat_ws(", ", F.col("consequent")).alias("consequent"),
            F.col("confidence").cast("float").alias("confidence"),
            F.col("lift").cast("float").alias("lift"),
            F.col("support").cast("float").alias("support")
        )

        print("4. Convert ke pandas...")
        final_results = df_flat.toPandas()
        rules_results = df_rules_clean.toPandas()

    except Exception as e:
        print(f"!!! ERROR SPARK: {e} !!!")
        raise e
    finally:
        spark.stop()
        print("5. SPARK done....")

    print("6. Memuat ke Clickhouse ...")
    client = Client(host='clickhouse-server', user='admin', password='rahasia')

    client.execute('CREATE DATABASE IF NOT EXISTS analytics')

    # Membuata tbel utama
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.order_items (
            order_id Int32,
            user_id Int32,
            order_number Int32,
            order_dow Int32,
            order_hour_of_day Int32,
            days_since_prior_order Float32,
            product_id Int32,
            product_name String,
            department String,
            reordered Int32
        ) ENGINE = MergeTree()
        ORDER BY (order_dow, order_hour_of_day, order_id)
    ''')

    # membuat Tbl Rules FP-Growth
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.fp_growth_rules (
            antecedent String,
            consequent String,
            confidence Float32,
            lift Float32,
            support Float32
        ) ENGINE = MergeTree()
        ORDER BY (confidence, lift)
    ''')

    # Truncate & Insert data ke clickHouse
    client.execute('TRUNCATE TABLE analytics.order_items')
    data_tuples = [tuple(x) for x in final_results.to_numpy()]
    if data_tuples:
        client.execute('INSERT INTO analytics.order_items VALUES', data_tuples)

    client.execute('TRUNCATE TABLE analytics.fp_growth_rules')
    rules_tuples = [tuple(x) for x in rules_results.to_numpy()]
    if rules_tuples:
        client.execute('INSERT INTO analytics.fp_growth_rules VALUES', rules_tuples)

    print("7. Membersihkan file Parquet lama dari Data Lake...")
    for f in glob.glob('/opt/airflow/data_lake/order/*.parquet'):
        try: os.remove(f)
        except OSError as e: print(f"Error: {f} : {e.strerror}")

    print("✅ PIPELINE SELESAI")

if __name__ == "__main__":
    run_spark_analytics()

```

File ini berfungsi sebagai tahap **Data Processing & Analytics** menggunakan **PySpark**, serta **Data Loading** ke dalam database analitik (ClickHouse). Alurnya seperti ini:

1. Inisialisasi Spark

```
print("1. Menginisialisai SPARK...")
    spark = SparkSession.builder \
        .appName("Orders_Analytics_Final") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
```

Ini sebgai titik masuk (entry point) utama untuk memulai pemrosesan data menggunakan **PySpark**. Kode ini mengonfigurasi dan membangun "mesin" Spark sebelum digunakan.

- `SparkSession.builder:` Pemanggilan awal untuk mengatur konfigurasi sesi Spark.
- `.appName("Orders_Analytics_Final"):` Memberikan nama identitas untuk aplikasi Spark ini.
- `.master("local[*]")`: Menentukan di mana Spark akan dieksekusi. `local` berarti Spark berjalan secara mandiri di komputer/server lokal. Tanda bintang [*] menginstruksikan Spark untuk menggunakan semua inti (cores) CPU yang tersedia pada sistem, sehingga proses komputasi bisa berjalan secara paralel dan jauh lebih cepat.
- `.config("spark.driver.memory", "1g"):`Membatasi alokasi RAM (memori) maksimal sebesar 1 Gigabyte (1g) untuk driver process (program utama) Spark. Hal ini mencegah Spark memakan semua RAM sistem yang bisa menyebabkan Out of Memory (OOM) error atau crash pada server Airflow.
- `.getOrCreate():` Jika sebelumnya sudah ada sesi Spark yang berjalan, perintah ini akan menggunakan sesi tersebut. Namun jika belum ada, ia akan membuat sesi (session) yang baru sesuai dengan konfigurasi di atas.

2. Membaca & Meratakan Data (Flattening)

```
 print("2. Mmbaca parquet & flattening...")
        df_raw = spark.read.parquet("file:///opt/airflow/data_lake/order/")

        df_exploded = df_raw.withColumn("product_item", F.explode("products"))

        df_flat = df_exploded.select(
            F.col("order_id").cast("int").alias("order_id"),
            F.col("user_id").cast("int").alias("user_id"),
            F.col("order_number").cast("int").alias("order_number"),
            F.col("order_dow").cast("int").alias("order_dow"),
            F.col("order_hour_of_day").cast("int").alias("order_hour_of_day"),
            F.col("days_since_prior_order").cast("float").alias("days_since_prior_order"),
            F.col("product_item.product_id").cast("int").alias("product_id"),
            F.col("product_item.product_name").cast("string").alias("product_name"),
            F.col("product_item.department").cast("string").alias("department"),
            F.col("product_item.reordered").cast("int").alias("reordered")
        )
```

Tahap ini bertujuan untuk mengubah data mentah menjadi bentuk tabular yang standar (flat table) agar mudah dianalisis.

- `df_raw:` **PySpark** membaca data pesanan mentah berformat Parquet yang sebelumnya disimpan oleh proses `fecth_orders.py` di direktori Data Lake (/opt/airflow/data_lake/order/).

- `df_exploded:` Karena data produk dari **API** berbentuk nested array (bersarang, di mana satu pesanan berisi daftar banyak produk), fungsi `F.explode("products")` digunakan untuk memecah array tersebut. Hasilnya, setiap item produk akan memiliki barisnya sendiri (duplikasi detail pesanan untuk setiap produk di dalamnya).

- `df_flat:` Memilih (select) kolom-kolom yang diperlukan dan melakukan penyesuaian tipe data menjadi tipe yang benar seperti `int, float, dan string.` Proses ini menghasilkan skema data relasional yang sepenuhnya rata (flattened) dan siap untuk dimasukkan ke model Machine Learning atau database.

3. Membuat FP-GROWTH (Frequent Pattern)43Q

```
print("3. Menjalankan FP-GROWTH...")
        df_basket = df_flat.groupBy("order_id").agg(
            F.collect_set("product_name").alias("items")
        )

        fpGrowth = FPGrowth(itemsCol="items", minSupport=0.02, minConfidence=0.1)
        model = fpGrowth.fit(df_basket)

        df_rules = model.associationRules
        df_rules_clean = df_rules.select(
            F.concat_ws(", ", F.col("antecedent")).alias("antecedent"),
            F.concat_ws(", ", F.col("consequent")).alias("consequent"),
            F.col("confidence").cast("float").alias("confidence"),
            F.col("lift").cast("float").alias("lift"),
            F.col("support").cast("float").alias("support")
        )
```

Tahap ini mengimplementasikan algoritma FP-Growth dari library **PySpark MLlib** untuk melakukan **Market Basket Analysis** (analisis keranjang belanja).

- `df_basket:` Sebelum masuk ke model, data yang sudah rata dikelompokkan kembali berdasarkan `order_id`. Fungsi `F.collect_set()` digunakan untuk mengumpulkan semua nama produk dari ID pesanan yang sama ke dalam satu set array tanpa duplikat.

- Membangun model FP-Growth dengan parameter threshold sebagai berikut:

  - `minSupport=0.02:` Artinya, sebuah kombinasi produk hanya akan dianggap sebagai pola yang relevan jika muncul (dibeli secara bersamaan) di minimal 2% dari total keseluruhan transaksi.

  - `minConfidence=0.1:` Artinya, menetapkan probabilitas bersyarat minimum sebesar 10%. Jika pelanggan membeli barang A, harus ada kemungkinan minimal 10% bahwa mereka juga akan membeli barang B.

- `model = fpGrowth.fit(df_basket):` Melatih **fitting** model asosiasi menggunakan dataset keranjang belanja yang sudah disiapkan.

- `df_rules_clean:` Mengekstrak aturan asosiasi (Association Rules) dari model yang telah dilatih. Bagian ini merapikan data dengan mengubah array pada kolom antecedent (produk awal/penyebab) dan consequent (produk rekomendasi/akibat) menjadi teks biasa (string) yang dipisahkan oleh koma menggunakan `F.concat_ws()`. Selain itu, metrik statistik (confidence, lift, support) di cast menjadi float agar kompatibel saat di-insert ke dalam ClickHouse.

4. Convert ke Pandas

```
 print("4. Convert ke pandas...")
        final_results = df_flat.toPandas()
        rules_results = df_rules_clean.toPandas()
```

Pada tahap ini, objek DataFrame milik PySpark (yang sifatnya terdistribusi) dikonversi menjadi Pandas DataFrame lokal.

5. Menghentikan sesi SPARK

```
 spark.stop()
        print("5. SPARK done....")
```

Perintah `spark.stop()` digunakan untuk secara eksplisit menutup sesi Spark dan membebaskan memori (RAM) serta CPU yang sebelumnya dialokasikan untuk pemrosesan data guna mencegah kebocoran memori (memory leaks) dan agar resource server tidak terkunci.

6. Memuat data ke Clickhouse

```
print("6. Memuat ke Clickhouse ...")
    client = Client(host='clickhouse-server', user='admin', password='rahasia')

    client.execute('CREATE DATABASE IF NOT EXISTS analytics')

    # Membuata tbel utama
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.order_items (
            order_id Int32,
            user_id Int32,
            order_number Int32,
            order_dow Int32,
            order_hour_of_day Int32,
            days_since_prior_order Float32,
            product_id Int32,
            product_name String,
            department String,
            reordered Int32
        ) ENGINE = MergeTree()
        ORDER BY (order_dow, order_hour_of_day, order_id)
    ''')

    # membuat Tbl Rules FP-Growth
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.fp_growth_rules (
            antecedent String,
            consequent String,
            confidence Float32,
            lift Float32,
            support Float32
        ) ENGINE = MergeTree()
        ORDER BY (confidence, lift)
    ''')

    # Truncate & Insert data ke clickHouse
    client.execute('TRUNCATE TABLE analytics.order_items')
    data_tuples = [tuple(x) for x in final_results.to_numpy()]
    if data_tuples:
        client.execute('INSERT INTO analytics.order_items VALUES', data_tuples)

    client.execute('TRUNCATE TABLE analytics.fp_growth_rules')
    rules_tuples = [tuple(x) for x in rules_results.to_numpy()]
    if rules_tuples:
        client.execute('INSERT INTO analytics.fp_growth_rules VALUES', rules_tuples)
```

Tahap ini adalah proses memuat data yang sudah bersih dan hasil analisis ke dalam Data Warehouse (ClickHouse).

- Koneksi & Skema: Membuka koneksi ke server ClickHouse dan membuat database analytics jika belum ada.

- Pembuatan Tabel (MergeTree): Membuat tabel `order_items` (untuk data pesanan flat) dan `fp_growth_rules` (untuk hasil rekomendasi produk). Kedua tabel menggunakan engine `MergeTree`, yang merupakan engine ClickHouse yang sangat cepat untuk kueri analitik dan pemrosesan data bervolume besar. Penggunaan `ORDER BY` berfungsi sebagai Primary Key atau indeks untuk mempercepat pencarian data.

- Idempotensi (Truncate & Insert): Sebelum memasukkan data baru, kode menjalankan perintah `TRUNCATE TABLE` untuk mengosongkan tabel. Hal ini memastikan pipeline bersifat idempoten (tidak akan menghasilkan data ganda/duplikat meskipun pipeline dijalankan ulang berkali-kali di Airflow). Data dari `Pandas` kemudian diubah menjadi tuple dan di-insert secara massal ke tabel terkait.

7. Menghapus data yang telah berhasil

```
print("7. Membersihkan file Parquet lama dari Data Lake...")
    for f in glob.glob('/opt/airflow/data_lake/order/*.parquet'):
        try: os.remove(f)
        except OSError as e: print(f"Error: {f} : {e.strerror}")

    print("✅ PIPELINE SELESAI")
```

Setelah dipastikan semua data telah berhasil dimasukkan ke ClickHouse dengan aman, proses terakhir adalah housekeeping.

- `glob.glob & os.remove:` Kode ini mencari semua file berekstensi `.parquet` di dalam direktori penyimpanan sementara (Data Lake) dan menghapusnya.
- Hal ini dilakukan agar ruang penyimpanan (storage) server Airflow tidak membengkak dan penuh oleh file data mentah hasil ingestion (tarikan API) dari waktu ke waktu.

#### 3. pipeline `orders_pipeline.py`

```
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mci_kelompok_20',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'ecommerce_orders_pipeline',
    default_args=default_args,
    schedule_interval='@hourly', # Narik data setiap 1 jam
    catchup=False,
    max_active_runs=1,
    description='Pipeline ETL Data Orders -> Spark -> ClickHouse'
) as dag:

    # ambil data
    ingest_api = BashOperator(
        task_id='fetch_orders_data',
        bash_command='python /opt/airflow/dags/scripts/fetch_orders.py'
    )

    # Load  ClickHouse
    process_spark = BashOperator(
        task_id='process_to_clickhouse',
        bash_command='python -u /opt/airflow/dags/scripts/process_orders_spark.py'
    )

    ingest_api >> process_spark
```

File ini adalah inti dari Apache Airflow DAG (Directed Acyclic Graph) yang bertugas mengorkestrasi (mengatur jadwal dan urutan) kedua script di atas.

- Konfigurasi Jadwal: Pipeline ini diberi nama `ecommerce_orders_pipeline` dan dijadwalkan berjalan secara otomatis setiap satu jam sekali `(schedule_interval='@hourly')`. Jika ada proses yang gagal, Airflow akan mencoba mengulanginya 1 kali setelah jeda 1 menit `(retries: 1, retry_delay: timedelta(minutes=1))`.

- Pipeline ini membungkus kedua file Python menggunakan **BashOperator**:

  - ingest_api: Menjalankan script `fetch_orders.py`.

  - process_spark: Menjalankan script `process_orders_spark.py`.

- `ingest_api >> process_spark`: Ini memastikan bahwa tugas memproses data ke **Spark/ClickHouse** hanya akan berjalan jika tugas penarikan data dari API berhasil diselesaikan lebih dulu.

---

## Membuat Visualisasi & Questions di Metabase

1. Q1 — Peak Order Times

<img width="2004" height="614" alt="Screenshot 2026-05-18 184918" src="https://github.com/user-attachments/assets/e8cbbab5-00b2-4cd6-8933-7a8033c6b746" />

- Query :
```
SELECT
    order_hour_of_day AS hour_of_day,
    uniqExact(order_id) AS total_unique_orders
FROM analytics.order_items
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day ASC;

```
- Visualisasi : Bar Chart

- Penjelasan Question:
Question ini digunakan untuk menganalisis jam terjadinya transaksi paling banyak dalam satu hari. Query menghitung jumlah transaksi unik berdasarkan order_id yang dikelompokkan menurut order_hour_of_day. Fungsi uniqExact(order_id) digunakan karena yang ingin diketahui adalah jumlah order yang berbeda, bukan jumlah baris produk pada setiap transaksi. Dalam ClickHouse, uniqExact() menghasilkan perhitungan jumlah nilai unik secara eksak.

- Penjelasan visualisasi:
Bar Chart menampilkan jam pada sumbu X dan jumlah transaksi unik pada sumbu Y. Setiap batang menunjukkan total order yang terjadi pada jam tertentu. Semakin tinggi batang, semakin banyak transaksi yang terjadi pada jam tersebut.

   
