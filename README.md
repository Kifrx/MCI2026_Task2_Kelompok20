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
