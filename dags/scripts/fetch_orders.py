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