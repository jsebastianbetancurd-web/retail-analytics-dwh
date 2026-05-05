"""
download_dataset.py — Descarga del dataset Olist desde Kaggle
=============================================================
Descarga y descomprime los 9 CSV del Brazilian E-Commerce Dataset
directamente en la carpeta data/raw/ del proyecto.

Requisito: Variable de entorno KAGGLE_API_TOKEN configurada.
Ejecución: python 01_ingestion/download_dataset.py
"""

import os
import sys
import zipfile
from pathlib import Path

# --- Configuración ---
# Nombre del dataset en Kaggle (owner/dataset-name)
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"

# Ruta de destino para los CSV crudos (relativa a la raíz del proyecto)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def setup_kaggle_credentials():
    """
    Configura las credenciales de Kaggle desde la variable de entorno.
    """
    if os.environ.get("MOCK_DATA") == "true":
        print("🛠️  MODO MOCK ACTIVADO: Se generarán datos sintéticos para demostración.")
        return True

    token = os.environ.get("KAGGLE_API_TOKEN")
    if not token:
        print("⚠️  ADVERTENCIA: KAGGLE_API_TOKEN no configurado.")
        print("   Para demostración sin API, usa: $env:MOCK_DATA = 'true'")
        return False
    print(f"✅ Token de Kaggle detectado (últimos 4 caracteres: ...{token[-4:]})")
    return True


def download_and_extract():
    """
    Descarga el dataset como ZIP y lo descomprime en data/raw/.
    Si los archivos ya existen, pregunta al usuario si quiere reemplazarlos.
    Esto asegura idempotencia: ejecutar el script múltiples veces
    no genera duplicados ni estados inconsistentes.
    """
    # Crear directorio de destino si no existe
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Verificar si ya existen archivos CSV descargados
    existing_csvs = list(RAW_DATA_DIR.glob("*.csv"))
    if existing_csvs:
        print(f"\n⚠️  Ya existen {len(existing_csvs)} archivos CSV en {RAW_DATA_DIR}:")
        for csv_file in sorted(existing_csvs):
            size_mb = csv_file.stat().st_size / (1024 * 1024)
            print(f"   📄 {csv_file.name} ({size_mb:.1f} MB)")

        respuesta = input("\n¿Deseas reemplazarlos? (s/n): ").strip().lower()
        if respuesta != "s":
            print("⏭️  Descarga cancelada. Usando archivos existentes.")
            return

    print(f"\n📥 Descargando dataset: {KAGGLE_DATASET}...")
    print(f"   Destino: {RAW_DATA_DIR}")

    try:
        # Importamos kaggle aquí para que el error de credenciales
        # se muestre antes de intentar importar la librería
        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()

        # Descargar el dataset como ZIP
        api.dataset_download_files(
            KAGGLE_DATASET,
            path=str(RAW_DATA_DIR),
            unzip=True,  # Descomprimir automáticamente
        )

        print("✅ Dataset descargado y descomprimido exitosamente.")

    except Exception as e:
        print(f"❌ Error al descargar: {e}")
        print("\n💡 Alternativa manual:")
        print(f"   1. Ve a: https://www.kaggle.com/datasets/{KAGGLE_DATASET}")
        print(f"   2. Descarga el ZIP manualmente")
        print(f"   3. Descomprime los CSV en: {RAW_DATA_DIR}")
        sys.exit(1)


def generate_mock_data():
    """
    Genera archivos CSV mínimos para que el pipeline pueda ejecutarse en modo demo.
    """
    import pandas as pd
    import numpy as np
    
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"📝 Generando datos mock en {RAW_DATA_DIR}...")
    
    # 10 orders across 3 states to ensure plots work
    order_ids = [f'o{i}' for i in range(1, 11)]
    cust_ids = [f'c{i}' for i in range(1, 11)]
    states = ['SP', 'SP', 'RJ', 'RJ', 'MG', 'MG', 'SP', 'RJ', 'SP', 'MG']
    cities = ['sao paulo', 'sao paulo', 'rio', 'rio', 'belo horizonte', 'belo horizonte', 'sao paulo', 'rio', 'sao paulo', 'belo horizonte']
    
    orders = pd.DataFrame({
        'order_id': order_ids,
        'customer_id': cust_ids,
        'order_status': ['delivered'] * 10,
        'order_purchase_timestamp': [f'2024-01-{i:02d} 10:00:00' for i in range(1, 11)],
        'order_approved_at': [f'2024-01-{i:02d} 10:05:00' for i in range(1, 11)],
        'order_delivered_carrier_date': [f'2024-01-{i:02d} 15:00:00' for i in range(1, 11)],
        'order_delivered_customer_date': [f'2024-01-{i+2:02d} 10:00:00' for i in range(1, 11)],
        'order_estimated_delivery_date': [f'2024-01-{i+5:02d} 00:00:00' for i in range(1, 11)]
    })
    orders.to_csv(RAW_DATA_DIR / "olist_orders_dataset.csv", index=False)
    
    pd.DataFrame({
        'customer_id': cust_ids,
        'customer_unique_id': [f'u{i}' for i in range(1, 11)],
        'customer_zip_code_prefix': range(100, 110),
        'customer_city': cities,
        'customer_state': states
    }).to_csv(RAW_DATA_DIR / "olist_customers_dataset.csv", index=False)
    
    pd.DataFrame({
        'order_id': order_ids,
        'order_item_id': [1] * 10,
        'product_id': [f'p{i%3+1}' for i in range(10)],
        'seller_id': [f's{i%2+1}' for i in range(10)],
        'shipping_limit_date': [f'2024-01-{i+5:02d}' for i in range(1, 11)],
        'price': [100.0, 150.0, 200.0] * 3 + [100.0],
        'freight_value': [10.0, 15.0, 20.0] * 3 + [10.0]
    }).to_csv(RAW_DATA_DIR / "olist_order_items_dataset.csv", index=False)
    
    pd.DataFrame({
        'product_id': ['p1', 'p2', 'p3'],
        'product_category_name': ['perfumaria', 'artes', 'esporte_lazer'],
        'product_name_lenght': [10, 20, 30],
        'product_description_lenght': [100, 200, 300],
        'product_photos_qty': [1, 2, 3],
        'product_weight_g': [100, 200, 300],
        'product_length_cm': [10, 20, 30],
        'product_height_cm': [10, 20, 30],
        'product_width_cm': [10, 20, 30]
    }).to_csv(RAW_DATA_DIR / "olist_products_dataset.csv", index=False)
    
    pd.DataFrame({
        'seller_id': ['s1', 's2'],
        'seller_zip_code_prefix': [123, 456],
        'seller_city': ['sao paulo', 'curitiba'],
        'seller_state': ['SP', 'PR']
    }).to_csv(RAW_DATA_DIR / "olist_sellers_dataset.csv", index=False)
    
    pd.DataFrame({
        'product_category_name': ['perfumaria', 'artes', 'esporte_lazer'],
        'product_category_name_english': ['perfumery', 'arts', 'sports_leisure']
    }).to_csv(RAW_DATA_DIR / "product_category_name_translation.csv", index=False)
    
    pd.DataFrame({
        'order_id': order_ids,
        'payment_sequential': [1] * 10,
        'payment_type': ['credit_card'] * 7 + ['boleto'] * 3,
        'payment_installments': [1] * 10,
        'payment_value': [110.0, 165.0, 220.0] * 3 + [110.0]
    }).to_csv(RAW_DATA_DIR / "olist_order_payments_dataset.csv", index=False)
    
    pd.DataFrame({
        'review_id': [f'r{i}' for i in range(1, 11)],
        'order_id': order_ids,
        'review_score': [5, 4, 3, 5, 4, 3, 5, 4, 3, 5],
        'review_comment_title': ['ok'] * 10,
        'review_comment_message': ['ok'] * 10,
        'review_creation_date': [f'2024-01-{i+3:02d}' for i in range(1, 11)],
        'review_answer_timestamp': [f'2024-01-{i+3:02d} 10:00:00' for i in range(1, 11)]
    }).to_csv(RAW_DATA_DIR / "olist_order_reviews_dataset.csv", index=False)
    
    pd.DataFrame({
        'geolocation_zip_code_prefix': [100, 101, 102, 123, 456],
        'geolocation_lat': [-23.5] * 5,
        'geolocation_lng': [-46.6] * 5,
        'geolocation_city': ['sao paulo'] * 5,
        'geolocation_state': ['SP'] * 5
    }).to_csv(RAW_DATA_DIR / "olist_geolocation_dataset.csv", index=False)

    print("✅ Datos mock generados exitosamente.")


def verify_download():
    """
    Verifica que todos los archivos esperados del dataset están presentes.
    Los 9 archivos CSV son los componentes del dataset de Olist.
    """
    # Archivos esperados del dataset Olist
    expected_files = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv",
    ]

    print("\n📋 Verificación de archivos descargados:")
    all_present = True
    total_size_mb = 0

    for filename in expected_files:
        filepath = RAW_DATA_DIR / filename
        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
            total_size_mb += size_mb
            print(f"   ✅ {filename} ({size_mb:.1f} MB)")
        else:
            print(f"   ❌ {filename} — NO ENCONTRADO")
            all_present = False

    print(f"\n   📦 Tamaño total: {total_size_mb:.1f} MB")

    if all_present:
        print(f"\n🎉 Los 9 archivos están listos en {RAW_DATA_DIR}")
    else:
        print("\n⚠️  Faltan algunos archivos. Verifica la descarga.")
        sys.exit(1)

    return all_present


if __name__ == "__main__":
    print("=" * 60)
    print("  RETAIL ANALYTICS DWH — Descarga del Dataset Olist")
    print("=" * 60)

    # Paso 1: Verificar credenciales o modo mock
    has_creds = setup_kaggle_credentials()
    is_mock = os.environ.get("MOCK_DATA") == "true"

    if is_mock:
        generate_mock_data()
    elif has_creds:
        download_and_extract()
    else:
        print("❌ Error: Se requieren credenciales de Kaggle o activar MOCK_DATA=true")
        sys.exit(1)

    # Paso 3: Verificar que todo esté en orden
    verify_download()

    print("\n✅ Etapa de descarga completada. Puedes continuar con validate_raw.py")
