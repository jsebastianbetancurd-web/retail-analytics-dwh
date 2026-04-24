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
    La API de Kaggle busca el token en la variable KAGGLE_API_TOKEN.
    Nunca almacenamos credenciales en el código fuente.
    """
    token = os.environ.get("KAGGLE_API_TOKEN")
    if not token:
        print("❌ ERROR: La variable de entorno KAGGLE_API_TOKEN no está configurada.")
        print("   Configúrala con: $env:KAGGLE_API_TOKEN = 'tu_token_aquí'")
        sys.exit(1)
    print(f"✅ Token de Kaggle detectado (últimos 4 caracteres: ...{token[-4:]})")


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

    # Paso 1: Verificar credenciales
    setup_kaggle_credentials()

    # Paso 2: Descargar y descomprimir
    download_and_extract()

    # Paso 3: Verificar que todo esté en orden
    verify_download()

    print("\n✅ Etapa de descarga completada. Puedes continuar con validate_raw.py")
