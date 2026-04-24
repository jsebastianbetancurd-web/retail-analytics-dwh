"""
validate_raw.py — Validacion de calidad sobre los CSV crudos
=============================================================
Genera un reporte de calidad de datos para cada archivo CSV del dataset Olist.
El reporte incluye: conteo de filas, nulos por columna, duplicados en claves
primarias, y tipos de datos inferidos.

Este paso se ejecuta ANTES de cargar a staging, para detectar problemas
y documentar las transformaciones de limpieza necesarias.

Ejecucion: python 01_ingestion/validate_raw.py
"""

import pandas as pd
from pathlib import Path
import sys

# --- Configuracion ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Definimos las claves primarias esperadas para cada tabla.
# Esto nos permite verificar unicidad — un concepto fundamental en DWH.
# Si hay duplicados en la PK, significa que los datos tienen problemas de calidad.
PRIMARY_KEYS = {
    "olist_customers_dataset.csv": ["customer_id"],
    "olist_geolocation_dataset.csv": ["geolocation_zip_code_prefix"],  # No es unica, multiples coords por zip
    "olist_order_items_dataset.csv": ["order_id", "order_item_id"],    # PK compuesta: orden + item
    "olist_order_payments_dataset.csv": ["order_id", "payment_sequential"],  # PK compuesta
    "olist_order_reviews_dataset.csv": ["review_id"],
    "olist_orders_dataset.csv": ["order_id"],
    "olist_products_dataset.csv": ["product_id"],
    "olist_sellers_dataset.csv": ["seller_id"],
    "product_category_name_translation.csv": ["product_category_name"],
}


def validate_file(filepath: Path) -> dict:
    """
    Ejecuta todas las validaciones sobre un archivo CSV.
    Retorna un diccionario con los resultados del diagnostico.
    """
    filename = filepath.name
    print(f"\n{'='*70}")
    print(f"  ARCHIVO: {filename}")
    print(f"{'='*70}")

    # Leer el CSV completo
    df = pd.read_csv(filepath)

    # --- 1. Conteo de filas y columnas ---
    n_rows, n_cols = df.shape
    print(f"\n  [DIMENSIONES]")
    print(f"    Filas:    {n_rows:,}")
    print(f"    Columnas: {n_cols}")

    # --- 2. Tipos de datos inferidos por Pandas ---
    # Esto nos dice como Pandas interpreta cada columna.
    # Comparar con el tipo esperado nos ayuda a detectar problemas
    # (ej: una columna numerica que Pandas lee como string = datos sucios)
    print(f"\n  [TIPOS DE DATOS]")
    for col in df.columns:
        dtype = df[col].dtype
        print(f"    {col:<45} {str(dtype):<20}")

    # --- 3. Analisis de valores nulos ---
    # Los nulos son el problema #1 de calidad de datos.
    # Aqui contamos cuantos hay por columna y que porcentaje representan.
    print(f"\n  [VALORES NULOS]")
    null_counts = df.isnull().sum()
    has_nulls = False
    for col in df.columns:
        nulls = null_counts[col]
        if nulls > 0:
            pct = (nulls / n_rows) * 100
            print(f"    {col:<45} {nulls:>7,} nulos ({pct:.1f}%)")
            has_nulls = True
    if not has_nulls:
        print(f"    Sin valores nulos -- datos completos")

    # --- 4. Verificacion de duplicados en clave primaria ---
    # En un DWH, las claves primarias DEBEN ser unicas.
    # Si hay duplicados, necesitamos decidir: deduplicar o investigar.
    pk_cols = PRIMARY_KEYS.get(filename, [])
    print(f"\n  [CLAVE PRIMARIA: {pk_cols}]")
    if pk_cols:
        duplicates = df.duplicated(subset=pk_cols, keep=False).sum()
        if duplicates > 0:
            pct = (duplicates / n_rows) * 100
            print(f"    !! DUPLICADOS ENCONTRADOS: {duplicates:,} filas ({pct:.1f}%)")
            # Mostrar ejemplo de duplicados para diagnostico
            dup_sample = df[df.duplicated(subset=pk_cols, keep=False)].head(4)
            print(f"    Ejemplo de duplicados:")
            for _, row in dup_sample.iterrows():
                pk_values = {col: row[col] for col in pk_cols}
                print(f"      {pk_values}")
        else:
            print(f"    Sin duplicados -- clave primaria unica")
    else:
        print(f"    No se definio clave primaria para esta tabla")

    # --- 5. Resumen estadistico de columnas numericas ---
    print(f"\n  [ESTADISTICAS NUMERICAS]")
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    if len(numeric_cols) > 0:
        stats = df[numeric_cols].describe().round(2)
        # Mostrar min, max, mean para cada columna numerica
        for col in numeric_cols:
            col_min = stats.loc["min", col] if col in stats.columns else "N/A"
            col_max = stats.loc["max", col] if col in stats.columns else "N/A"
            col_mean = stats.loc["mean", col] if col in stats.columns else "N/A"
            print(f"    {col:<35} min={col_min:<12} max={col_max:<12} mean={col_mean}")
    else:
        print(f"    Sin columnas numericas")

    # --- 6. Valores unicos en columnas categoricas clave ---
    print(f"\n  [CARDINALIDAD]")
    for col in df.columns:
        n_unique = df[col].nunique()
        print(f"    {col:<45} {n_unique:>7,} valores unicos")

    return {
        "archivo": filename,
        "filas": n_rows,
        "columnas": n_cols,
        "nulos_totales": null_counts.sum(),
        "tiene_duplicados_pk": duplicates > 0 if pk_cols else None,
    }


def print_summary(results: list):
    """
    Imprime un resumen consolidado de todas las validaciones.
    Este resumen es util para documentar en el README del proyecto.
    """
    print(f"\n\n{'='*70}")
    print(f"  RESUMEN CONSOLIDADO DE VALIDACION")
    print(f"{'='*70}")
    print(f"\n  {'Archivo':<45} {'Filas':>10} {'Nulos':>8} {'Dup PK':>8}")
    print(f"  {'-'*45} {'-'*10} {'-'*8} {'-'*8}")

    total_rows = 0
    total_nulls = 0

    for r in results:
        dup_str = "SI" if r["tiene_duplicados_pk"] else ("No" if r["tiene_duplicados_pk"] is not None else "N/A")
        print(f"  {r['archivo']:<45} {r['filas']:>10,} {r['nulos_totales']:>8,} {dup_str:>8}")
        total_rows += r["filas"]
        total_nulls += r["nulos_totales"]

    print(f"  {'-'*45} {'-'*10} {'-'*8}")
    print(f"  {'TOTAL':<45} {total_rows:>10,} {total_nulls:>8,}")

    # Hallazgos clave para documentar
    print(f"\n  HALLAZGOS CLAVE:")
    print(f"  1. El dataset tiene {total_rows:,} registros en total")
    print(f"  2. Hay {total_nulls:,} valores nulos que necesitan tratamiento")

    tables_with_dups = [r["archivo"] for r in results if r["tiene_duplicados_pk"]]
    if tables_with_dups:
        print(f"  3. Tablas con duplicados en PK: {', '.join(tables_with_dups)}")
        print(f"     -> Estos duplicados se eliminaran en la carga a staging")
    else:
        print(f"  3. Ninguna tabla tiene duplicados en su clave primaria")


if __name__ == "__main__":
    print("="*70)
    print("  RETAIL ANALYTICS DWH -- Validacion de Datos Crudos")
    print("="*70)

    # Verificar que existen los archivos
    csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))
    if not csv_files:
        print(f"\n  ERROR: No se encontraron archivos CSV en {RAW_DATA_DIR}")
        print(f"  Ejecuta primero: python 01_ingestion/download_dataset.py")
        sys.exit(1)

    print(f"\n  Archivos encontrados: {len(csv_files)}")

    # Ejecutar validaciones sobre cada archivo
    results = []
    for csv_file in csv_files:
        result = validate_file(csv_file)
        results.append(result)

    # Resumen consolidado
    print_summary(results)

    print(f"\n  Validacion completada. Revisa los hallazgos antes de cargar a staging.")
