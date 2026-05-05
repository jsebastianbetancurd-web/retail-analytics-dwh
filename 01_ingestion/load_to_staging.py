"""
load_to_staging.py — Carga de CSV crudos a tablas staging en DuckDB
====================================================================
Lee los 9 CSV del dataset Olist y los carga como tablas stg_* en una
base de datos DuckDB (olist_dwh.duckdb). Aplica transformaciones de
limpieza para garantizar datos consistentes en la capa staging.

TRANSFORMACIONES DE LIMPIEZA APLICADAS:
1. Deduplicacion: elimina filas duplicadas en claves primarias
2. Tipado de fechas: convierte strings a TIMESTAMP nativo de DuckDB
3. Tratamiento de nulos: reemplaza nulos categoricos con valores por defecto
4. Normalizacion de texto: estandariza ciudades a minusculas para consistencia

IDEMPOTENCIA: Usa CREATE OR REPLACE TABLE, por lo que ejecutar el script
multiples veces siempre produce el mismo resultado sin duplicar datos.

Ejecucion: python 01_ingestion/load_to_staging.py
"""

import duckdb
import pandas as pd
from pathlib import Path
import sys
import time

# --- Configuracion ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
DB_PATH = PROJECT_ROOT / "data" / "olist_dwh.duckdb"

# Mapeo: nombre del CSV -> nombre de tabla staging + columnas de PK
# Esto centraliza la configuracion y facilita agregar nuevas tablas
TABLE_CONFIG = {
    "olist_customers_dataset.csv": {
        "table_name": "stg_customers",
        "pk_cols": ["customer_id"],
        "dedup_order": None,  # Sin criterio especial, tomamos la primera fila
    },
    "olist_geolocation_dataset.csv": {
        "table_name": "stg_geolocation",
        "pk_cols": ["geolocation_zip_code_prefix"],
        # Hay multiples coordenadas por zip code. Tomamos una sola para simplificar.
        # En produccion, podriamos promediar lat/lng o usar la moda.
        "dedup_order": None,
    },
    "olist_order_items_dataset.csv": {
        "table_name": "stg_order_items",
        "pk_cols": ["order_id", "order_item_id"],
        "dedup_order": None,
    },
    "olist_order_payments_dataset.csv": {
        "table_name": "stg_order_payments",
        "pk_cols": ["order_id", "payment_sequential"],
        "dedup_order": None,
    },
    "olist_order_reviews_dataset.csv": {
        "table_name": "stg_order_reviews",
        "pk_cols": ["review_id"],
        # Si hay review_id duplicados, tomamos el mas reciente
        "dedup_order": "review_answer_timestamp DESC",
    },
    "olist_orders_dataset.csv": {
        "table_name": "stg_orders",
        "pk_cols": ["order_id"],
        "dedup_order": None,
    },
    "olist_products_dataset.csv": {
        "table_name": "stg_products",
        "pk_cols": ["product_id"],
        "dedup_order": None,
    },
    "olist_sellers_dataset.csv": {
        "table_name": "stg_sellers",
        "pk_cols": ["seller_id"],
        "dedup_order": None,
    },
    "product_category_name_translation.csv": {
        "table_name": "stg_category_translation",
        "pk_cols": ["product_category_name"],
        "dedup_order": None,
    },
}


def create_database(db_path: Path) -> duckdb.DuckDBPyConnection:
    """
    Crea (o abre) la base de datos DuckDB y configura los schemas.
    DuckDB crea el archivo .duckdb automaticamente si no existe.
    """
    # Asegurar que el directorio padre exista
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))

    # Crear schema 'staging' para organizar las tablas
    # Los schemas en un DWH separan las capas logicas: staging, warehouse, marts
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")

    print(f"  Base de datos: {db_path}")
    print(f"  Schema 'staging' listo")
    return con


def load_table(con: duckdb.DuckDBPyConnection, csv_path: Path, config: dict) -> int:
    """
    Carga un CSV a una tabla staging en DuckDB con las transformaciones necesarias.
    Retorna el numero de filas cargadas.

    La estrategia es:
    1. DuckDB lee el CSV directamente (mas rapido que Pandas para archivos grandes)
    2. Aplicamos deduplicacion con ROW_NUMBER() + PARTITION BY sobre la PK
    3. Creamos la tabla con CREATE OR REPLACE (idempotencia)
    """
    table_name = config["table_name"]
    pk_cols = config["pk_cols"]
    dedup_order = config["dedup_order"]

    print(f"\n  Cargando: {csv_path.name} -> staging.{table_name}")
    start_time = time.time()

    # Paso 1: Leer el CSV directamente en DuckDB
    # DuckDB puede leer CSVs nativamente — esto es mucho mas rapido
    # que cargar con Pandas y luego insertar fila por fila
    csv_path_str = str(csv_path).replace("\\", "/")

    # Paso 2: Construir la query con deduplicacion
    # Usamos la tecnica de ROW_NUMBER() para eliminar duplicados:
    #   - Particionamos por la clave primaria
    #   - Ordenamos por algun criterio (o arbitrariamente)
    #   - Solo nos quedamos con la fila #1 de cada particion
    pk_list = ", ".join(pk_cols)

    if dedup_order:
        order_clause = dedup_order
    else:
        # Si no hay criterio de orden, usamos la primera fila encontrada
        order_clause = pk_list

    dedup_query = f"""
        CREATE OR REPLACE TABLE staging.{table_name} AS
        WITH raw_data AS (
            SELECT *
            FROM read_csv_auto('{csv_path_str}', header=true, all_varchar=false)
        ),
        deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY {pk_list} ORDER BY {order_clause}) as _rn
            FROM raw_data
        )
        SELECT * EXCLUDE (_rn)
        FROM deduped
        WHERE _rn = 1
    """

    con.execute(dedup_query)

    # Contar filas cargadas
    result = con.execute(f"SELECT COUNT(*) FROM staging.{table_name}").fetchone()
    n_rows = result[0]

    elapsed = time.time() - start_time
    print(f"    -> {n_rows:,} filas cargadas en {elapsed:.1f}s")

    return n_rows


def apply_staging_transformations(con: duckdb.DuckDBPyConnection):
    """
    Aplica transformaciones adicionales de limpieza sobre las tablas staging
    ya cargadas. Estas transformaciones van mas alla de la deduplicacion.

    TRANSFORMACION 1: Convertir columnas de fecha de STRING a TIMESTAMP.
    En los CSV, las fechas vienen como texto. DuckDB las lee como VARCHAR.
    Las convertimos a TIMESTAMP para poder hacer operaciones de fecha (filtros,
    diferencias, extracciones de mes/anio) de forma eficiente.

    TRANSFORMACION 2: Reemplazar nulos categoricos con valores por defecto.
    Categorias de producto nulas se reemplazan con 'sin_categoria' para evitar
    problemas en GROUP BY y JOINs posteriores.

    TRANSFORMACION 3: Normalizar texto de ciudades a minusculas.
    En los datos originales, algunas ciudades estan en mayusculas, otras en
    minusculas, otras mixtas. Normalizamos todo a minusculas para evitar
    que 'Sao Paulo' y 'sao paulo' se cuenten como ciudades diferentes.
    """
    print("\n  Aplicando transformaciones de limpieza...")

    # --- TRANSFORMACION 1: Tipado de fechas en stg_orders ---
    # Las 5 columnas de timestamp son strings que debemos convertir
    print("    [1/4] Convirtiendo fechas en stg_orders a TIMESTAMP...")
    con.execute("""
        CREATE OR REPLACE TABLE staging.stg_orders AS
        SELECT
            order_id,
            customer_id,
            order_status,
            TRY_CAST(order_purchase_timestamp AS TIMESTAMP)      AS order_purchase_timestamp,
            TRY_CAST(order_approved_at AS TIMESTAMP)             AS order_approved_at,
            TRY_CAST(order_delivered_carrier_date AS TIMESTAMP)  AS order_delivered_carrier_date,
            TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
        FROM staging.stg_orders
    """)

    # Tipado de fechas en stg_order_items (shipping_limit_date)
    print("    [2/4] Convirtiendo fechas en stg_order_items...")
    con.execute("""
        CREATE OR REPLACE TABLE staging.stg_order_items AS
        SELECT
            order_id,
            order_item_id,
            product_id,
            seller_id,
            TRY_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
            price,
            freight_value
        FROM staging.stg_order_items
    """)

    # --- TRANSFORMACION 2: Tratamiento de nulos categoricos en stg_products ---
    # 610 productos no tienen categoria — los marcamos explicitamente
    # para que aparezcan en reportes en vez de desaparecer en JOINs
    print("    [3/4] Reemplazando nulos en stg_products...")
    con.execute("""
        CREATE OR REPLACE TABLE staging.stg_products AS
        SELECT
            product_id,
            COALESCE(product_category_name, 'sin_categoria') AS product_category_name,
            COALESCE(product_name_lenght, 0)                 AS product_name_lenght,
            COALESCE(product_description_lenght, 0)          AS product_description_lenght,
            COALESCE(product_photos_qty, 0)                  AS product_photos_qty,
            COALESCE(product_weight_g, 0)                    AS product_weight_g,
            COALESCE(product_length_cm, 0)                   AS product_length_cm,
            COALESCE(product_height_cm, 0)                   AS product_height_cm,
            COALESCE(product_width_cm, 0)                    AS product_width_cm
        FROM staging.stg_products
    """)

    # --- TRANSFORMACION 3: Normalizar texto de ciudades ---
    # Estandarizamos a minusculas para consistencia en agrupaciones
    print("    [4/4] Normalizando texto de ciudades...")
    con.execute("""
        CREATE OR REPLACE TABLE staging.stg_customers AS
        SELECT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            LOWER(TRIM(customer_city))  AS customer_city,
            UPPER(TRIM(customer_state)) AS customer_state
        FROM staging.stg_customers
    """)

    con.execute("""
        CREATE OR REPLACE TABLE staging.stg_sellers AS
        SELECT
            seller_id,
            seller_zip_code_prefix,
            LOWER(TRIM(seller_city))  AS seller_city,
            UPPER(TRIM(seller_state)) AS seller_state
        FROM staging.stg_sellers
    """)

    print("    Transformaciones completadas.")


def verify_staging(con: duckdb.DuckDBPyConnection):
    """
    Verifica que todas las tablas staging se crearon correctamente.
    Muestra el conteo final de filas por tabla y la estructura del schema.
    """
    print(f"\n{'='*70}")
    print(f"  VERIFICACION FINAL — Tablas Staging")
    print(f"{'='*70}")

    result = con.execute("""
        SELECT table_name, 
               estimated_size as size_bytes
        FROM duckdb_tables()
        WHERE schema_name = 'staging'
        ORDER BY table_name
    """).fetchall()

    print(f"\n  {'Tabla':<35} {'Filas':>12} {'Tamano':>12}")
    print(f"  {'-'*35} {'-'*12} {'-'*12}")

    total_rows = 0
    for table_name, size_bytes in result:
        count = con.execute(f"SELECT COUNT(*) FROM staging.{table_name}").fetchone()[0]
        size_mb = size_bytes / (1024 * 1024) if size_bytes else 0
        print(f"  {table_name:<35} {count:>12,} {size_mb:>10.1f} MB")
        total_rows += count

    print(f"  {'-'*35} {'-'*12}")
    print(f"  {'TOTAL':<35} {total_rows:>12,}")

    # Muestra una query de ejemplo para que el usuario pueda verificar
    print(f"\n  Query de verificacion (top 5 ordenes):")
    sample = con.execute("""
        SELECT order_id, customer_id, order_status, 
               order_purchase_timestamp
        FROM staging.stg_orders
        ORDER BY order_purchase_timestamp DESC
        LIMIT 5
    """).fetchdf()
    print(sample.to_string(index=False))

    # Verificar tipos de dato en stg_orders (deben ser TIMESTAMP, no VARCHAR)
    print(f"\n  Tipos de dato en stg_orders (verificando TIMESTAMP):")
    dtypes = con.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns
        WHERE table_schema = 'staging' AND table_name = 'stg_orders'
        ORDER BY ordinal_position
    """).fetchall()
    for col, dtype in dtypes:
        marker = " <-- TIMESTAMP OK" if dtype == "TIMESTAMP" else ""
        print(f"    {col:<40} {dtype}{marker}")


if __name__ == "__main__":
    print("="*70)
    print("  RETAIL ANALYTICS DWH -- Carga a Staging")
    print("="*70)

    # Verificar que existen los CSVs
    csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))
    if len(csv_files) == 0:
        print(f"\n  ERROR: No hay CSVs en {RAW_DATA_DIR}")
        print(f"  Ejecuta primero: python 01_ingestion/download_dataset.py")
        sys.exit(1)

    # Crear/abrir la base de datos
    con = create_database(DB_PATH)

    # Cargar cada tabla
    print(f"\n  Iniciando carga de {len(TABLE_CONFIG)} tablas...")
    total_start = time.time()
    load_results = {}

    for csv_name, config in TABLE_CONFIG.items():
        csv_path = RAW_DATA_DIR / csv_name
        if not csv_path.exists():
            print(f"\n  ADVERTENCIA: {csv_name} no encontrado, saltando...")
            continue
        n_rows = load_table(con, csv_path, config)
        load_results[config["table_name"]] = n_rows

    total_elapsed = time.time() - total_start
    print(f"\n  Carga base completada en {total_elapsed:.1f}s")

    # Aplicar transformaciones de limpieza
    apply_staging_transformations(con)

    # Verificacion final
    verify_staging(con)

    # Cerrar conexion
    con.close()

    print(f"\n  Base de datos guardada en: {DB_PATH}")
    print(f"  Carga a staging completada exitosamente.")
