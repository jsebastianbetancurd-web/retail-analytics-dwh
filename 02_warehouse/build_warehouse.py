"""
build_warehouse.py — Construye el star schema desde las tablas staging
======================================================================
Ejecuta los scripts SQL en orden para crear y poblar el modelo dimensional:
  1. create_schema.sql  -> DDL (crea tablas vacias)
  2. dim_date.sql       -> Pobla dimension de calendario
  3. dim_customers.sql  -> Pobla dimension de clientes
  4. dim_products.sql   -> Pobla dimension de productos
  5. dim_sellers.sql    -> Pobla dimension de vendedores
  6. fact_orders.sql    -> Pobla tabla de hechos (depende de todas las dims)

El orden importa: las dimensiones DEBEN existir antes de la fact table,
porque fact_orders hace lookup a las surrogate keys de cada dimension.

Ejecucion: python 02_warehouse/build_warehouse.py
"""

import duckdb
from pathlib import Path
import time
import sys

# --- Configuracion ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "olist_dwh.duckdb"
SQL_DIR = Path(__file__).resolve().parent  # Carpeta 02_warehouse/

# Orden de ejecucion de los scripts SQL
# IMPORTANTE: las dimensiones van primero, la fact table al final
SQL_SCRIPTS = [
    ("create_schema.sql",   "Creando esquema y tablas"),
    ("dim_date.sql",        "Poblando dim_date"),
    ("dim_customers.sql",   "Poblando dim_customers"),
    ("dim_products.sql",    "Poblando dim_products"),
    ("dim_sellers.sql",     "Poblando dim_sellers"),
    ("fact_orders.sql",     "Poblando fact_orders"),
]


def execute_sql_file(con: duckdb.DuckDBPyConnection, sql_path: Path, description: str):
    """
    Lee y ejecuta un archivo SQL contra la base de datos DuckDB.
    Muestra el tiempo de ejecucion para cada paso.
    """
    print(f"\n  [{description}]")
    print(f"    Archivo: {sql_path.name}")

    # Leer el contenido SQL
    sql_content = sql_path.read_text(encoding="utf-8")

    start = time.time()
    con.execute(sql_content)
    elapsed = time.time() - start

    print(f"    Completado en {elapsed:.2f}s")


def verify_warehouse(con: duckdb.DuckDBPyConnection):
    """
    Verificacion final del star schema construido.
    Muestra conteos, un JOIN de verificacion y el resumen del modelo.
    """
    print(f"\n{'='*70}")
    print(f"  VERIFICACION DEL STAR SCHEMA")
    print(f"{'='*70}")

    # Conteo de filas por tabla
    warehouse_tables = [
        "dim_date", "dim_customers", "dim_products", "dim_sellers", "fact_orders"
    ]

    print(f"\n  {'Tabla':<25} {'Filas':>12} {'Tipo':>12}")
    print(f"  {'-'*25} {'-'*12} {'-'*12}")

    for table in warehouse_tables:
        count = con.execute(f"SELECT COUNT(*) FROM warehouse.{table}").fetchone()[0]
        tipo = "Dimension" if table.startswith("dim_") else "Fact"
        print(f"  {table:<25} {count:>12,} {tipo:>12}")

    # Query de verificacion: Revenue por estado (senal de cierre de etapa)
    print(f"\n  QUERY DE VERIFICACION: Revenue total por estado (Top 5)")
    print(f"  {'='*60}")
    result = con.execute("""
        SELECT
            dc.customer_state                          AS estado,
            COUNT(DISTINCT f.order_id)                 AS total_ordenes,
            ROUND(SUM(f.price), 2)                     AS revenue_total,
            ROUND(AVG(f.price), 2)                     AS ticket_promedio,
            ROUND(AVG(f.days_to_deliver), 1)           AS dias_entrega_avg
        FROM warehouse.fact_orders f
        INNER JOIN warehouse.dim_customers dc ON f.customer_key = dc.customer_key
        GROUP BY dc.customer_state
        ORDER BY revenue_total DESC
        LIMIT 5
    """).fetchdf()
    print(result.to_string(index=False))

    # Verificar integridad referencial: todas las FKs deben tener match
    print(f"\n  INTEGRIDAD REFERENCIAL:")
    checks = [
        ("customer_key", "dim_customers"),
        ("product_key", "dim_products"),
        ("seller_key", "dim_sellers"),
        ("order_date_key", "dim_date"),
    ]

    for fk_col, dim_table in checks:
        pk_col = fk_col.replace("order_date_key", "date_key")  # Ajuste para dim_date
        orphans = con.execute(f"""
            SELECT COUNT(*)
            FROM warehouse.fact_orders f
            LEFT JOIN warehouse.{dim_table} d ON f.{fk_col} = d.{pk_col}
            WHERE d.{pk_col} IS NULL
        """).fetchone()[0]
        status = "OK" if orphans == 0 else f"!! {orphans:,} huerfanos"
        print(f"    fact_orders.{fk_col:<20} -> {dim_table:<20} {status}")

    # Resumen de metricas de entrega
    print(f"\n  METRICAS DE ENTREGA (ordenes entregadas):")
    delivery = con.execute("""
        SELECT
            COUNT(*) AS ordenes_entregadas,
            ROUND(AVG(days_to_deliver), 1) AS dias_promedio,
            ROUND(AVG(delivery_delay_days), 1) AS delay_promedio,
            SUM(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) AS entregas_tarde,
            ROUND(100.0 * SUM(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_tarde
        FROM warehouse.fact_orders
        WHERE days_to_deliver IS NOT NULL
    """).fetchone()
    print(f"    Ordenes entregadas:   {delivery[0]:,}")
    print(f"    Dias promedio:        {delivery[1]}")
    print(f"    Delay promedio:       {delivery[2]} dias")
    print(f"    Entregas con retraso: {delivery[3]:,} ({delivery[4]}%)")


if __name__ == "__main__":
    print("="*70)
    print("  RETAIL ANALYTICS DWH -- Construccion del Star Schema")
    print("="*70)

    # Verificar que existe la base de datos con staging
    if not DB_PATH.exists():
        print(f"\n  ERROR: No se encontro {DB_PATH}")
        print(f"  Ejecuta primero: python 01_ingestion/load_to_staging.py")
        sys.exit(1)

    # Conectar a la base de datos
    con = duckdb.connect(str(DB_PATH))
    print(f"\n  Conectado a: {DB_PATH}")

    # Ejecutar cada script SQL en orden
    total_start = time.time()
    for sql_file, description in SQL_SCRIPTS:
        sql_path = SQL_DIR / sql_file
        if not sql_path.exists():
            print(f"\n  ERROR: No se encontro {sql_path}")
            sys.exit(1)
        execute_sql_file(con, sql_path, description)

    total_elapsed = time.time() - total_start
    print(f"\n  Star schema construido en {total_elapsed:.2f}s")

    # Verificacion final
    verify_warehouse(con)

    con.close()
    print(f"\n  Warehouse listo. Base de datos: {DB_PATH}")
