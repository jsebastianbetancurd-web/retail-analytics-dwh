"""Verificacion rapida del staging — ejecutar despues de load_to_staging.py"""
import duckdb

con = duckdb.connect("data/olist_dwh.duckdb", read_only=True)

# Conteo por tabla staging
print("=== CONTEO POR TABLA STAGING ===")
tables = con.execute(
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = 'staging' ORDER BY table_name"
).fetchall()

for (t,) in tables:
    count = con.execute(f"SELECT COUNT(*) FROM staging.{t}").fetchone()[0]
    print(f"  staging.{t:<30} {count:>10,} filas")

# Verificacion de JOIN: orders + customers (integridad referencial)
print()
print("=== VERIFICACION DE JOIN (orders + customers) ===")
result = con.execute("""
    SELECT 
        c.customer_state,
        COUNT(DISTINCT o.order_id) as total_ordenes,
        COUNT(DISTINCT c.customer_unique_id) as total_clientes
    FROM staging.stg_orders o
    JOIN staging.stg_customers c ON o.customer_id = c.customer_id
    GROUP BY c.customer_state
    ORDER BY total_ordenes DESC
    LIMIT 5
""").fetchdf()
print(result.to_string(index=False))

con.close()
print("\nVerificacion completada.")
