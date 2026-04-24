# 📊 Guía de Conexión: Power BI → DuckDB

## Prerrequisitos

1. **Power BI Desktop** instalado (gratuito desde Microsoft Store)
2. **ODBC Driver para DuckDB** — [Descargar aquí](https://duckdb.org/docs/api/odbc/overview.html)

---

## Opción 1: Conexión directa via ODBC

### Paso 1: Instalar el driver ODBC de DuckDB
- Descargar desde: https://duckdb.org/docs/api/odbc/overview.html
- Ejecutar el instalador `.msi` para Windows

### Paso 2: Configurar el DSN
1. Abrir "ODBC Data Sources (64-bit)" desde Windows
2. En la pestaña "User DSN", click en "Add"
3. Seleccionar "DuckDB Driver"
4. Configurar:
   - **Database**: `C:\...\retail-analytics-dwh\data\olist_dwh.duckdb`
   - **Read Only**: Yes

### Paso 3: Conectar desde Power BI
1. Abrir Power BI Desktop
2. **Get Data** → **ODBC** → Seleccionar el DSN creado
3. Navegar al schema `analytics_marts`
4. Seleccionar las 3 tablas mart:
   - `mart_sales_by_region`
   - `mart_product_performance`
   - `mart_delivery_kpis`

---

## Opción 2: Exportar a CSV/Parquet (más simple)

Si tienes problemas con ODBC, puedes exportar los marts directamente:

```bash
# Activar el entorno
.venv\Scripts\activate

# Exportar marts a Parquet (recomendado para Power BI)
python -c "
import duckdb
con = duckdb.connect('data/olist_dwh.duckdb', read_only=True)
for table in ['mart_sales_by_region', 'mart_product_performance', 'mart_delivery_kpis']:
    con.execute(f\"COPY (SELECT * FROM analytics_marts.{table}) TO '05_dashboards/{table}.parquet' (FORMAT PARQUET)\")
    print(f'Exported {table}.parquet')
con.close()
"
```

Luego en Power BI: **Get Data** → **Parquet** → Seleccionar los archivos.

---

## Tablas disponibles para el Dashboard

| Tabla | Filas | Descripción |
|-------|-------|-------------|
| `mart_sales_by_region` | ~4,100 | KPIs de ventas por estado y ciudad |
| `mart_product_performance` | ~200 | Performance por categoría y banda de precio |
| `mart_delivery_kpis` | ~500 | KPIs de logística por estado y mes |

---

## Métricas DAX sugeridas

```dax
// Revenue Total
Total Revenue = SUM(mart_sales_by_region[total_revenue])

// On-Time Delivery Rate
On-Time Rate = AVERAGE(mart_delivery_kpis[on_time_rate_pct])

// Ticket Promedio
Avg Ticket = DIVIDE([Total Revenue], SUM(mart_sales_by_region[total_orders]))

// Review Score Promedio
Avg Review = AVERAGE(mart_sales_by_region[avg_review_score])
```

---

## Dashboard Sugerido

### Página 1: Resumen Ejecutivo
- KPI Cards: Revenue, Ordenes, Ticket Promedio, Review Score
- Mapa de Brasil por estado (revenue por color)
- Tendencia mensual de revenue

### Página 2: Performance de Productos
- Top 10 categorías por revenue
- Distribución por banda de precio
- Scatter: precio vs review score

### Página 3: Logística y Entregas
- On-Time Rate por estado
- Heatmap: mes × estado (dias de entrega)
- Delay promedio cuando llega tarde
