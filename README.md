# 📊 Retail Analytics Data Warehouse

**Data Warehouse de retail con pipeline ETL/ELT completo**.

> Simulación de un entorno analítico para un equipo de retail.

[![Python](https://img.shields.io/badge/Python-3.14-blue)](https://python.org)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.5.2-yellow)](https://duckdb.org)
[![dbt](https://img.shields.io/badge/dbt_Core-1.11.8-orange)](https://getdbt.com)
[![Tests](https://img.shields.io/badge/dbt_tests-23%20PASS-brightgreen)](https://getdbt.com)

---

## 🚀 Cómo Ejecutar el Proyecto (Rápido)

Este proyecto está diseñado para ser probado fácilmente, incluso sin credenciales de Kaggle o dbt configurado.

### 1. Instalación de Dependencias
```bash
pip install -r requirements.txt
```

### 2. Ejecución "One-Click" (Recomendado para Entrevistas)
Ejecuta el pipeline completo (ingesta mock, staging, warehouse y análisis) con un solo comando:
```bash
python run_all.py
```
*Este comando genera datos sintéticos automáticamente para que puedas ver el DWH y las visualizaciones funcionando de inmediato.*

---

## 🎯 Problema de Negocio

Un equipo de retail necesita insights accionables sobre:
- **Ventas:** ¿Cuál es el revenue por región, categoría y período? ¿Cuál es el ticket promedio?
- **Clientes:** ¿Dónde se concentran los clientes? ¿Cómo varía la satisfacción por región?
- **Logística:** ¿Cuánto tarda la entrega promedio? ¿Qué estados tienen peor performance logístico?

Este proyecto construye la infraestructura de datos que responde esas preguntas, desde la ingesta de raw data hasta visualizaciones para decisiones ejecutivas.

---

## 📈 Resultados Clave

| KPI | Valor |
|-----|-------|
| 🛒 Total Órdenes | 98,666 |
| 💰 Revenue Total | R$ 13,591,643.70 |
| 🎫 Ticket Promedio | R$ 120.65 |
| ⭐ Review Score Promedio | 4.04 / 5.0 |
| 📦 Días de Entrega (promedio) | 12.4 días |
| ✅ Entregas a Tiempo | 93.4% |

### 🔍 Insights Descubiertos

1. **São Paulo (SP) concentra ~40% del revenue total** — R$ 5.2M de R$ 13.6M, seguido de Rio de Janeiro con R$ 1.8M.
2. **Solo 6.6% de entregas llegan tarde** — performance logística sólida, pero los estados del norte tienen tiempos significativamente más altos.
3. **Correlación negativa entre tiempo de entrega y satisfacción** — cada día adicional de entrega reduce el review score. Los estados con <10 días de entrega tienen review promedio >4.2.
4. **Tarjetas de crédito dominan 74% de las transacciones** — seguido de boleto bancario (19%).
5. **Lunes es el día con más compras, sábado el más bajo** — oportunidad de campañas de fin de semana.

---

## 🏗️ Arquitectura

```mermaid
flowchart LR
    A[📦 Kaggle\nOlist Dataset] -->|Python + Pandas| B[🗃️ Staging\nDuckDB]
    B -->|SQL DDL| C[⭐ Star Schema\nDims + Fact]
    B -->|dbt Core| D[📊 Marts\nListas para BI]
    C --> E[📈 Power BI\nDashboard]
    D --> E
    D --> F[📓 Python\nAnálisis EDA]
```

| Capa | Herramienta | Propósito |
|------|-------------|-----------|
| Ingesta (ETL) | Python + Pandas | Validación y carga a staging |
| DWH local | DuckDB | Motor SQL analítico sin servidor |
| Star Schema | SQL DDL | 4 dimensiones + 1 fact table |
| Transformaciones | dbt Core | 14 modelos ELT + 23 tests automatizados |
| Análisis | Python + Matplotlib | EDA con 8 visualizaciones de negocio |
| Visualización | Power BI Desktop | Dashboard final (guía incluida) |

---

## 🛠️ Nota sobre dbt y Python 3.14
Este proyecto incluye una capa de transformaciones con **dbt Core** en `03_transform/`.
Debido a que este entorno utiliza **Python 3.14 (experimental)**, algunas dependencias de dbt (`mashumaro`) pueden presentar incompatibilidades. Por esta razón, se ha incluido la carpeta `02_warehouse/` con scripts SQL puros que construyen el mismo Star Schema de forma nativa en DuckDB, garantizando que el proyecto sea siempre ejecutable mediante `run_all.py` o los scripts individuales de Python.

---

## ⭐ Star Schema (Modelo Dimensional)

```mermaid
erDiagram
    dim_date ||--o{ fact_orders : "order_date_key"
    dim_customers ||--o{ fact_orders : "customer_key"
    dim_products ||--o{ fact_orders : "product_key"
    dim_sellers ||--o{ fact_orders : "seller_key"

    dim_date {
        int date_key PK
        date full_date
        int year
        int quarter
        int month
        varchar month_name
        boolean is_weekend
    }

    dim_customers {
        int customer_key PK
        varchar customer_unique_id
        varchar customer_city
        varchar customer_state
    }

    dim_products {
        int product_key PK
        varchar product_id
        varchar product_category_pt
        varchar product_category_en
        double avg_price
        varchar price_band
    }

    dim_sellers {
        int seller_key PK
        varchar seller_id
        varchar seller_city
        varchar seller_state
    }

    fact_orders {
        varchar order_id PK
        int order_item_id PK
        int customer_key FK
        int product_key FK
        int seller_key FK
        int order_date_key FK
        double price
        double freight_value
        int review_score
        int days_to_deliver
        int delivery_delay_days
    }
```

> **Granularidad:** Cada fila en `fact_orders` representa un ítem dentro de una orden. Una orden con 3 productos genera 3 filas.

---

## 📊 Visualizaciones del EDA

### Revenue por Estado (Top 10)
![Revenue por Estado](04_analysis/figures/01_revenue_by_state.png)

### Evolución Mensual de Revenue
![Revenue Mensual](04_analysis/figures/02_revenue_over_time.png)

### Top 15 Categorías por Revenue
![Top Categorías](04_analysis/figures/03_top_categories.png)

### Relación Entrega vs Satisfacción
![Delivery vs Satisfaction](04_analysis/figures/04_delivery_vs_satisfaction.png)

---

## 📂 Estructura del Proyecto

```
retail-analytics-dwh/
├── 01_ingestion/                  ← Pipeline ETL
│   ├── download_dataset.py        ← Descarga desde Kaggle API (soporta MOCK_DATA)
│   ├── validate_raw.py            ← Análisis de calidad de CSVs
│   ├── load_to_staging.py         ← Carga a DuckDB con limpieza
│   └── verify_staging.py          ← Verificación de integridad
├── 02_warehouse/                  ← Star Schema (SQL DDL)
│   ├── create_schema.sql          ← DDL de dimensiones y fact table
│   ├── dim_date.sql               ← Población de dim_date
│   ├── dim_customers.sql          ← Población de dim_customers
│   ├── dim_products.sql           ← Población de dim_products
│   ├── dim_sellers.sql            ← Población de dim_sellers
│   ├── fact_orders.sql            ← Población de fact_orders
│   └── build_warehouse.py         ← Orquestador: ejecuta todo en orden
├── 03_transform/                  ← Proyecto dbt Core
│   ├── models/
│   │   ├── staging/               ← 9 modelos + sources (views)
│   │   ├── intermediate/          ← 2 modelos de joins enriquecidos (views)
│   │   └── marts/                 ← 3 marts finales para BI (tables)
│   ├── dbt_project.yml
│   └── profiles.yml               ← Conexión a DuckDB
├── 04_analysis/                   ← Análisis Exploratorio
│   ├── eda_retail_analytics.py    ← Script EDA con 8 visualizaciones (robusto a datos pequeños)
│   └── figures/                   ← Gráficos generados (PNG)
├── 05_dashboards/                 ← Power BI
│   └── powerbi_connection_guide.md
├── data/raw/                      ← CSVs (no versionados)
├── run_all.py                     ← Orquestador principal del proyecto
├── requirements.txt
└── README.md
```

---

## 👤 Autor

**Jose Betancur** — Economista cuantitativo e Ingeniero de Datos.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Conectar-blue)](https://www.linkedin.com/in/jose-sebastian-betancur-devia/)
[![GitHub](https://img.shields.io/badge/GitHub-Portfolio-black)](https://github.com/jsebastianbetancur-web)
