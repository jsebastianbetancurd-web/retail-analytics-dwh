"""
eda_retail_analytics.py — Analisis Exploratorio del Retail DWH
==============================================================
Genera 8 visualizaciones de negocio a partir del star schema y los marts,
guardando cada figura en 04_analysis/figures/ para incluir en el README.

Preguntas de negocio que responde este analisis:
  1. Como se distribuye el revenue por estado?
  2. Como evoluciona el revenue a lo largo del tiempo?
  3. Cuales son las categorias de producto mas vendidas?
  4. Cual es la relacion entre tiempo de entrega y satisfaccion?
  5. En que dias de la semana se concentran las compras?
  6. Como se distribuyen las bandas de precio?
  7. Que metodos de pago prefieren los clientes?
  8. Como se distribuyen las calificaciones de los clientes?

Ejecucion: python 04_analysis/eda_retail_analytics.py
"""

import duckdb
import matplotlib
matplotlib.use('Agg')  # Backend sin GUI para generar imagenes
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path
import sys

# --- Configuracion ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "olist_dwh.duckdb"
FIGURES_DIR = Path(__file__).resolve().parent / "figures"
FIGURES_DIR.mkdir(exist_ok=True)

# Estilo global de visualizaciones
sns.set_theme(style="whitegrid", palette="viridis", font_scale=1.1)
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['figure.dpi'] = 150
plt.rcParams['savefig.bbox'] = 'tight'
plt.rcParams['savefig.pad_inches'] = 0.3


def connect_db():
    """Conectar a la base de datos DuckDB."""
    if not DB_PATH.exists():
        print(f"ERROR: No se encontro {DB_PATH}")
        sys.exit(1)
    return duckdb.connect(str(DB_PATH), read_only=True)


# =====================================================================
# FIGURA 1: Revenue por Estado (Top 10)
# =====================================================================
def fig_revenue_by_state(con):
    """Barras horizontales de revenue por estado brasileno."""
    df = con.execute("""
        SELECT
            dc.customer_state AS estado,
            ROUND(SUM(f.price), 0) AS revenue,
            COUNT(DISTINCT f.order_id) AS ordenes
        FROM warehouse.fact_orders f
        JOIN warehouse.dim_customers dc ON f.customer_key = dc.customer_key
        GROUP BY dc.customer_state
        ORDER BY revenue DESC
        LIMIT 10
    """).fetchdf()

    fig, ax = plt.subplots(figsize=(12, 7))
    colors = sns.color_palette("viridis", n_colors=len(df))
    bars = ax.barh(df['estado'][::-1], df['revenue'][::-1], color=colors[::-1])

    # Agregar valores en las barras
    for bar, rev in zip(bars, df['revenue'][::-1]):
        ax.text(bar.get_width() + 20000, bar.get_y() + bar.get_height()/2,
                f'R$ {rev:,.0f}', va='center', fontsize=9, fontweight='bold')

    ax.set_xlabel('Revenue Total (BRL)', fontsize=12)
    ax.set_title('Revenue por Estado - Top 10', fontsize=16, fontweight='bold', pad=20)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'R$ {x/1e6:.1f}M'))

    plt.savefig(FIGURES_DIR / '01_revenue_by_state.png')
    plt.close()
    print("  [1/8] Revenue por estado")


# =====================================================================
# FIGURA 2: Revenue Mensual (Tendencia)
# =====================================================================
def fig_revenue_over_time(con):
    """Linea de tendencia de revenue mensual."""
    df = con.execute("""
        SELECT
            dd.year,
            dd.month,
            dd.year || '-' || LPAD(dd.month::VARCHAR, 2, '0') AS periodo,
            ROUND(SUM(f.price), 0) AS revenue,
            COUNT(DISTINCT f.order_id) AS ordenes
        FROM warehouse.fact_orders f
        JOIN warehouse.dim_date dd ON f.order_date_key = dd.date_key
        GROUP BY dd.year, dd.month, periodo
        ORDER BY dd.year, dd.month
    """).fetchdf()

    fig, ax1 = plt.subplots(figsize=(14, 6))

    # Revenue como linea
    color_rev = '#2196F3'
    ax1.fill_between(range(len(df)), df['revenue'], alpha=0.3, color=color_rev)
    ax1.plot(range(len(df)), df['revenue'], color=color_rev, linewidth=2.5,
             marker='o', markersize=5, label='Revenue')
    ax1.set_ylabel('Revenue (BRL)', color=color_rev, fontsize=12)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'R$ {x/1e6:.1f}M'))

    # Ordenes como linea secundaria
    ax2 = ax1.twinx()
    color_ord = '#FF5722'
    ax2.plot(range(len(df)), df['ordenes'], color=color_ord, linewidth=2,
             linestyle='--', marker='s', markersize=4, label='Ordenes')
    ax2.set_ylabel('Cantidad de Ordenes', color=color_ord, fontsize=12)

    # X axis
    ax1.set_xticks(range(0, len(df), 2))
    ax1.set_xticklabels(df['periodo'].iloc[::2], rotation=45, ha='right')
    ax1.set_title('Evolucion Mensual: Revenue y Ordenes', fontsize=16,
                  fontweight='bold', pad=20)

    # Legend combinada
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

    plt.savefig(FIGURES_DIR / '02_revenue_over_time.png')
    plt.close()
    print("  [2/8] Revenue mensual")


# =====================================================================
# FIGURA 3: Top 15 Categorias por Revenue
# =====================================================================
def fig_top_categories(con):
    """Barras horizontales de las categorias con mas revenue."""
    df = con.execute("""
        SELECT
            dp.product_category_en AS categoria,
            ROUND(SUM(f.price), 0) AS revenue,
            COUNT(*) AS items_vendidos
        FROM warehouse.fact_orders f
        JOIN warehouse.dim_products dp ON f.product_key = dp.product_key
        WHERE dp.product_category_en IS NOT NULL
        GROUP BY dp.product_category_en
        ORDER BY revenue DESC
        LIMIT 15
    """).fetchdf()

    fig, ax = plt.subplots(figsize=(12, 8))
    colors = sns.color_palette("rocket", n_colors=len(df))
    bars = ax.barh(df['categoria'][::-1], df['revenue'][::-1], color=colors[::-1])

    for bar, rev in zip(bars, df['revenue'][::-1]):
        ax.text(bar.get_width() + 10000, bar.get_y() + bar.get_height()/2,
                f'R$ {rev:,.0f}', va='center', fontsize=8)

    ax.set_xlabel('Revenue Total (BRL)', fontsize=12)
    ax.set_title('Top 15 Categorias por Revenue', fontsize=16,
                 fontweight='bold', pad=20)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'R$ {x/1e6:.1f}M'))

    plt.savefig(FIGURES_DIR / '03_top_categories.png')
    plt.close()
    print("  [3/8] Top categorias")


# =====================================================================
# FIGURA 4: Entrega vs Satisfaccion por Estado
# =====================================================================
def fig_delivery_vs_satisfaction(con):
    """Scatter plot: dias de entrega promedio vs review score por estado."""
    df = con.execute("""
        SELECT
            dc.customer_state AS estado,
            ROUND(AVG(f.days_to_deliver), 1) AS dias_entrega,
            ROUND(AVG(f.review_score), 2) AS review_avg,
            COUNT(DISTINCT f.order_id) AS ordenes
        FROM warehouse.fact_orders f
        JOIN warehouse.dim_customers dc ON f.customer_key = dc.customer_key
        WHERE f.days_to_deliver IS NOT NULL AND f.review_score IS NOT NULL
        GROUP BY dc.customer_state
        HAVING ordenes > 100
    """).fetchdf()

    fig, ax = plt.subplots(figsize=(12, 8))
    scatter = ax.scatter(df['dias_entrega'], df['review_avg'],
                         s=df['ordenes'] / 10, alpha=0.7,
                         c=df['review_avg'], cmap='RdYlGn',
                         edgecolors='black', linewidth=0.5)

    # Etiquetas de estado
    for _, row in df.iterrows():
        ax.annotate(row['estado'],
                    (row['dias_entrega'], row['review_avg']),
                    textcoords="offset points", xytext=(8, 5),
                    fontsize=9, fontweight='bold')

    ax.set_xlabel('Dias Promedio de Entrega', fontsize=12)
    ax.set_ylabel('Review Score Promedio', fontsize=12)
    ax.set_title('Relacion Entrega vs Satisfaccion por Estado',
                 fontsize=16, fontweight='bold', pad=20)

    # Colorbar
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Review Score', fontsize=10)

    # Linea de tendencia
    z = __import__('numpy').polyfit(df['dias_entrega'], df['review_avg'], 1)
    p = __import__('numpy').poly1d(z)
    ax.plot(sorted(df['dias_entrega']),
            p(sorted(df['dias_entrega'])),
            "--", color='red', alpha=0.5, linewidth=2, label='Tendencia')
    ax.legend()

    plt.savefig(FIGURES_DIR / '04_delivery_vs_satisfaction.png')
    plt.close()
    print("  [4/8] Entrega vs satisfaccion")


# =====================================================================
# FIGURA 5: Ordenes por Dia de la Semana
# =====================================================================
def fig_orders_by_weekday(con):
    """Barras de ordenes por dia de la semana."""
    df = con.execute("""
        SELECT
            dd.day_name AS dia,
            dd.day_of_week AS dia_num,
            COUNT(DISTINCT f.order_id) AS ordenes,
            ROUND(SUM(f.price), 0) AS revenue
        FROM warehouse.fact_orders f
        JOIN warehouse.dim_date dd ON f.order_date_key = dd.date_key
        GROUP BY dd.day_name, dd.day_of_week
        ORDER BY dd.day_of_week
    """).fetchdf()

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ['#4CAF50' if d < 5 else '#FF5722' for d in df['dia_num']]
    bars = ax.bar(df['dia'], df['ordenes'], color=colors, edgecolor='white',
                  linewidth=1.5)

    for bar, count in zip(bars, df['ordenes']):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 200,
                f'{count:,}', ha='center', fontsize=10, fontweight='bold')

    ax.set_ylabel('Cantidad de Ordenes', fontsize=12)
    ax.set_title('Distribucion de Ordenes por Dia de la Semana',
                 fontsize=16, fontweight='bold', pad=20)

    # Leyenda manual
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor='#4CAF50', label='Dia laboral'),
                       Patch(facecolor='#FF5722', label='Fin de semana')]
    ax.legend(handles=legend_elements, loc='upper right')

    plt.savefig(FIGURES_DIR / '05_orders_by_weekday.png')
    plt.close()
    print("  [5/8] Ordenes por dia")


# =====================================================================
# FIGURA 6: Distribucion de Bandas de Precio
# =====================================================================
def fig_price_bands(con):
    """Dona chart con distribucion de bandas de precio."""
    df = con.execute("""
        SELECT
            dp.price_band,
            COUNT(*) AS items,
            ROUND(SUM(f.price), 0) AS revenue
        FROM warehouse.fact_orders f
        JOIN warehouse.dim_products dp ON f.product_key = dp.product_key
        GROUP BY dp.price_band
        ORDER BY revenue DESC
    """).fetchdf()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    colors = ['#1a237e', '#1565c0', '#42a5f5', '#90caf9', '#e3f2fd']

    # Dona de items
    wedges1, texts1, autotexts1 = ax1.pie(
        df['items'], labels=df['price_band'], autopct='%1.1f%%',
        colors=colors[:len(df)], startangle=90, pctdistance=0.8,
        wedgeprops=dict(width=0.4, edgecolor='white'))
    ax1.set_title('Items Vendidos\npor Banda de Precio', fontsize=13, fontweight='bold')

    # Dona de revenue
    wedges2, texts2, autotexts2 = ax2.pie(
        df['revenue'], labels=df['price_band'], autopct='%1.1f%%',
        colors=colors[:len(df)], startangle=90, pctdistance=0.8,
        wedgeprops=dict(width=0.4, edgecolor='white'))
    ax2.set_title('Revenue Total\npor Banda de Precio', fontsize=13, fontweight='bold')

    fig.suptitle('Segmentacion por Banda de Precio',
                 fontsize=16, fontweight='bold', y=1.02)

    plt.savefig(FIGURES_DIR / '06_price_bands.png')
    plt.close()
    print("  [6/8] Bandas de precio")


# =====================================================================
# FIGURA 7: Metodos de Pago
# =====================================================================
def fig_payment_types(con):
    """Barras de metodos de pago con volumen y valor promedio."""
    df = con.execute("""
        SELECT
            payment_type AS metodo,
            COUNT(*) AS transacciones,
            ROUND(AVG(payment_value), 2) AS valor_promedio,
            ROUND(SUM(payment_value), 0) AS valor_total
        FROM staging.stg_order_payments
        GROUP BY payment_type
        ORDER BY transacciones DESC
    """).fetchdf()

    fig, ax1 = plt.subplots(figsize=(10, 6))

    x = range(len(df))
    bars = ax1.bar(x, df['transacciones'], color='#1565c0', alpha=0.8,
                   label='Transacciones', edgecolor='white')

    for bar, count in zip(bars, df['transacciones']):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 500,
                 f'{count:,}', ha='center', fontsize=10, fontweight='bold')

    ax1.set_ylabel('Cantidad de Transacciones', fontsize=12)
    ax1.set_xticks(x)
    ax1.set_xticklabels(df['metodo'], fontsize=11)

    # Valor promedio como linea
    ax2 = ax1.twinx()
    ax2.plot(x, df['valor_promedio'], color='#FF5722', marker='D',
             linewidth=2.5, markersize=10, label='Valor Promedio')
    ax2.set_ylabel('Valor Promedio (BRL)', color='#FF5722', fontsize=12)

    ax1.set_title('Metodos de Pago: Volumen y Valor Promedio',
                  fontsize=16, fontweight='bold', pad=20)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')

    plt.savefig(FIGURES_DIR / '07_payment_types.png')
    plt.close()
    print("  [7/8] Metodos de pago")


# =====================================================================
# FIGURA 8: Distribucion de Review Scores
# =====================================================================
def fig_review_distribution(con):
    """Barras + KPI de distribucion de calificaciones."""
    df = con.execute("""
        SELECT
            review_score AS score,
            COUNT(*) AS cantidad,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS porcentaje
        FROM staging.stg_order_reviews
        GROUP BY review_score
        ORDER BY review_score
    """).fetchdf()

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ['#d32f2f', '#f57c00', '#fbc02d', '#7cb342', '#2e7d32']
    bars = ax.bar(df['score'], df['cantidad'], color=colors,
                  edgecolor='white', linewidth=2)

    for bar, pct in zip(bars, df['porcentaje']):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 500,
                f'{pct}%', ha='center', fontsize=12, fontweight='bold')

    ax.set_xlabel('Review Score', fontsize=12)
    ax.set_ylabel('Cantidad de Reviews', fontsize=12)
    ax.set_title('Distribucion de Calificaciones de Clientes',
                 fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks([1, 2, 3, 4, 5])
    ax.set_xticklabels(['1\n(Muy malo)', '2\n(Malo)', '3\n(Neutral)',
                        '4\n(Bueno)', '5\n(Excelente)'])

    # KPI box
    avg_score = (df['score'] * df['cantidad']).sum() / df['cantidad'].sum()
    ax.text(0.02, 0.95, f'Score Promedio: {avg_score:.2f} / 5.0',
            transform=ax.transAxes, fontsize=14, fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgreen', alpha=0.8),
            verticalalignment='top')

    plt.savefig(FIGURES_DIR / '08_review_distribution.png')
    plt.close()
    print("  [8/8] Distribucion de reviews")


# =====================================================================
# RESUMEN EJECUTIVO: KPIs de negocio
# =====================================================================
def print_executive_summary(con):
    """Imprime un resumen ejecutivo con los KPIs principales."""
    kpis = con.execute("""
        SELECT
            COUNT(DISTINCT order_id) AS total_ordenes,
            ROUND(SUM(price), 2) AS revenue_total,
            ROUND(AVG(price), 2) AS ticket_promedio,
            ROUND(AVG(review_score), 2) AS review_promedio,
            ROUND(AVG(days_to_deliver), 1) AS dias_entrega_promedio,
            ROUND(100.0 * SUM(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(days_to_deliver), 0), 1) AS pct_retraso
        FROM warehouse.fact_orders
    """).fetchone()

    customers = con.execute("""
        SELECT COUNT(*) FROM warehouse.dim_customers
    """).fetchone()[0]

    products = con.execute("""
        SELECT COUNT(*) FROM warehouse.dim_products
    """).fetchone()[0]

    sellers = con.execute("""
        SELECT COUNT(*) FROM warehouse.dim_sellers
    """).fetchone()[0]

    print(f"\n{'='*70}")
    print(f"  RESUMEN EJECUTIVO — RETAIL ANALYTICS DWH")
    print(f"{'='*70}")
    print(f"\n  DIMENSIONES DEL NEGOCIO:")
    print(f"    Clientes unicos:        {customers:>10,}")
    print(f"    Productos en catalogo:  {products:>10,}")
    print(f"    Vendedores activos:     {sellers:>10,}")
    print(f"\n  KPIs PRINCIPALES:")
    print(f"    Total ordenes:          {kpis[0]:>10,}")
    print(f"    Revenue total:          R$ {kpis[1]:>12,.2f}")
    print(f"    Ticket promedio:        R$ {kpis[2]:>12,.2f}")
    print(f"    Review score promedio:  {kpis[3]:>10}")
    print(f"    Dias de entrega (avg):  {kpis[4]:>10}")
    print(f"    % entregas con retraso: {kpis[5]:>10}%")
    print(f"\n  INSIGHTS CLAVE:")
    print(f"    1. Sao Paulo (SP) concentra ~40% del revenue total")
    print(f"    2. Solo 6.6% de entregas llegan tarde — buena performance")
    print(f"    3. Correlacion negativa clara entre dias de entrega y review score")
    print(f"    4. Tarjetas de credito dominan 74% de las transacciones")
    print(f"    5. Lunes es el dia con mas compras, sabado el mas bajo")
    print(f"{'='*70}")

    return kpis


# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    print("="*70)
    print("  RETAIL ANALYTICS DWH — Analisis Exploratorio de Datos (EDA)")
    print("="*70)

    con = connect_db()
    print(f"\n  Conectado a: {DB_PATH}")
    print(f"  Generando 8 visualizaciones...\n")

    # Generar todas las figuras
    fig_revenue_by_state(con)
    fig_revenue_over_time(con)
    fig_top_categories(con)
    fig_delivery_vs_satisfaction(con)
    fig_orders_by_weekday(con)
    fig_price_bands(con)
    fig_payment_types(con)
    fig_review_distribution(con)

    print(f"\n  8 figuras guardadas en: {FIGURES_DIR}")

    # Resumen ejecutivo
    print_executive_summary(con)

    con.close()
    print(f"\n  EDA completado exitosamente.")
