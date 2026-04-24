# 🎯 Guía de Entrevista: Retail Analytics Data Warehouse

Este documento está diseñado para ayudarte a defender tu proyecto en entrevistas técnicas y de negocio. Contiene el "elevator pitch", la explicación detallada de la arquitectura, el valor de negocio de los hallazgos y una batería de preguntas frecuentes.

---

## 1. 📢 Elevator Pitch (Tu resumen en 1 minuto)

> *"Para mi portfolio, construí un Data Warehouse de retail end-to-end simulando el entorno de una empresa de comercio electrónico grande como KOAJ. Utilicé datos reales de Olist (100k+ órdenes). Diseñé una arquitectura ELT donde uso Python para la ingesta y validación de datos crudos hacia DuckDB, y luego dbt Core para orquestar todas las transformaciones SQL, pasando por capas staging, intermediate y marts. Implementé 23 tests automatizados de calidad de datos y modelé la información en un Star Schema. Finalmente, realicé un análisis exploratorio que descubrió insights clave sobre la relación entre los tiempos logísticos y la satisfacción del cliente, preparando los datos para ser consumidos en Power BI."*

---

## 2. ⚙️ ¿Cómo funciona el proyecto? (Arquitectura Técnica Detallada)

El proyecto sigue un paradigma **ELT (Extract, Load, Transform)** moderno, dividido en 4 fases principales:

### A. Extracción y Carga (Python + DuckDB)
1. **API de Kaggle**: Un script de Python (`download_dataset.py`) interactúa con la API de Kaggle para obtener 9 archivos CSV crudos.
2. **Validación Cruda**: `validate_raw.py` revisa la completitud de los datos (nulos, duplicados) antes de procesarlos.
3. **Carga a Staging**: `load_to_staging.py` usa Pandas y DuckDB para cargar los CSV en el esquema `staging` de DuckDB. Aquí se aplican reglas de **idempotencia** (deduplicación por Primary Keys usando `ROW_NUMBER()`) y estandarización de nombres de columnas.

### B. Modelado Dimensional (Star Schema)
En lugar de consultar tablas transaccionales altamente normalizadas, se construyó un esquema estrella (`02_warehouse/`):
*   **1 Fact Table (`fact_orders`)**: Con granularidad a nivel de *ítem de la orden*. Contiene las llaves foráneas y las métricas (precio, flete, días de entrega).
*   **4 Dimensiones (`dim_date`, `dim_customers`, `dim_products`, `dim_sellers`)**: Contienen atributos descriptivos (categorías, ubicaciones, fechas).
*   *Decisión de diseño:* Se usaron **Surrogate Keys** (enteros autoincrementales) para los JOINs en lugar de las Natural Keys (strings alfanuméricos largos) para mejorar drásticamente el rendimiento de las consultas.

### C. Transformaciones y Calidad (dbt Core)
El motor de transformación es dbt Core (`03_transform/`). Se divide en 3 capas (Medallion Architecture adaptada):
1.  **Staging (`stg_`)**: Vistas materializadas (views) que sirven como una capa de abstracción sobre los datos crudos. Se aplican conversiones de tipo explícitas y renombramiento de columnas.
2.  **Intermediate (`int_`)**: Vistas donde se realizan JOINs complejos (ej. unir órdenes, ítems e información de pago).
3.  **Marts (`mart_`)**: Tablas físicas (tables) agregadas por dominio de negocio (Ventas por región, Desempeño de productos, KPIs logísticos). Al ser materializadas como tablas, son extremadamente rápidas para Power BI.
*   **Calidad de Datos:** Se ejecutan 23 tests (nulos, unicidad, valores aceptados) en cada corrida (`dbt test`) garantizando la confianza en los datos.

### D. Consumo y Análisis (Python / EDA)
Un script final (`eda_retail_analytics.py`) se conecta a DuckDB y genera visualizaciones con Seaborn y Matplotlib, extrayendo KPIs como Ticket Promedio, Tasas de Retraso y Revenue Total.

---

## 3. 💡 Contexto de Negocio y Hallazgos (El "Por Qué")

Un Data Engineer/Analytics Engineer no solo mueve datos, sino que habilita la toma de decisiones. Este proyecto resuelve preguntas críticas del retail.

### El Problema Simulado
Un equipo comercial necesita entender dónde están perdiendo dinero, qué productos impulsan el crecimiento y cómo la logística está afectando la retención de clientes. Antes de este DWH, hacer estas consultas implicaba cruzar 9 archivos CSV gigantes manualmente.

### Hallazgos Clave y su Valor
1.  **"São Paulo (SP) concentra ~40% del revenue (R$ 5.2M)"**
    *   *Contexto de Negocio:* Esto justifica enfocar el presupuesto de pauta digital y abrir centros de distribución locales (micro-fulfillment) en SP para abaratar costos logísticos.
2.  **"Correlación negativa clara entre días de entrega y review score"**
    *   *Contexto de Negocio:* Se comprobó con datos que la logística tardía destruye la experiencia del cliente (estados con más de 15 días de entrega promedian menos de 3.5 estrellas). Esto da argumentos a Operaciones para renegociar contratos con transportadoras o cambiar proveedores en las regiones del norte.
3.  **"Lunes es el día con más compras, sábado el más bajo"**
    *   *Contexto de Negocio:* El equipo de Marketing puede lanzar campañas de "Flash Sales" los sábados para levantar el valle de ventas, o concentrar su presupuesto en remarketing los domingos por la noche para capturar la ola del lunes.
4.  **"Tarjetas de crédito dominan el 74% de las transacciones"**
    *   *Contexto de Negocio:* Indica que ofrecer pagos a cuotas sin interés en categorías premium (que según el EDA tienen buen revenue) podría incrementar aún más el ticket promedio.

---

## 4. 🗣️ Posibles Preguntas de Entrevista y Respuestas

### 💻 Preguntas Técnicas (Ingeniería & dbt)

**Q1: ¿Por qué elegiste DuckDB en lugar de PostgreSQL o BigQuery para este proyecto?**
> *"Elegí DuckDB porque es un motor OLAP en proceso (in-process). Es decir, está altamente optimizado para consultas analíticas pesadas (columnar) pero no requiere la infraestructura ni el mantenimiento de una base de datos tradicional como Postgres, ni genera costos en la nube como BigQuery. Para un proyecto de esta escala y para demostración, me permitía iterar localmente a máxima velocidad mientras aplicaba las mismas técnicas (SQL, dbt) que usaría en un entorno Cloud."*

**Q2: ¿Cuál es la diferencia entre la forma en que estructuraste la capa `staging` y la capa `marts` en dbt?**
> *"En la capa `staging` materialicé los modelos como `vistas` (views). El objetivo allí es hacer limpieza ligera (casteos, renombrar columnas) sin duplicar datos en disco innecesariamente. En cambio, los `marts` los materialicé como `tablas` (tables) porque son las estructuras finales que consumirá Power BI. Al ser tablas físicas, las consultas desde los dashboards son inmediatas y no tienen que recalcular toda la lógica de los JOINs subyacentes."*

**Q3: Mencionas que tu pipeline es "idempotente". ¿Qué significa eso y cómo lo lograste?**
> *"Idempotencia significa que si ejecuto mi pipeline una, dos o cien veces, el resultado final en la base de datos será exactamente el mismo, sin duplicar registros. Lo logré en la fase de extracción usando funciones ventana (`ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp)`) para seleccionar siempre la última versión de un registro y utilizando operaciones `CREATE OR REPLACE` en la carga inicial."*

**Q4: ¿Qué tipo de tests configuraste en dbt y por qué son importantes?**
> *"Configuré 23 tests, enfocándome en 3 pilares: Unicidad (`unique` en llaves primarias), No nulidad (`not_null` en FKs y métricas clave) y Valores aceptados (`accepted_values` para asegurar que campos como 'estado de la orden' solo contengan valores válidos). Son críticos porque en retail una orden huérfana o un precio nulo arruina el cálculo del Ticket Promedio o el Revenue. Si un test falla, dbt detiene el proceso antes de que el error llegue a los dashboards."*

### 📊 Preguntas de Modelado y Negocio

**Q5: ¿Por qué usaste un Star Schema? ¿No era más fácil hacer una sola tabla plana (One Big Table)?**
> *"Una OBT (One Big Table) es más fácil de consultar para un analista, pero un Star Schema me da escalabilidad. Si mañana el equipo quiere analizar inventarios, solo creo una nueva `fact_inventory` que se conecta a mis dimensiones existentes (`dim_products`, `dim_date`). Además, mantiene el modelo normalizado evitando redundancias masivas de texto (como repetir el nombre de la categoría 100k veces)."*

**Q6: ¿Cuál fue la granularidad elegida para tu Fact Table principal y por qué?**
> *"Elegí la granularidad de 'Order Item' (Ítem de la orden) en lugar de 'Orden'. Una orden puede tener una camisa y unos zapatos. Si modelo a nivel de orden, pierdo la capacidad de analizar qué categorías de productos se venden más o se devuelven más. A nivel de ítem, puedo hacer drill-down hasta el mínimo nivel de detalle o agregar hacia arriba fácilmente."*

**Q7: Si KOAJ (Permoda) te pidiera llevar este proyecto a Producción en AWS/GCP, ¿qué cambiarías de la arquitectura?**
> *"La lógica de transformación de dbt y el SQL se mantendrían intactos, ese es el valor de dbt. Cambiaría la infraestructura: usaría Amazon S3 o GCS para almacenar los CSVs crudos (Data Lake). Cambiaría DuckDB por un Data Warehouse en la nube como Snowflake o BigQuery. Y finalmente, orquestaría el pipeline, que hoy ejecuto con scripts de Python, usando una herramienta como Apache Airflow o dbt Cloud para programar ejecuciones diarias y manejar reintentos."*

**Q8: Imagina que el Ticket Promedio cae un 10% la próxima semana en el dashboard. ¿Cómo usarías este modelo para investigar la causa?**
> *"Gracias al modelo dimensional, haría un análisis de 'drill-down'. Primero cruzaría el Ticket Promedio por la dimensión `dim_date` para ver el día exacto de la caída. Luego cruzaría por `dim_products` (quizás una categoría de ticket alto dejó de venderse por falta de stock), o por `dim_customers` (¿hubo una caída en una región fuerte como SP?). El esquema permite aislar rápidamente la variable que causó el impacto."*
