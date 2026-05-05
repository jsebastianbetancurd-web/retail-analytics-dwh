"""
run_all.py — Ejecución completa del pipeline Retail Analytics DWH
==================================================================
Este script orquestra todas las etapas del proyecto de forma secuencial:
  1. Ingesta (Modo Mock por defecto)
  2. Carga a Staging (DuckDB)
  3. Construcción del Star Schema (SQL Manual)
  4. Análisis Exploratorio (EDA + Visualizaciones)

Uso:
  python run_all.py
"""

import subprocess
import sys
import os
from pathlib import Path

def run_step(name, script_path, env=None):
    print(f"\n{'='*70}")
    print(f"  ETAPA: {name}")
    print(f"{'='*70}")
    
    cmd = [sys.executable, script_path]
    
    current_env = os.environ.copy()
    if env:
        current_env.update(env)
        
    result = subprocess.run(cmd, env=current_env)
    
    if result.returncode != 0:
        print(f"\n❌ ERROR en la etapa: {name}")
        sys.exit(1)
    
    print(f"✅ Etapa {name} completada con éxito.")

if __name__ == "__main__":
    print("🚀 Iniciando Pipeline Retail Analytics DWH...")
    
    # 1. Ingesta (forzamos MOCK_DATA para asegurar que funcione siempre)
    run_step("Descarga/Generación de Datos", "01_ingestion/download_dataset.py", {"MOCK_DATA": "true"})
    
    # 2. Carga a Staging
    run_step("Carga a Staging", "01_ingestion/load_to_staging.py")
    
    # 3. Warehouse (Star Schema)
    run_step("Construcción del DWH", "02_warehouse/build_warehouse.py")
    
    # 4. Análisis (EDA)
    run_step("Análisis de Negocio", "04_analysis/eda_retail_analytics.py", {"MOCK_DATA": "true"})
    
    print(f"\n{'='*70}")
    print("✨ PIPELINE COMPLETADO EXITOSAMENTE")
    print("   Resultados:")
    print("   - Base de datos: data/olist_dwh.duckdb")
    print("   - Visualizaciones: 04_analysis/figures/")
    print(f"{'='*70}")
