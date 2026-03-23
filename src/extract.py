import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_from_excel(file_path: str) -> dict:
    """
    Módulo de Ingesta (Extract)
    Extrae las hojas requeridas verificando existencias.
    """
    logging.info(f"Extrayendo datos desde origen Excel: {file_path}")
    try:
        trans = pd.read_excel(file_path, sheet_name='transacciones')
        precios = pd.read_excel(file_path, sheet_name='precios')
        indices = pd.read_excel(file_path, sheet_name='indices')
        
        # Limpieza estándar formativa de nombres de columnas
        for df in [trans, precios, indices]:
            df.columns = [str(c).strip().lower() for c in df.columns]
            
        logging.info(f"Ingesta completada. Transacciones: {trans.shape[0]} filas | Precios: {precios.shape[0]} filas.")
        return {"transacciones": trans, "precios": precios, "indices": indices}
    except Exception as e:
        logging.error(f"Error crítico de lectura en el dataset: {e}")
        raise
