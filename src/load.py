import os
import pandas as pd
import logging

def save_to_parquet(df: pd.DataFrame, file_name: str, out_dir: str = "data/out_parquet"):
    """
    Capa de Carga (Load).
    Exporta un DataFrame nativo a formato Apache Parquet, el formato idóneo para Business Intelligence analítico.
    Reduce el peso en disco más del 80% frente a CSV y conserva los metadatos y datatypes de cada columna.
    """
    os.makedirs(out_dir, exist_ok=True)
    
    # Validamos que los directorios existan
    path = os.path.join(out_dir, f"{file_name}.parquet")
    
    try:
        # engine='pyarrow' fuerza la ultra-compresión subyacente de C++.
        df.to_parquet(path, engine="pyarrow", index=False)
        logging.info(f"✔ Exportado correctamente (Parquet): {path}")
    except Exception as e:
        logging.error(f"Fallo al codificar archivo Parquet {file_name}: {e}")
