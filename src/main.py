import logging
from extract import extract_from_excel
from transform import build_cartera_y_posiciones, calculate_exposures
from load import save_to_parquet
from visualize import generate_all_figures

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def run_pipeline():
    logging.info("INICIANDO ORQUESTACIÓN ETL FINTECH")
    
    # 1. EXTRACT
    raw_data = extract_from_excel("data/nuevo_dataset.xlsx")
    trans = raw_data["transacciones"]
    precios = raw_data["precios"]
    
    # 2. TRANSFORM
    cartera, posiciones = build_cartera_y_posiciones(trans, precios)
    exp_ticker, exp_sector = calculate_exposures(posiciones, trans)
    
    # 3. LOAD (Carga final a Parquet)
    save_to_parquet(cartera, "cartera_diaria")
    save_to_parquet(posiciones, "posiciones_diarias")
    save_to_parquet(exp_ticker, "exposicion_ticker")
    save_to_parquet(exp_sector, "exposicion_sector")
    
    # 4. VISUALIZE (Figuras para paper académico)
    generate_all_figures()
    
    logging.info("PIPELINE EJECUTADO EXITOSAMENTE")

if __name__ == "__main__":
    run_pipeline()
