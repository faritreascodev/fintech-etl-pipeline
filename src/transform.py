import pandas as pd
import numpy as np
import logging

def build_cartera_y_posiciones(trans: pd.DataFrame, precios: pd.DataFrame) -> tuple:
    """Calcula y proyecta las transacciones convirtiéndolas en capitalizaciones diarias."""
    logging.info("Iniciando Transformación: Reconstrucción de la Línea de Tiempo de Posiciones y Cartera")
    
    trans = trans.sort_values(["cuenta_id", "ticker", "fecha"])
    trans["cantidad_acum"] = trans.groupby(["cuenta_id", "ticker"])["cantidad"].cumsum()
    
    px = precios.drop_duplicates(subset=["fecha", "ticker"], keep="last") \
                .pivot(index="fecha", columns="ticker", values="cierre") \
                .sort_index().ffill()
    
    valores = []
    for cta, gcta in trans.groupby("cuenta_id"):
        for tkr, g in gcta.groupby("ticker"):
            q_day_last = g.set_index("fecha")["cantidad_acum"].groupby(level=0).last()
            q = q_day_last.reindex(px.index).ffill().fillna(0.0)
            
            p = px[tkr] if tkr in px.columns else pd.Series(0.0, index=px.index)
            p = p.ffill().fillna(0.0)
            
            valores.append(pd.DataFrame({
                "fecha": px.index,
                "cuenta_id": cta,
                "ticker": tkr,
                "valor_posicion": q.values * p.values
            }))

    posiciones = (pd.concat(valores, ignore_index=True)
                  if valores else
                  pd.DataFrame(columns=["fecha","cuenta_id","ticker","valor_posicion"]))
                  
    cartera = (posiciones.groupby(["fecha","cuenta_id"], as_index=False)["valor_posicion"]
               .sum()
               .rename(columns={"valor_posicion":"valor_cartera"})
               .sort_values(["cuenta_id","fecha"]))

    prev = cartera.groupby("cuenta_id")["valor_cartera"].shift()
    cartera["retorno"] = (cartera["valor_cartera"] / prev - 1).where(prev > 0, 0.0)
    cartera["retorno"] = cartera["retorno"].replace([np.inf, -np.inf], 0.0).fillna(0.0)

    cartera["acumulado"] = 1.0
    bloques = []
    for acc, g in cartera.groupby("cuenta_id"):
        g = g.copy()
        activos = g["valor_cartera"] > 0
        if activos.any():
            i0 = g.index[activos][0]
            g.loc[i0:, "acumulado"] = (1 + g.loc[i0:, "retorno"]).cumprod()
        bloques.append(g)

    cartera = pd.concat(bloques, ignore_index=True).sort_values(["cuenta_id","fecha"])
    
    logging.info("Transformación exitosa.")
    return cartera, posiciones

def calculate_exposures(posiciones: pd.DataFrame, trans: pd.DataFrame) -> tuple:
    """Calcula pesos de riesgo y exposición por sector/ticker."""
    logging.info("Calculando factores de Riesgo y Exposición de la Cartera.")
    exp_ticker = posiciones.groupby(["cuenta_id","ticker"], as_index=False)["valor_posicion"].mean()
    exp_ticker.rename(columns={"valor_posicion":"valor_promedio"}, inplace=True)

    if "sector" in trans.columns:
        sector_map = trans[["ticker","sector"]].drop_duplicates().set_index("ticker")["sector"]
        exp_ticker["sector"] = exp_ticker["ticker"].map(sector_map)
    else:
        sector_map = None

    if sector_map is not None:
        exp_sector = exp_ticker.groupby(["cuenta_id","sector"], as_index=False)["valor_promedio"].sum()
    else:
        exp_sector = exp_ticker.rename(columns={"ticker":"sector"})[["cuenta_id","sector","valor_promedio"]]

    def normaliza(df):
        outs = []
        for cta, g in df.groupby("cuenta_id"):
            total = g["valor_promedio"].sum()
            g = g.copy()
            g["peso"] = 0.0 if total == 0 else g["valor_promedio"] / total
            outs.append(g)
        return pd.concat(outs, ignore_index=True)

    return normaliza(exp_ticker), normaliza(exp_sector)
