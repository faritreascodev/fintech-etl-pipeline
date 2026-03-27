"""
visualize.py
Generación de figuras científicas a partir de los artefactos Parquet del ETL.
Produce 8 figuras en DPI=300 para publicación académica.
Autor: Farit Alexander Reasco Torres — PUCESE (2026)
"""

import os
import logging
import warnings
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

matplotlib.use("Agg")
warnings.filterwarnings("ignore", category=FutureWarning)

plt.rcParams.update({
    "figure.facecolor":  "#FFFFFF",
    "axes.facecolor":    "#F8F8F8",
    "axes.edgecolor":    "#CCCCCC",
    "axes.grid":         True,
    "grid.color":        "#E0E0E0",
    "grid.linewidth":    0.6,
    "font.family":       "DejaVu Sans",
    "font.size":         9,
    "axes.titlesize":    10,
    "axes.labelsize":    9,
    "legend.fontsize":   8,
    "xtick.labelsize":   8,
    "ytick.labelsize":   8,
})

FIG_DIR = "docs/data_analysis_in_Fintech"
DPI     = 300
PALETTE = sns.color_palette("tab10")


def _save(fig: plt.Figure, nombre: str) -> None:
    os.makedirs(FIG_DIR, exist_ok=True)
    ruta = os.path.join(FIG_DIR, nombre)
    fig.savefig(ruta, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    logging.info(f"Figura exportada: {ruta}")


def _load_parquet(nombre: str, out_dir: str = "data/out_parquet") -> pd.DataFrame:
    ruta = os.path.join(out_dir, f"{nombre}.parquet")
    if not os.path.exists(ruta):
        logging.warning(f"Artefacto no encontrado: {ruta}")
        return pd.DataFrame()
    df = pd.read_parquet(ruta, engine="pyarrow")
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    return df


def fig_portafolio_vs_benchmark(cartera: pd.DataFrame, indices: pd.DataFrame) -> None:
    """
    Rendimiento acumulado del portafolio vs. índice de referencia.
    Usa el campo 'acumulado' calculado en transform.py (idempotente, con retornos
    seguros) para evitar recalcular desde valor_cartera bruto. El benchmark se
    rebasa en la primera fecha activa del portafolio.
    """
    if cartera.empty:
        logging.error("fig1: cartera vacía — figura omitida.")
        return

    cartera = cartera.copy()
    cartera["fecha"] = pd.to_datetime(cartera["fecha"], errors="coerce")
    cartera["acumulado"] = pd.to_numeric(cartera["acumulado"], errors="coerce")

    port = (
        cartera.groupby("fecha")["acumulado"]
        .mean()
        .sort_index()
    )

    primera_fecha = port[port > 0].index.min()
    if pd.isna(primera_fecha):
        logging.error("fig1: portafolio sin valor positivo — figura omitida.")
        return

    fig, ax = plt.subplots(figsize=(7.5, 3.5))
    ax.plot(port.index, port.values, lw=1.6, color=PALETTE[0], label="Portafolio")

    if not indices.empty:
        idx = indices.copy()
        idx["fecha"] = pd.to_datetime(idx["fecha"], errors="coerce")
        precio_col = next(
            (c for c in ["indice", "cierre", "close", "precio", "value"]
             if c in idx.columns),
            None
        )
        if precio_col:
            idx[precio_col] = pd.to_numeric(idx[precio_col], errors="coerce")
            bench = (
                idx.set_index("fecha")[precio_col]
                .sort_index()
                .reindex(port.index)
                .ffill()
                .bfill()
            )
            base_b = bench.loc[primera_fecha] if primera_fecha in bench.index else bench.iloc[0]
            bench_norm = bench / base_b
            ax.plot(bench_norm.index, bench_norm.values, lw=1.2,
                    color=PALETTE[1], linestyle="--", label=f"Índice ({precio_col})")
        else:
            logging.warning("fig1: no se encontró columna de precios en la hoja indices.")

    ax.set_title("Rendimiento Acumulado: Portafolio vs. Índice de Referencia")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Rendimiento Acumulado (base = 1)")
    ax.legend()
    _save(fig, "fig_portafolio_vs_benchmark.png")


def fig_donut_sector(exp_sector: pd.DataFrame) -> None:
    """Composición sectorial del portafolio como peso promedio."""
    if exp_sector.empty:
        logging.error("fig2: exposición_sector vacía — figura omitida.")
        return

    df = exp_sector.copy()
    df["peso"] = pd.to_numeric(df["peso"], errors="coerce")
    agg = df.groupby("sector")["peso"].mean().sort_values(ascending=False)
    agg = agg[agg > 0]

    if agg.empty:
        logging.error("fig2: todos los pesos son cero — figura omitida.")
        return

    fig, ax = plt.subplots(figsize=(5, 5))
    wedges, texts, autotexts = ax.pie(
        agg.values,
        labels=agg.index,
        autopct="%1.1f%%",
        startangle=140,
        wedgeprops=dict(width=0.55, edgecolor="white"),
        colors=sns.color_palette("tab10", len(agg)),
    )
    for at in autotexts:
        at.set_fontsize(7)
    ax.set_title("Exposición Promedio por Sector (Dona)")
    _save(fig, "fig_donut_sector.png")


def fig_hist_retornos(cartera: pd.DataFrame) -> None:
    """Distribución empírica de retornos diarios por cuenta."""
    if cartera.empty:
        logging.error("fig3: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["retorno"] = pd.to_numeric(df["retorno"], errors="coerce")
    df = df.dropna(subset=["retorno"])
    df = df[np.isfinite(df["retorno"]) & (df["retorno"] != 0)]

    if df.empty:
        logging.error("fig3: sin retornos válidos — figura omitida.")
        return

    fig, ax = plt.subplots(figsize=(7, 3.5))
    sns.histplot(
        data=df, x="retorno", hue="cuenta_id",
        bins=40, kde=True, ax=ax,
        palette="tab10", alpha=0.6,
    )
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=1))
    ax.set_title("Distribución de Retornos Diarios por Cuenta")
    ax.set_xlabel("Retorno Diario")
    ax.set_ylabel("Frecuencia")
    _save(fig, "fig_hist_retornos.png")


def fig_box_retornos(cartera: pd.DataFrame) -> None:
    """Dispersión de retornos diarios por cuenta (boxplot)."""
    if cartera.empty:
        logging.error("fig4: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["retorno"] = pd.to_numeric(df["retorno"], errors="coerce")
    df = df.dropna(subset=["retorno"])
    df = df[np.isfinite(df["retorno"]) & (df["retorno"] != 0)]

    if df.empty:
        logging.error("fig4: sin retornos válidos — figura omitida.")
        return

    fig, ax = plt.subplots(figsize=(7, 3.5))
    sns.boxplot(
        data=df, x="cuenta_id", y="retorno",
        order=sorted(df["cuenta_id"].unique()),
        palette="tab10", ax=ax,
        flierprops=dict(marker=".", markersize=3, alpha=0.4),
    )
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=1))
    ax.set_title("Distribución de Retornos Diarios por Cuenta (Boxplot)")
    ax.set_xlabel("Cuenta")
    ax.set_ylabel("Retorno Diario")
    _save(fig, "fig_box_retornos.png")


def fig_vol_rolling(cartera: pd.DataFrame) -> None:
    """
    Volatilidad móvil anualizada (ventana 21 días de trading).
    Se aplica únicamente sobre el campo 'retorno' calculado por transform.py,
    que ya excluye transiciones sin capital previo.
    """
    if cartera.empty:
        logging.error("fig5: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["retorno"] = pd.to_numeric(df["retorno"], errors="coerce")
    df["fecha"]   = pd.to_datetime(df["fecha"], errors="coerce")

    fig, ax = plt.subplots(figsize=(7.5, 3.5))
    ploteado = False

    for cta, g in df.groupby("cuenta_id"):
        g = g.sort_values("fecha").set_index("fecha")
        vol = (
            g["retorno"]
            .rolling(21, min_periods=15)
            .std()
            .mul(np.sqrt(252))
        )
        vol = vol.dropna()
        if vol.empty:
            continue
        ax.plot(vol.index, vol.values, lw=1.2, label=str(cta))
        ploteado = True

    if not ploteado:
        logging.error("fig5: sin datos de volatilidad — figura omitida.")
        plt.close(fig)
        return

    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=0))
    ax.set_title("Volatilidad Móvil Anualizada — Ventana 21 Días")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Volatilidad Anualizada (σ × √252)")
    ax.legend(title="Cuenta")
    _save(fig, "fig_vol_rolling.png")


def fig_retornos_mensuales(cartera: pd.DataFrame) -> None:
    """Rendimiento mensual del portafolio agregado (producto de retornos diarios)."""
    if cartera.empty:
        logging.error("fig6: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["retorno"] = pd.to_numeric(df["retorno"], errors="coerce")
    df["fecha"]   = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha", "retorno"])

    df["anio_mes"] = df["fecha"].dt.to_period("M")
    mensual = (
        df.groupby("anio_mes")["retorno"]
        .apply(lambda r: (1 + r).prod() - 1)
        .reset_index()
    )
    mensual.columns = ["periodo", "retorno_mensual"]

    if mensual.empty or mensual["retorno_mensual"].isna().all():
        logging.error("fig6: sin retornos mensuales — figura omitida.")
        return

    mensual["anio"] = mensual["periodo"].dt.year
    mensual["mes"]  = mensual["periodo"].dt.month
    pivot = mensual.pivot(index="anio", columns="mes", values="retorno_mensual")
    MESES_ES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    pivot.columns = [MESES_ES[m - 1] for m in pivot.columns]

    fig, ax = plt.subplots(figsize=(10, max(2.5, len(pivot) * 0.8)))
    sns.heatmap(
        pivot, annot=True, fmt=".1%", linewidths=0.5,
        cmap="RdYlGn", center=0, ax=ax,
        annot_kws={"size": 7},
    )
    ax.set_title("Rendimiento Mensual del Portafolio Agregado")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Año")
    _save(fig, "fig_retornos_mensuales.png")


def fig_drawdown(cartera: pd.DataFrame) -> None:
    """
    Curva de drawdown: distancia porcentual del valor actual respecto al
    máximo histórico acumulado. Fórmula: (V_actual / max_acumulado) - 1.
    """
    if cartera.empty:
        logging.error("fig7: cartera vacía — figura omitida.")
        return

    df = cartera.copy()
    df["valor_cartera"] = pd.to_numeric(df["valor_cartera"], errors="coerce")
    df["fecha"]         = pd.to_datetime(df["fecha"], errors="coerce")

    port = (
        df.groupby("fecha")["valor_cartera"]
        .sum()
        .sort_index()
        .astype(float)
    )
    port = port[port > 0]

    if port.empty:
        logging.error("fig7: portafolio sin valor positivo — figura omitida.")
        return

    max_acum = port.expanding().max()
    drawdown = (port / max_acum) - 1.0
    drawdown = drawdown.replace([np.inf, -np.inf], np.nan).dropna()

    if drawdown.empty:
        logging.error("fig7: drawdown sin datos válidos — figura omitida.")
        return

    fig, ax = plt.subplots(figsize=(7.5, 3.5))
    ax.fill_between(drawdown.index, drawdown.values, 0,
                    color=PALETTE[3], alpha=0.55, label="Drawdown")
    ax.plot(drawdown.index, drawdown.values, lw=0.8, color=PALETTE[3])
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1, decimals=1))
    ax.set_title("Curva de Drawdown — Caída desde Máximos Históricos")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Drawdown (%)")
    ax.legend()
    _save(fig, "fig_drawdown.png")


def fig_top10_tickers(exp_ticker: pd.DataFrame) -> None:
    """Top-10 activos por valor promedio de posición durante el período analizado."""
    if exp_ticker.empty:
        logging.error("fig8: exposición_ticker vacía — figura omitida.")
        return

    df = exp_ticker.copy()
    df["valor_promedio"] = pd.to_numeric(df["valor_promedio"], errors="coerce")
    df = df.dropna(subset=["valor_promedio"])
    df = df[df["valor_promedio"] > 0]

    if df.empty:
        logging.error("fig8: todos los valores son cero — figura omitida.")
        return

    top = (
        df.groupby("ticker")["valor_promedio"]
        .sum()
        .nlargest(10)
        .sort_values()
    )

    fig, ax = plt.subplots(figsize=(6.5, 4))
    bars = ax.barh(top.index, top.values,
                   color=sns.color_palette("Blues_d", len(top)),
                   edgecolor="white")
    ax.bar_label(bars, labels=[f"${v:,.0f}" for v in top.values],
                 padding=4, fontsize=7)
    ax.set_title("Top 10 Tickers por Valor Promedio de Posición")
    ax.set_xlabel("Valor Promedio (USD)")
    ax.set_ylabel("Ticker")
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    _save(fig, "fig_top10_tickers.png")


def generate_all_figures(out_parquet: str = "data/out_parquet") -> None:
    """Carga artefactos Parquet y genera las 8 figuras del paper."""
    logging.info("Iniciando generación de figuras.")

    cartera    = _load_parquet("cartera_diaria",    out_parquet)
    exp_ticker = _load_parquet("exposicion_ticker", out_parquet)
    exp_sector = _load_parquet("exposicion_sector", out_parquet)

    try:
        indices = pd.read_excel("data/nuevo_dataset.xlsx", sheet_name="indices")
        indices.columns = [str(c).strip().lower() for c in indices.columns]
    except Exception as e:
        logging.warning(f"No se pudo cargar la hoja 'indices': {e}")
        indices = pd.DataFrame()

    if not cartera.empty:
        logging.info(
            f"cartera — filas: {len(cartera)} | "
            f"cuentas: {cartera['cuenta_id'].nunique()} | "
            f"rango: {cartera['fecha'].min().date()} → {cartera['fecha'].max().date()}"
        )

    fig_portafolio_vs_benchmark(cartera, indices)
    fig_donut_sector(exp_sector)
    fig_hist_retornos(cartera)
    fig_box_retornos(cartera)
    fig_vol_rolling(cartera)
    fig_retornos_mensuales(cartera)
    fig_drawdown(cartera)
    fig_top10_tickers(exp_ticker)

    logging.info("Generación de figuras completada.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    generate_all_figures()
